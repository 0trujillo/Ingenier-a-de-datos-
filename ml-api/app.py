import os
import sys
import logging
import joblib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

# Agregar path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model_registry import ModelRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Wildfire Risk Prediction API")

MODEL_PATH = os.getenv("MODEL_PATH", "/app/model.pkl")
REGISTRY_PATH = os.getenv("REGISTRY_PATH", "/app/models")

# Variables globales para el modelo
_model = None
_model_version = None
_registry = None


class PredictionRequest(BaseModel):
    """Esquema de solicitud de predicción."""
    temperature: float = Field(..., ge=-50, le=60, description="Temperatura en °C")
    humidity: float = Field(..., ge=0, le=100, description="Humedad relativa en %")
    wind_speed: float = Field(..., ge=0, description="Velocidad del viento en km/h")


class PredictionResponse(BaseModel):
    """Esquema de respuesta de predicción."""
    risk_score: float = Field(..., ge=0, le=100)
    risk_level: str = Field(..., pattern="^(NORMAL|MEDIO|ALTO)$")
    model_version: str
    confidence: float = Field(..., ge=0, le=1)


@app.on_event("startup")
async def load_model():
    """Carga el modelo al iniciar la aplicación."""
    global _model, _model_version, _registry
    
    logger.info("🚀 Inicializando API de predicción...")
    
    try:
        # Intentar usar el Model Registry
        _registry = ModelRegistry(registry_path=REGISTRY_PATH)
        _model, metadata = _registry.load_model(version_id="latest")
        _model_version = metadata["version"]
        
        logger.info(
            "✅ Modelo cargado desde registry v%s (Accuracy: %.3f)",
            _model_version, metadata["accuracy"]
        )
        
    except Exception as exc:
        logger.warning(
            "⚠️  No se pudo cargar desde registry, intentando cargar modelo local: %s",
            exc
        )
        
        # Fallback: cargar modelo local
        if os.path.exists(MODEL_PATH):
            try:
                _model = joblib.load(MODEL_PATH)
                _model_version = "local"
                logger.info("✅ Modelo local cargado desde %s", MODEL_PATH)
            except Exception as exc_local:
                logger.error("❌ Error cargando modelo local: %s", exc_local, exc_info=True)
                raise RuntimeError("No se pudo cargar el modelo") from exc_local
        else:
            raise FileNotFoundError(f"Modelo no encontrado en {MODEL_PATH}")


@app.get("/health")
async def health():
    """Endpoint de salud."""
    if _model is None:
        return {"status": "unhealthy", "reason": "model_not_loaded"}
    return {
        "status": "healthy",
        "model_version": _model_version
    }


@app.get("/model/info")
async def model_info():
    """Obtiene información del modelo actual."""
    if _registry is None:
        return {
            "version": _model_version,
            "source": "local_file"
        }
    
    try:
        metadata = _registry.get_version_info(_model_version)
        return {
            "version": _model_version,
            "source": "model_registry",
            "metadata": metadata
        }
    except Exception as exc:
        logger.error("Error obteniendo metadata: %s", exc)
        return {"version": _model_version, "error": str(exc)}


@app.get("/model/versions")
async def list_versions():
    """Lista todas las versiones disponibles del modelo."""
    if _registry is None:
        return {"versions": [{"version": "local", "source": "file"}]}
    
    try:
        versions = _registry.list_versions()
        return {"versions": versions}
    except Exception as exc:
        logger.error("Error listando versiones: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest) -> PredictionResponse:
    """
    Realiza una predicción de riesgo de incendio.
    
    Args:
        request: Datos meteorológicos (temperatura, humedad, viento)
    
    Returns:
        Predicción con score, nivel de riesgo y versión del modelo
    """
    if _model is None:
        raise HTTPException(status_code=503, detail="Modelo no cargado")
    
    try:
        # Preparar entrada
        features = [[request.temperature, request.humidity, request.wind_speed]]
        
        # Predicción
        prediction = _model.predict(features)[0]
        probabilities = _model.predict_proba(features)[0]
        
        # Calcular score (0-100)
        risk_probability = probabilities[1]  # Probabilidad de clase "ALTO"
        risk_score = risk_probability * 100
        
        # Determinar nivel de riesgo a partir del score (3 niveles)
        # score: 0-100
        if risk_score >= 75:
            risk_level = "ALTO"
        elif risk_score >= 50:
            risk_level = "MEDIO"
        else:
            risk_level = "NORMAL"
        
        logger.info(
            "🔮 Predicción: temp=%.1f, humidity=%.1f, wind=%.1f -> "
            "score=%.1f, level=%s",
            request.temperature, request.humidity, request.wind_speed,
            risk_score, risk_level
        )
        
        return PredictionResponse(
            risk_score=round(risk_score, 2),
            risk_level=risk_level,
            model_version=_model_version,
            confidence=round(max(probabilities), 3)
        )
    
    except Exception as exc:
        logger.error("❌ Error en predicción: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/")
async def root():
    """Endpoint raíz."""
    return {
        "service": "Wildfire Risk Prediction API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "predict": "/predict",
            "model_info": "/model/info",
            "model_versions": "/model/versions"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)