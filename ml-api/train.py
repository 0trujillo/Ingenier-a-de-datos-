import pandas as pd
import joblib
import logging
import sys
import os
from pathlib import Path

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# Agregar path para importar model_registry
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model_registry import ModelRegistry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Crear datos de entrenamiento
data = []

for temp in range(0, 50):
    for humidity in range(5, 100, 5):
        for wind in range(0, 40, 5):
            risk = 0

            if temp > 35:
                risk += 40

            if humidity < 20:
                risk += 30

            if wind > 25:
                risk += 30

            label = 1 if risk >= 70 else 0

            data.append([
                temp,
                humidity,
                wind,
                label
            ])

df = pd.DataFrame(
    data,
    columns=[
        "temperature",
        "humidity",
        "wind_speed",
        "risk"
    ]
)

X = df[["temperature", "humidity", "wind_speed"]]
y = df["risk"]

# Entrenar modelo
logger.info("🎓 Iniciando entrenamiento del modelo...")

model = RandomForestClassifier(
    n_estimators=100,
    random_state=42,
    n_jobs=-1
)

model.fit(X, y)

# Calcular métricas
y_pred = model.predict(X)
accuracy = accuracy_score(y, y_pred)
precision = precision_score(y, y_pred, zero_division=0)
recall = recall_score(y, y_pred, zero_division=0)
f1 = f1_score(y, y_pred, zero_division=0)

logger.info("✅ Modelo entrenado")
logger.info("📊 Métricas:")
logger.info("   - Accuracy:  %.4f", accuracy)
logger.info("   - Precision: %.4f", precision)
logger.info("   - Recall:    %.4f", recall)
logger.info("   - F1-Score:  %.4f", f1)

# Guardar modelo en archivo principal
model_path = os.path.join(os.path.dirname(__file__), "model.pkl")
joblib.dump(model, model_path)
logger.info("💾 Modelo guardado en: %s", model_path)

# Registrar modelo con versionamiento
try:
    registry = ModelRegistry(registry_path="/app/models")
    version_id = registry.save_model(
        model=model,
        accuracy=accuracy,
        precision=precision,
        recall=recall,
        f1_score=f1,
        training_samples=len(X),
        notes="Modelo de línea base entrenado con datos sintéticos"
    )
    logger.info("📦 Modelo v%s registrado en el Model Registry", version_id)
except Exception as exc:
    logger.warning("⚠️  Error registrando modelo: %s", exc)