"""
Model Registry - Sistema de versionamiento y gestión de modelos ML.
Permite guardar versiones de modelos con metadatos y facilita rollback.
"""

import os
import json
import joblib
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List


class ModelRegistry:
    """
    Registro centralizado de versiones de modelos de Machine Learning.
    """
    
    def __init__(self, registry_path: str = "/app/models"):
        self.registry_path = Path(registry_path)
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def save_model(
        self,
        model: Any,
        accuracy: float,
        precision: float,
        recall: float,
        f1_score: Optional[float] = None,
        training_samples: int = 0,
        notes: str = ""
    ) -> str:
        """
        Guarda una versión del modelo con metadatos.
        
        Args:
            model: Objeto del modelo entrenado (sklearn, xgboost, etc.)
            accuracy: Exactitud del modelo (0-1)
            precision: Precisión del modelo (0-1)
            recall: Recall del modelo (0-1)
            f1_score: F1 score opcional (0-1)
            training_samples: Cantidad de muestras de entrenamiento
            notes: Notas adicionales sobre el modelo
        
        Returns:
            version_id: ID de la versión guardada (timestamp)
        """
        version_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_path = self.registry_path / f"model_v{version_id}"
        version_path.mkdir(parents=True, exist_ok=True)
        
        try:
            # Guardar modelo
            model_file = version_path / "model.pkl"
            joblib.dump(model, model_file)
            self.logger.info("✅ Modelo guardado en %s", model_file)
            
            # Guardar metadatos
            metadata = {
                "version": version_id,
                "created_at": datetime.now().isoformat(),
                "accuracy": round(accuracy, 4),
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1_score": round(f1_score, 4) if f1_score else None,
                "training_samples": training_samples,
                "notes": notes,
                "status": "active"
            }
            
            metadata_file = version_path / "metadata.json"
            with open(metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)
            
            self.logger.info(
                "✅ Versión v%s guardada - Accuracy: %.3f, Precision: %.3f, Recall: %.3f",
                version_id, accuracy, precision, recall
            )
            
            # Actualizar last_version symlink
            self._update_latest_version(version_id)
            
            return version_id
            
        except Exception as exc:
            self.logger.error("❌ Error guardando modelo v%s: %s", version_id, exc, exc_info=True)
            raise
    
    def load_model(self, version_id: str = "latest") -> tuple:
        """
        Carga un modelo y sus metadatos.
        
        Args:
            version_id: ID de versión a cargar ('latest' para la última)
        
        Returns:
            (model, metadata): Tupla con modelo y diccionario de metadatos
        """
        if version_id == "latest":
            version_id = self._get_latest_version()
            if not version_id:
                raise ValueError("No hay versiones de modelo disponibles")
        
        version_path = self.registry_path / f"model_v{version_id}"
        
        if not version_path.exists():
            raise ValueError(f"Versión {version_id} no encontrada")
        
        try:
            model_file = version_path / "model.pkl"
            model = joblib.load(model_file)
            
            metadata_file = version_path / "metadata.json"
            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            
            self.logger.info("✅ Modelo v%s cargado", version_id)
            return model, metadata
            
        except Exception as exc:
            self.logger.error("❌ Error cargando modelo v%s: %s", version_id, exc, exc_info=True)
            raise
    
    def list_versions(self) -> List[Dict[str, Any]]:
        """
        Lista todas las versiones disponibles con sus metadatos.
        
        Returns:
            Lista de diccionarios con versiones y metadatos
        """
        versions = []
        
        for version_dir in sorted(self.registry_path.glob("model_v*"), reverse=True):
            metadata_file = version_dir / "metadata.json"
            if metadata_file.exists():
                with open(metadata_file, "r") as f:
                    metadata = json.load(f)
                    versions.append(metadata)
        
        return versions
    
    def get_version_info(self, version_id: str) -> Dict[str, Any]:
        """
        Obtiene información detallada de una versión específica.
        """
        version_path = self.registry_path / f"model_v{version_id}"
        metadata_file = version_path / "metadata.json"
        
        if not metadata_file.exists():
            raise ValueError(f"Versión {version_id} no encontrada")
        
        with open(metadata_file, "r") as f:
            return json.load(f)
    
    def compare_versions(self, version_a: str, version_b: str) -> Dict[str, Any]:
        """
        Compara dos versiones de modelos.
        
        Returns:
            Diccionario con comparativa de métricas
        """
        meta_a = self.get_version_info(version_a)
        meta_b = self.get_version_info(version_b)
        
        return {
            "version_a": version_a,
            "version_b": version_b,
            "accuracy_improvement": (meta_b["accuracy"] - meta_a["accuracy"]) * 100,
            "precision_improvement": (meta_b["precision"] - meta_a["precision"]) * 100,
            "recall_improvement": (meta_b["recall"] - meta_a["recall"]) * 100,
            "created_a": meta_a["created_at"],
            "created_b": meta_b["created_at"],
        }
    
    def _get_latest_version(self) -> Optional[str]:
        """Obtiene el ID de la última versión guardada."""
        versions = sorted(self.registry_path.glob("model_v*"), reverse=True)
        if versions:
            return versions[0].name.replace("model_v", "")
        return None
    
    def _update_latest_version(self, version_id: str):
        """Actualiza un archivo que apunta a la última versión."""
        latest_file = self.registry_path / "LATEST_VERSION"
        with open(latest_file, "w") as f:
            f.write(version_id)


# Ejemplo de uso
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Crear registry
    registry = ModelRegistry("/tmp/model_registry")
    
    # (Después de entrenar un modelo)
    # version_id = registry.save_model(
    #     model=trained_model,
    #     accuracy=0.95,
    #     precision=0.93,
    #     recall=0.94,
    #     f1_score=0.935,
    #     training_samples=5000,
    #     notes="Entrenado con datos de 2026-06"
    # )
    
    # Listar versiones
    # print(registry.list_versions())
    
    # Cargar última versión
    # model, metadata = registry.load_model("latest")
