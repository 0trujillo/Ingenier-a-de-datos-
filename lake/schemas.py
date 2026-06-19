"""
Módulos de validación y esquemas para el pipeline de datos.
Utiliza Pydantic para validar la estructura de los datos en cada etapa.
"""

from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime


class WeatherData(BaseModel):
    """Esquema de datos meteorológicos del productor."""
    temperature: float = Field(..., ge=-50, le=60, description="Temperatura en °C")
    humidity: float = Field(..., ge=0, le=100, description="Humedad relativa en %")
    wind_speed: float = Field(..., ge=0, description="Velocidad del viento en km/h")
    timestamp: float = Field(..., description="Timestamp Unix del evento")
    source: Optional[str] = Field(default="weather-api", description="Origen del dato")

    @validator('temperature')
    def validate_temperature(cls, v):
        if v < -50 or v > 60:
            raise ValueError('Temperatura fuera del rango válido [-50, 60]°C')
        return v

    @validator('humidity')
    def validate_humidity(cls, v):
        if v < 0 or v > 100:
            raise ValueError('Humedad debe estar entre 0 y 100%')
        return v

    @validator('wind_speed')
    def validate_wind_speed(cls, v):
        if v < 0:
            raise ValueError('Velocidad del viento no puede ser negativa')
        return v


class SensorData(BaseModel):
    """Esquema de datos de sensores IoT."""
    sensor_id: int = Field(..., gt=0, description="ID del sensor")
    sensor_type: str = Field(default="environmental", description="Tipo de sensor")
    temperature: float = Field(..., ge=-50, le=60)
    humidity: float = Field(..., ge=0, le=100)
    pressure: float = Field(..., ge=300, le=1200, description="Presión en mb")
    solar_radiation: float = Field(..., ge=0, description="Radiación solar en W/m²")
    air_quality_index: float = Field(..., ge=0, description="Índice de calidad del aire")
    co2_level: float = Field(..., ge=300, le=5000, description="Nivel de CO2 en ppm")
    timestamp: float = Field(..., description="Timestamp Unix del evento")
    source: str = Field(default="sensor-simulator")


class VideoFrameData(BaseModel):
    """Esquema de metadatos de frames de video."""
    camera_id: str = Field(..., description="ID de la cámara")
    frame_id: str = Field(..., description="ID único del frame")
    frame_number: int = Field(..., gt=0)
    fps: float = Field(..., gt=0, le=60, description="Fotogramas por segundo")
    timestamp: float = Field(...)
    has_motion: bool = Field(default=False)
    detection_count: int = Field(..., ge=0)
    detected_objects: list = Field(default_factory=list, description="Tipos de objetos detectados")
    detection_confidence: float = Field(..., ge=0, le=1)
    source: str = Field(default="video-simulator")


class CleanedWeatherData(BaseModel):
    """Esquema de datos meteorológicos procesados (Silver)."""
    temperature: float = Field(..., ge=-50, le=60)
    humidity: float = Field(..., ge=0, le=100)
    wind_speed: float = Field(..., ge=0)
    heat_index: float = Field(..., description="Índice de calor calculado")
    timestamp: float
    processed_at: float = Field(default_factory=lambda: datetime.now().timestamp())
    source: Optional[str] = None


class EnrichedWeatherData(BaseModel):
    """Esquema de datos enriquecidos con predicción ML (Gold)."""
    temperature: float
    humidity: float
    wind_speed: float
    heat_index: float
    timestamp: float
    processed_at: float
    risk_score: float = Field(..., ge=0, le=100, description="Score de riesgo predicho")
    risk_level: str = Field(..., pattern="^(NORMAL|MEDIO|ALTO)$")
    enriched_at: float = Field(default_factory=lambda: datetime.now().timestamp())


class ModelMetadata(BaseModel):
    """Metadatos de un modelo de Machine Learning."""
    version: str = Field(..., description="Versión del modelo (timestamp)")
    created_at: str = Field(..., description="Fecha de creación")
    accuracy: float = Field(..., ge=0, le=1)
    precision: float = Field(..., ge=0, le=1)
    recall: float = Field(..., ge=0, le=1)
    f1_score: Optional[float] = Field(None, ge=0, le=1)
    training_samples: int = Field(..., gt=0)
    notes: Optional[str] = None
