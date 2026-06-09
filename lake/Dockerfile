FROM python:3.11-slim

WORKDIR /app

# Copiar archivos de dependencias
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY lake/ ./lake/
COPY producer/ ./producer/

# Por defecto ejecutar analytics loader
CMD ["python", "lake/analytics/loader.py"]
