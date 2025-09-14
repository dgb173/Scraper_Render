# Usar una imagen oficial de Python como base
FROM python:3.11-slim

# Instalar dependencias del sistema operativo necesarias para el navegador
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdbus-1-3 \
    libdrm2 libgbm1 libgtk-3-0 libasound2 && \
    rm -rf /var/lib/apt/lists/*

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiar el archivo de requisitos e instalar las librerías de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar el navegador Chromium para Playwright
RUN playwright install chromium

# Copiar todo el código de la aplicación al contenedor
COPY . .

# Exponer un puerto por conveniencia (Render/Fly establecerán $PORT)
EXPOSE 8080

# Definir el comando para iniciar la aplicación con Gunicorn
# Usa $PORT si está disponible (Render/Fly), si no, 8080 por defecto
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:${PORT:-8080} --timeout 120 app:app"]
