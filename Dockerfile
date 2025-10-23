# Usa un'immagine Python leggera
FROM python:3.9-slim

# Imposta la directory di lavoro
WORKDIR /app

# Installa le dipendenze: 'requests' per GeoServer e 'minio' per MinIO
RUN pip install requests minio

# Copia lo script del publisher nel container
COPY geoserver_publisher.py .

# Comando da eseguire all'avvio del container
CMD ["python", "geoserver_publisher.py"]
