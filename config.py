import os

# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'geodata')
MINIO_SECURE = os.getenv('MINIO_SECURE', 'false').lower() == 'true'
MINIO_PROXY_URL = os.getenv('MINIO_PROXY_URL', 'http://localhost:9090')

# GeoServer Configuration
GEOSERVER_URL = os.getenv('GEOSERVER_URL', 'http://geoserver:8080/geoserver')
GEOSERVER_USER = os.getenv('GEOSERVER_USER', 'admin')
GEOSERVER_PASSWORD = os.getenv('GEOSERVER_PASSWORD', 'geoserver')

# IDRA Configuration
IDRA_URL = os.getenv('IDRA_URL', '')
GEOSERVER_PUBLIC_URL = os.getenv('GEOSERVER_PUBLIC_URL', GEOSERVER_URL)

# Paths & Settings
TARGET_DIR = os.getenv('TARGET_DIR', '/data')
GEOSERVER_DATA_ROOT = os.getenv('GEOSERVER_DATA_ROOT', '/opt/geoserver_data')

PUBLISH_INTERVAL_SECONDS = int(os.getenv('PUBLISH_INTERVAL_SECONDS', '30'))
CONFIG_FILE_NAME = "_publish.json"
PROCESSED_FILE_NAME = "_published.json"

# --- NEW PATH ---
DISTRIBUTION_TEMPLATE_PATH = "/app/distribution_template.json"
DATASET_TEMPLATE_PATH = "/app/dataset_template.json"