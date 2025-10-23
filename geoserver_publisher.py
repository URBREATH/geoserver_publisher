import os
import time
import json
import logging
import requests
import io
from requests.auth import HTTPBasicAuth
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)


TARGET_DIR = os.environ['TARGET_DIR'] 

GEOSERVER_DATA_ROOT = "/opt/geoserver_data" 


GEOSERVER_URL = os.environ['GEOSERVER_URL']
GEOSERVER_USER = os.environ['GEOSERVER_USER']
GEOSERVER_PASSWORD = os.environ['GEOSERVER_PASSWORD']

# Configurazione MinIO
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']
MINIO_SECURE = os.environ.get('MINIO_SECURE', 'false').lower() == 'true'


PUBLISH_INTERVAL_SECONDS = 300
CONFIG_FILE_NAME = "_publish.json"
PROCESSED_FILE_NAME = "_published.json"


auth = HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD)
headers_json = {"Content-type": "application/json", "Accept": "application/json"}
base_rest_url = f"{GEOSERVER_URL}/rest"


try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )
    logging.info(f"Successfully connected to MinIO endpoint at '{MINIO_ENDPOINT}'")
except Exception as e:
    logging.error(f"Failed to initialize MinIO client: {e}")
    exit(1)

logging.info("--- Starting GeoServer automatic publishing service (MinIO-aware) ---")

def find_publish_requests_in_minio():
    """Scansiona il bucket MinIO per file _publish.json."""
    config_files = []
    try:
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)
        for obj in objects:
            if obj.object_name.endswith(CONFIG_FILE_NAME):
                config_files.append(obj.object_name)
    except S3Error as e:
        logging.error(f"Failed to list objects from MinIO bucket '{MINIO_BUCKET}': {e}")
    return config_files

def get_geoserver_path(publisher_local_path):
    """Traduce il percorso locale del publisher nel percorso che GeoServer vede."""

    try:
        relative_path = os.path.relpath(publisher_local_path, TARGET_DIR)
        geoserver_path = os.path.join(GEOSERVER_DATA_ROOT, relative_path)

        return geoserver_path.replace(os.path.sep, '/')
    except ValueError:
        logging.warning(f"Path '{publisher_local_path}' non sembra essere in TARGET_DIR '{TARGET_DIR}'.")
        return None

def publish_datastore(workspace, store_name, data_path_for_geoserver):
    """Crea un DataStore (es. Shapefile) e pubblica il layer."""
    url = f"{base_rest_url}/workspaces/{workspace}/datastores"
    
    payload = {
        "dataStore": {
            "name": store_name,
            "enabled": True,
            "connectionParameters": {
                "entry": [
                    {"@key": "url", "$": f"file:{data_path_for_geoserver}"},
                    {"@key": "namespace", "$": f"urn:geoserver:{workspace}"}
                ]
            }
        }
    }
    
    response = requests.post(url, data=json.dumps(payload), auth=auth, headers=headers_json)
    
    if response.status_code == 201:
        logging.info(f"Successfully created DataStore '{store_name}' in workspace '{workspace}'.")
        ft_url = f"{url}/{store_name}/featuretypes.json"
        response_ft = requests.post(ft_url, data='{}', auth=auth, headers=headers_json)
        if response_ft.status_code == 201:
            logging.info(f"Layer for store '{store_name}' published successfully.")
            return True
        else:
            logging.error(f"Failed to publish layer from store '{store_name}'. Status: {response_ft.status_code}, Text: {response_ft.text}")
            return False
    elif response.status_code == 409:
        logging.warning(f"DataStore '{store_name}' esiste già. Si assume sia pubblicato.")
        return True
    else:
        logging.error(f"Failed to create DataStore '{store_name}'. Status: {response.status_code}, Text: {response.text}")
        return False

def publish_coveragestore(workspace, store_name, publisher_local_path_to_read):
    """Crea un CoverageStore (GeoTIFF) caricando il file."""
    

    url = f"{base_rest_url}/workspaces/{workspace}/coveragestores/{store_name}/file.geotiff"
    

    headers_raster = {"Content-type": "image/tiff"}
    
    try:
        with open(publisher_local_path_to_read, 'rb') as f:
            data = f.read()
    except IOError as e:
        logging.error(f"Failed to read local file '{publisher_local_path_to_read}': {e}")
        return False

    response = requests.put(url, data=data, auth=auth, headers=headers_raster)

    if response.status_code == 201:
        logging.info(f"Successfully created CoverageStore and Layer '{store_name}' in workspace '{workspace}'.")
        return True
    elif response.status_code == 409:
        logging.warning(f"CoverageStore '{store_name}' esiste già. Si assume sia pubblicato.")
        return True
    else:
        logging.error(f"Failed to create CoverageStore '{store_name}'. Status: {response.status_code}, Text: {response.text}")
        return False

def run_publish_cycle():
    """Esegue un singolo ciclo di scansione MinIO e pubblicazione."""
    logging.info("Starting new publish cycle (scanning MinIO)...")
    
    publish_requests = find_publish_requests_in_minio()
    
    if not publish_requests:
        logging.info("No new publish requests found in MinIO.")
        return

    logging.info(f"Found {len(publish_requests)} new publish requests in MinIO.")
    success_count = 0
    
    for config_key in publish_requests:
        try:

            response = minio_client.get_object(MINIO_BUCKET, config_key)
            config_data = response.read()
            config = json.loads(config_data.decode('utf-8'))
            response.close()
            response.release_conn()
            
            workspace = config['workspace']
            store_name = config['store_name']
            data_path_rel = config['data_path']

            data_path_local_publisher = os.path.join(TARGET_DIR, data_path_rel)
            
            if not os.path.exists(data_path_local_publisher):
                logging.warning(f"Data file '{data_path_local_publisher}' not found locally for request '{config_key}'. Skipping (waiting for minio-sync).")
                continue

            data_path_for_geoserver = get_geoserver_path(data_path_local_publisher)
            if not data_path_for_geoserver:
                continue

            logging.info(f"Processing publish request for '{data_path_rel}'...")

            published = False
            if data_path_rel.lower().endswith('.shp'):
                published = publish_datastore(workspace, store_name, data_path_for_geoserver)
            elif data_path_rel.lower().endswith(('.tif', '.tiff', '.gtiff')):
                published = publish_coveragestore(workspace, store_name, data_path_local_publisher)
            else:
                logging.warning(f"Unsupported file type for '{data_path_rel}'. Skipping.")
                continue

            if published:
                processed_key = config_key.replace(CONFIG_FILE_NAME, PROCESSED_FILE_NAME)
                
                source = CopySource(MINIO_BUCKET, config_key)
                
                minio_client.copy_object(
                    MINIO_BUCKET,
                    processed_key,
                    source
                )
                minio_client.remove_object(MINIO_BUCKET, config_key)
                
                logging.info(f"Successfully processed and renamed '{config_key}' to '{processed_key}' in MinIO.")
                success_count += 1

        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from '{config_key}' in MinIO.")
        except S3Error as e:
            logging.error(f"S3 error while processing '{config_key}': {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing '{config_key}': {e}", exc_info=True)

    logging.info(f"Publish cycle finished. Successfully published {success_count} of {len(publish_requests)} requests.")


if __name__ == "__main__":
    while True:
        run_publish_cycle()
        logging.info(f"--- Cycle finished. Waiting for {PUBLISH_INTERVAL_SECONDS} seconds... ---")
        time.sleep(PUBLISH_INTERVAL_SECONDS)
