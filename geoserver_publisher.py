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

# --- Environment Variables ---
TARGET_DIR = os.environ['TARGET_DIR'] 
GEOSERVER_DATA_ROOT = os.environ.get('GEOSERVER_DATA_ROOT', '/opt/geoserver_data')
GEOSERVER_URL = os.environ['GEOSERVER_URL']
GEOSERVER_USER = os.environ['GEOSERVER_USER']
GEOSERVER_PASSWORD = os.environ['GEOSERVER_PASSWORD']

MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
MINIO_BUCKET = os.environ['MINIO_BUCKET']
MINIO_SECURE = os.environ.get('MINIO_SECURE', 'false').lower() == 'true'

# --- Constants ---
PUBLISH_INTERVAL_SECONDS = 300
CONFIG_FILE_NAME = "_publish.json"
PROCESSED_FILE_NAME = "_published.json"

# --- GeoServer REST Client Setup ---
auth = HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD)
headers_json = {"Content-type": "application/json", "Accept": "application/json"}
headers_sld = {"Content-type": "application/vnd.ogc.sld+xml", "Accept": "application/json"}
base_rest_url = f"{GEOSERVER_URL}/rest"

# --- MinIO Client Setup ---
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

logging.info("--- Starting GeoServer automatic publishing service (MinIO-aware + SLD + Workspace) ---")

def find_publish_requests_in_minio():
    """Scans the MinIO bucket for _publish.json files."""
    config_files = []
    try:
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)
        for obj in objects:
            if obj.object_name.endswith(CONFIG_FILE_NAME):
                config_files.append(obj.object_name)
    except S3Error as e:
        logging.error(f"Failed to list objects from MinIO bucket '{MINIO_BUCKET}': {e}")
    return config_files

# --- NEW FUNCTION ---
def ensure_workspace_exists(workspace):
    """Checks if a workspace exists, creates it if not."""
    url = f"{base_rest_url}/workspaces/{workspace}"
    
    response = requests.get(url, auth=auth, headers=headers_json)
    
    if response.status_code == 200:
        # logging.info(f"Workspace '{workspace}' already exists.")
        return True
    
    if response.status_code == 404:
        logging.info(f"Workspace '{workspace}' not found. Attempting to create it...")
        create_url = f"{base_rest_url}/workspaces"
        payload = {
            "workspace": {
                "name": workspace
            }
        }
        create_response = requests.post(create_url, data=json.dumps(payload), auth=auth, headers=headers_json)
        
        if create_response.status_code == 201:
            logging.info(f"Successfully created workspace '{workspace}'.")
            return True
        else:
            logging.error(f"Failed to create workspace '{workspace}'. Status: {create_response.status_code}, Text: {create_response.text}")
            return False
    
    logging.error(f"Error checking workspace '{workspace}'. Status: {response.status_code}, Text: {response.text}")
    return False

def get_geoserver_path(publisher_local_path):
    """Translates the local path to the path GeoServer sees on its mounted volume."""
    try:
        relative_path = os.path.relpath(publisher_local_path, TARGET_DIR)
        geoserver_path = os.path.join(GEOSERVER_DATA_ROOT, relative_path)
        return geoserver_path.replace(os.path.sep, '/')
    except ValueError:
        logging.warning(f"Path '{publisher_local_path}' does not seem to be in TARGET_DIR '{TARGET_DIR}'.")
        return None

def publish_datastore(workspace, store_name, data_path_for_geoserver):
    """Creates a DataStore (e.g., Shapefile) and publishes the layer."""
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
        logging.warning(f"DataStore '{store_name}' already exists. Assuming published.")
        return True
    else:
        logging.error(f"Failed to create DataStore '{store_name}'. Status: {response.status_code}, Text: {response.text}")
        return False

def publish_coveragestore(workspace, store_name, publisher_local_path_to_read):
    """Creates a CoverageStore (GeoTIFF) by uploading the file."""
    
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
    elif response.status_code == 200:
        logging.info(f"Successfully updated CoverageStore and Layer '{store_name}'.")
        return True
    elif response.status_code == 409:
        logging.warning(f"CoverageStore '{store_name}' already exists. Assuming published.")
        return True
    else:
        logging.error(f"Failed to create/update CoverageStore '{store_name}'. Status: {response.status_code}, Text: {response.text}")
        return False

def upload_style(workspace, style_name, sld_body):
    """Uploads or updates a style (SLD) to GeoServer."""
    logging.info(f"Uploading style '{style_name}' to workspace '{workspace}'...")
    
    headers_sld = {"Content-type": "application/vnd.ogc.sld+xml", "Accept": "application/json"}
    
    # 1. Try to CREATE the style using POST
    # This is the correct endpoint for creating a style by uploading its body
    post_url = f"{base_rest_url}/workspaces/{workspace}/styles?name={style_name}"
    
    post_response = requests.post(post_url, data=sld_body.encode('utf-8'), auth=auth, headers=headers_sld)
    
    if post_response.status_code == 201:
        logging.info(f"Style '{style_name}' created successfully via POST.")
        return True

    # 2. If it already exists (409 Conflict), try to UPDATE it using PUT
    if post_response.status_code == 409:
        logging.warning(f"Style '{style_name}' already exists. Attempting to update it via PUT...")
        
        put_url = f"{base_rest_url}/workspaces/{workspace}/styles/{style_name}"
        put_response = requests.put(put_url, data=sld_body.encode('utf-8'), auth=auth, headers=headers_sld)
        
        if put_response.status_code == 200:
            logging.info(f"Style '{style_name}' updated successfully via PUT.")
            return True
        else:
            # PUT failed after POST failed
            logging.error(f"Failed to update existing style '{style_name}'. Status: {put_response.status_code}, Text: {put_response.text}")
            return False
    
    # 3. If POST failed for another reason (like the 400 we saw)
    logging.error(f"Failed to create style '{style_name}' via POST. Status: {post_response.status_code}, Text: {post_response.text}")
    return False

def assign_style_to_layer(workspace, layer_name, style_name):
    """Assigns a default style to a specific layer."""
    logging.info(f"Assigning style '{style_name}' as default for layer '{layer_name}'...")
    
    url = f"{base_rest_url}/layers/{workspace}:{layer_name}"
    
    payload = {
        "layer": {
            "defaultStyle": {
                "name": f"{workspace}:{style_name}" 
            }
        }
    }
    
    response = requests.put(url, data=json.dumps(payload), auth=auth, headers=headers_json)
    
    if response.status_code == 200:
        logging.info(f"Successfully assigned style to layer '{layer_name}'.")
        return True
    else:
        logging.error(f"Failed to assign style to layer '{layer_name}'. Status: {response.status_code}, Text: {response.text}")
        return False

def run_publish_cycle():
    """Runs a single scan and publish cycle."""
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
            style_name = config.get('style_name')
            sld_path_rel = config.get('sld_path')

            # --- MODIFICATION START ---
            # 1. Ensure workspace exists before doing anything else
            if not ensure_workspace_exists(workspace):
                logging.error(f"Failed to create or verify workspace '{workspace}' for '{config_key}'. Skipping.")
                continue
            # --- MODIFICATION END ---

            # 2. Check if data file is synced locally
            data_path_local_publisher = os.path.join(TARGET_DIR, data_path_rel)
            
            if not os.path.exists(data_path_local_publisher):
                logging.warning(f"Data file '{data_path_local_publisher}' not found locally for '{config_key}'. Skipping (waiting for minio-sync).")
                continue

            # 3. Publish data (Store + Layer)
            logging.info(f"Processing publish request for '{data_path_rel}'...")

            published = False
            layer_name_for_style = store_name # Default for GeoTIFF
            
            if data_path_rel.lower().endswith('.shp'):
                data_path_for_geoserver = get_geoserver_path(data_path_local_publisher)
                if not data_path_for_geoserver:
                    continue
                published = publish_datastore(workspace, store_name, data_path_for_geoserver)
                layer_name_for_style = os.path.splitext(os.path.basename(data_path_rel))[0]

            elif data_path_rel.lower().endswith(('.tif', '.tiff', '.gtiff')):
                published = publish_coveragestore(workspace, store_name, data_path_local_publisher)
                layer_name_for_style = store_name
            else:
                logging.warning(f"Unsupported file type for '{data_path_rel}'. Skipping.")
                continue

            # 4. Handle style if publication was successful
            style_op_success = True 
            if published:
                if style_name and sld_path_rel:
                    logging.info(f"Style information found for '{store_name}'. Processing style...")
                    
                    sld_path_local_publisher = os.path.join(TARGET_DIR, sld_path_rel)
                    
                    if not os.path.exists(sld_path_local_publisher):
                        logging.warning(f"SLD file '{sld_path_local_publisher}' not found locally. Skipping style assignment (waiting for minio-sync).")
                        style_op_success = False
                    else:
                        try:
                            with open(sld_path_local_publisher, 'r', encoding='utf-8') as f:
                                sld_body = f.read()
                            
                            style_uploaded = upload_style(workspace, style_name, sld_body)
                            
                            if style_uploaded:
                                if not assign_style_to_layer(workspace, layer_name_for_style, style_name):
                                    style_op_success = False
                            else:
                                style_op_success = False
                                
                        except Exception as e:
                            logging.error(f"An error occurred during style processing: {e}", exc_info=True)
                            style_op_success = False
                
                # 5. Only rename if BOTH data and style (if attempted) were successful
                if style_op_success:
                    processed_key = config_key.replace(CONFIG_FILE_NAME, PROCESSED_FILE_NAME)
                    source = CopySource(MINIO_BUCKET, config_key)
                    
                    minio_client.copy_object(MINIO_BUCKET, processed_key, source)
                    minio_client.remove_object(MINIO_BUCKET, config_key)
                    
                    logging.info(f"Successfully processed and renamed '{config_key}' to '{processed_key}' in MinIO.")
                    success_count += 1
                else:
                    logging.error(f"Data for '{config_key}' was published, but style operation failed. File will NOT be renamed and will be retried.")

        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from '{config_key}' in MinIO.")
        except S3Error as e:
            logging.error(f"S3 error while processing '{config_key}': {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred while processing '{config_key}': {e}", exc_info=True)

    logging.info(f"Publish cycle finished. Successfully processed {success_count} of {len(publish_requests)} requests.")


if __name__ == "__main__":
    while True:
        try:
            run_publish_cycle()
        except Exception as e:
            logging.error(f"Unhandled exception in main loop: {e}", exc_info=True)
            
        logging.info(f"--- Cycle finished. Waiting for {PUBLISH_INTERVAL_SECONDS} seconds... ---")
        time.sleep(PUBLISH_INTERVAL_SECONDS)