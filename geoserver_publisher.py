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

IDRA_URL = os.environ.get('IDRA_URL') # e.g., http://idra-broker:8080
GEOSERVER_PUBLIC_URL = os.environ.get('GEOSERVER_PUBLIC_URL', GEOSERVER_URL) # Public URL for download links

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

def upload_style(workspace, style_name, sld_body, style_override_roule=False):
    """Uploads a style (SLD) to GeoServer, but only if it doesn't already exist."""
    logging.info(f"Checking for style '{style_name}' in workspace '{workspace}'...")

    # Check if the style already exists https://geoserver-dev.urbreath.tech/geoserver/rest//workspaces/Tallinn_3-30-300/styles/style_3.json
    check_url = f"{base_rest_url}/workspaces/{workspace}/styles/{style_name}.json"
    check_response = requests.get(check_url, auth=auth, headers=headers_json)

    if check_response.status_code == 200: # Style exists
        if not style_override_roule:
            logging.info(f"Style '{style_name}' already exists and override is disabled. Skipping upload.")
            return True
        else:
            logging.info(f"Style '{style_name}' already exists and override is enabled. Deleting it first...")
            delete_url = f"{base_rest_url}/workspaces/{workspace}/styles/{style_name}"
            delete_response = requests.delete(delete_url, auth=auth)
            if delete_response.status_code == 200:
                logging.info(f"Successfully deleted existing style '{style_name}'.")
                # Now we let the code fall through to the creation part
            else:
                logging.error(f"Failed to delete existing style '{style_name}'. Status: {delete_response.status_code}, Text: {delete_response.text}")
                return False

    
    # If not found (404), then proceed to create it
    if check_response.status_code == 404:
        logging.info(f"Style '{style_name}' not found. Proceeding with creation...")
        
        post_url = f"{base_rest_url}/workspaces/{workspace}/styles?name={style_name}"
        headers_sld = {"Content-type": "application/vnd.ogc.sld+xml", "Accept": "application/json"}
        
        post_response = requests.post(post_url, data=sld_body.encode('utf-8'), auth=auth, headers=headers_sld)
        
        if post_response.status_code == 201:
            logging.info(f"Style '{style_name}' created successfully.")
            return True
        else:
            logging.error(f"Failed to create style '{style_name}'. Status: {post_response.status_code}, Text: {post_response.text}")
            return False
            
    # Handle other unexpected status codes from the check
    logging.error(f"Error checking for style '{style_name}'. Status: {check_response.status_code}, Text: {check_response.text}")
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

def publish_to_idra(workspace, layer_name, description, layer_id, city, date_val, kpi_type, data_file_name, sld_file_name=None, style_name=None):
    """Publishes metadata to the IDRA NGSI-LD Broker."""
    if not IDRA_URL:
        logging.warning("IDRA_URL is not set. Skipping publication to catalogue.")
        return True # Return True to not block the workflow

    logging.info(f"Attempting to publish metadata for layer '{layer_name}' to IDRA catalogue.")

    # Load templates
    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'distribution_template.json')
    templates = []
    if os.path.exists(template_path):
        try:
            with open(template_path, 'r') as f:
                templates = json.load(f)
            if not isinstance(templates, list):
                templates = [templates]
        except Exception as e:
            logging.error(f"Error loading distribution_template.json: {e}")

    def find_template_in_json(filename):
        """Searches for a matching template in the loaded JSON list."""
        if not filename: return None
        
        filename_norm = filename.lower().strip()
        
        for t in templates:
            # Get the pattern from the template using the correct key 'File name'
            tmpl_pattern = t.get('File name')
            if not tmpl_pattern: continue
            
            # Normalize the template pattern
            # 1. Remove known placeholders
            pattern_norm = tmpl_pattern.replace('[City name]', '').replace('[dd-mm-yyyy]', '')
            # 2. Remove extra whitespace
            pattern_norm = " ".join(pattern_norm.split())
            # 3. Fix space before extension (e.g. " .tif" -> ".tif")
            pattern_norm = pattern_norm.replace(' .', '.')
            # 4. Lowercase
            pattern_norm = pattern_norm.lower()
            
            # Check for match (equality or containment for robustness)
            if pattern_norm == filename_norm:
                return t
            
            # Fallback: Check containment but ensure extensions match
            if (pattern_norm in filename_norm or filename_norm in pattern_norm):
                 ext_f = os.path.splitext(filename_norm)[1]
                 ext_p = os.path.splitext(pattern_norm)[1]
                 if ext_f == ext_p and ext_f:
                     return t
                     
        return None

    def replace_placeholders(text):
        if not isinstance(text, str): return text
        return text.replace("{city}", city).replace("{date}", date_val).replace("{layer_name}", layer_name)

    distribution_ids = []
    # 1. Publish Distribution (how to access the data)
    distribution_url = f"{IDRA_URL}/api/distributiondcatap"
    
    # Calculate BBOX for WMS URL
    bbox_str = "-180.0,-90.0,180.0,90.0"
    try:
        layer_url = f"{base_rest_url}/layers/{workspace}:{layer_name}.json"
        resp_layer = requests.get(layer_url, auth=auth, headers=headers_json)
        if resp_layer.status_code == 200:
            resource_href = resp_layer.json()['layer']['resource']['href']
            resp_res = requests.get(resource_href, auth=auth, headers=headers_json)
            if resp_res.status_code == 200:
                res_data = resp_res.json()
                resource = res_data.get('featureType') or res_data.get('coverage')
                if resource and 'latLonBoundingBox' in resource:
                    bb = resource['latLonBoundingBox']
                    bbox_str = f"{bb['minx']},{bb['miny']},{bb['maxx']},{bb['maxy']}"
    except Exception as e:
        logging.warning(f"Could not fetch BBOX for layer '{layer_name}': {e}")

    style_param = style_name if style_name else ""
    download_url = (f"{GEOSERVER_PUBLIC_URL}/{workspace}/wms?service=WMS&version=1.1.1"
                    f"&request=GetMap&layers={workspace}:{layer_name}&styles={style_param}"
                    f"&bbox={bbox_str}&width=768&height=330&srs=EPSG:4326&format=image/png")

    # Prepare default values
    dist_description = description
    dist_format = "image/png"
    dist_license = None

    # Apply template for Data File
    data_tmpl = find_template_in_json(data_file_name)
    if data_tmpl:
        logging.info(f"Found matching template for '{data_file_name}'. KPI: {data_tmpl.get('KPI')}")
        if 'Description' in data_tmpl:
            dist_description = replace_placeholders(data_tmpl['Description'])
        if 'Format' in data_tmpl:
            dist_format = data_tmpl['Format']
        if 'License' in data_tmpl:
            dist_license = data_tmpl['License']

    distribution_body = {
        "id": layer_id,
        "title": layer_name,
        "description": dist_description,
        "downloadURL": download_url,
        "format": dist_format
    }

    if dist_license:
        distribution_body["license"] = dist_license

    try:
        response_dist = requests.post(distribution_url, json=distribution_body)
        # We log but don't fail hard on this, as the Dataset is the main entity
        if response_dist.status_code in [200, 201, 204]:
             logging.info(f"IDRA Distribution for '{layer_id}' created/updated successfully. Status: {response_dist.status_code}")
             distribution_ids.append(layer_id)
        else:
             logging.warning(f"Failed to create IDRA Distribution for '{layer_id}'. Status: {response_dist.status_code}, Text: {response_dist.text}")
    except requests.RequestException as e:
        logging.error(f"Error calling IDRA for distribution creation: {e}")
        # Continue to dataset creation anyway, as it's the primary record

    # 2. Publish SLD Distribution (if applicable)
    if sld_file_name and style_name:
        sld_tmpl = find_template_in_json(sld_file_name)
        if sld_tmpl:
            logging.info(f"Found matching template for '{sld_file_name}'. KPI: {sld_tmpl.get('KPI')}")
            sld_dist_id = f"{layer_id}_sld"
            sld_url = f"{GEOSERVER_PUBLIC_URL}/workspaces/{workspace}/styles/{style_name}.sld"
            
            sld_body = {
                "id": sld_dist_id,
                "title": f"Style for {layer_name}",
                "description": replace_placeholders(sld_tmpl.get('Description', f"Style for {layer_name}")),
                "downloadURL": sld_url,
                "format": sld_tmpl.get('Format', "application/vnd.ogc.sld+xml"),
                "license": sld_tmpl.get('License', "Unknown")
            }
            
            try:
                resp_sld = requests.post(distribution_url, json=sld_body)
                if resp_sld.status_code in [200, 201, 204]:
                    logging.info(f"IDRA SLD Distribution '{sld_dist_id}' created.")
                    distribution_ids.append(sld_dist_id)
                else:
                    logging.warning(f"Failed to create IDRA SLD Distribution '{sld_dist_id}'. Status: {resp_sld.status_code}, Text: {resp_sld.text}")
            except Exception as e:
                logging.error(f"Error creating SLD distribution: {e}")

    # 3. Publish Dataset (the metadata record)
    dataset_url = f"{IDRA_URL}/api/dataset"
    dataset_id = f"{workspace}:{layer_id}"
    dataset_body = {
        "id": dataset_id,
        "title": layer_name,
        "description": description,
        "datasetDescription": [description],
        "datasetDistribution": distribution_ids # Link to the distributions
    }

    try:
        response_dataset = requests.post(dataset_url, json=dataset_body)
        if response_dataset.status_code in [200, 201, 204]:
            logging.info(f"IDRA Dataset '{dataset_id}' published successfully. Status: {response_dataset.status_code}")
            return True
        logging.error(f"Failed to publish IDRA Dataset '{dataset_id}'. Status: {response_dataset.status_code}, Text: {response_dataset.text}")
        return False
    except requests.RequestException as e:
        logging.error(f"Fatal error calling IDRA for dataset creation: {e}")
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
            
            # Extract required and optional fields from the config
            workspace = config['workspace']
            store_name = config['store_name']
            data_path_rel = config['data_path']
            style_name = config.get('style_name')
            sld_path_rel = config.get('sld_path')
            style_override_roule = config.get('override_style', False)
            write_on_catalogue = config.get('write_on_catalogue', False)
            description = config.get('description', f"Data layer {store_name}")
            layer_id = config.get('id', store_name) # Use 'id' from config or default to store_name
            kpi_type = config.get('type')
            
            # Extract City and Date from MinIO path
            path_parts = config_key.split('/')
            city_name = path_parts[0] if len(path_parts) > 0 else "Unknown"
            date_val = path_parts[1] if len(path_parts) > 1 else "Unknown"
            
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
            elif data_path_rel.lower().endswith('.geojson'):
                data_path_for_geoserver = get_geoserver_path(data_path_local_publisher)
                if not data_path_for_geoserver:
                    continue
                published = publish_datastore(workspace, store_name, data_path_for_geoserver)
                layer_name_for_style = store_name
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
                            
                            style_uploaded = upload_style(workspace, style_name, sld_body, style_override_roule)
                            
                            if style_uploaded:
                                if not assign_style_to_layer(workspace, layer_name_for_style, style_name):
                                    style_op_success = False
                            else:
                                style_op_success = False
                                
                        except Exception as e:
                            logging.error(f"An error occurred during style processing: {e}", exc_info=True)
                            style_op_success = False
                
                # 5. Publish to IDRA catalogue if requested and previous steps were successful
                idra_op_success = True
                if style_op_success and write_on_catalogue:
                    data_file_name = os.path.basename(data_path_rel)
                    sld_file_name = os.path.basename(sld_path_rel) if sld_path_rel else None
                    
                    idra_op_success = publish_to_idra(workspace, layer_name_for_style, description, layer_id, city_name, date_val, kpi_type, data_file_name, sld_file_name, style_name)

                # 6. Only rename if ALL requested operations were successful
                if style_op_success and idra_op_success:
                    processed_key = config_key.replace(CONFIG_FILE_NAME, PROCESSED_FILE_NAME)
                    source = CopySource(MINIO_BUCKET, config_key)
                    
                    minio_client.copy_object(MINIO_BUCKET, processed_key, source)
                    minio_client.remove_object(MINIO_BUCKET, config_key)
                    
                    logging.info(f"Successfully processed all steps for '{config_key}' and renamed to '{processed_key}' in MinIO.")
                    success_count += 1
                else:
                    error_msg = f"Processing for '{config_key}' failed at a final step and will be retried. "
                    if not style_op_success:
                        error_msg += "Reason: Style operation failed. "
                    if not idra_op_success:
                        error_msg += "Reason: IDRA catalogue publication failed."
                    logging.error(error_msg)

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