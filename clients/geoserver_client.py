import os
import json
import requests
import zipfile
import io
from requests.auth import HTTPBasicAuth
from config import (GEOSERVER_URL, GEOSERVER_USER, GEOSERVER_PASSWORD, TARGET_DIR)
from utils.logger import logger

class GeoServerClient:
    def __init__(self):
        self.base_url = f"{GEOSERVER_URL}/rest"
        self.auth = HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD)
        self.headers_json = {"Content-type": "application/json", "Accept": "application/json"}
        self.headers_sld = {"Content-type": "application/vnd.ogc.sld+xml", "Accept": "application/json"}
        
        # Headers for binary file uploads
        self.headers_zip = {"Content-type": "application/zip"}
        self.headers_tiff = {"Content-type": "image/tiff"}

    def ensure_workspace(self, workspace):
        url = f"{self.base_url}/workspaces/{workspace}"
        resp = requests.get(url, auth=self.auth, headers=self.headers_json)
        
        if resp.status_code == 200:
            return True
        
        if resp.status_code == 404:
            logger.info(f"Workspace '{workspace}' not found. Creating...")
            payload = {"workspace": {"name": workspace}}
            create_resp = requests.post(f"{self.base_url}/workspaces", 
                                      json=payload, auth=self.auth, headers=self.headers_json)
            return create_resp.status_code == 201
        
        logger.error(f"Error checking workspace: {resp.status_code} - {resp.text}")
        return False

    def _create_shapefile_zip(self, shp_path):
        """
        Creates an in-memory ZIP file containing the .shp and its related files 
        (.shx, .dbf, .prj, .cpg, .qpj).
        """
        base_root, _ = os.path.splitext(shp_path)
        base_name = os.path.basename(base_root)
        
        mem_zip = io.BytesIO()
        
        try:
            with zipfile.ZipFile(mem_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
                # Possible extensions for a shapefile
                extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.qpj']
                found_files = 0
                
                for ext in extensions:
                    file_to_add = base_root + ext
                    if os.path.exists(file_to_add):
                        found_files += 1
                        # Add the file to the zip with a clean base name (e.g., layer.shp)
                        zf.write(file_to_add, arcname=base_name + ext)
                
                if found_files == 0:
                    raise FileNotFoundError(f"No files found for the base shapefile: {shp_path}")
                    
        except Exception as e:
            logger.error(f"Error creating shapefile ZIP: {e}")
            return None

        mem_zip.seek(0)
        return mem_zip

    def publish_datastore(self, workspace, store_name, local_path):
        """
        Publishes a Shapefile via DIRECT UPLOAD (PUT of a ZIP).
        It does not require GeoServer to see the local file system.
        """
        # Endpoint for shapefile upload: .../file.shp accepts a ZIP body
        url = f"{self.base_url}/workspaces/{workspace}/datastores/{store_name}/file.shp?configure=first"
        
        zip_buffer = self._create_shapefile_zip(local_path)
        if not zip_buffer:
            return False

        try:
            logger.info(f"Uploading Shapefile (ZIP) for store '{store_name}'...")
            resp = requests.put(url, data=zip_buffer, auth=self.auth, headers=self.headers_zip)
            
            if resp.status_code in [200, 201]:
                return True
            
            # Handle "Already exists" case (Idempotency)
            if resp.status_code == 500 and "already exists" in resp.text:
                logger.warning(f"DataStore '{store_name}' seems to already exist (500).")
                return True # We consider it a success to avoid blocking
                
            logger.error(f"Error Uploading Shapefile: {resp.status_code} - {resp.text}")
            return False
            
        except Exception as e:
            logger.error(f"Exception Uploading Shapefile: {e}")
            return False

    def publish_coveragestore(self, workspace, store_name, local_path):
        """
        Publishes a Raster (GeoTIFF) via DIRECT UPLOAD (PUT of the stream).
        It does not require GeoServer to see the local file system.
        """
        # Endpoint for raster file upload
        url = f"{self.base_url}/workspaces/{workspace}/coveragestores/{store_name}/file.geotiff?configure=first&coverageName={store_name}"
        
        try:
            logger.info(f"Uploading GeoTIFF (stream) for store '{store_name}' from {local_path}...")
            
            # Open in binary streaming
            with open(local_path, 'rb') as f:
                resp = requests.put(url, data=f, auth=self.auth, headers=self.headers_tiff)
            
            if resp.status_code in [200, 201]:
                return True

            if resp.status_code == 500 and "already exists" in resp.text:
                 logger.warning(f"CoverageStore '{store_name}' seems to already exist (500).")
                 return True

            logger.error(f"Error Uploading GeoTIFF: {resp.status_code} - {resp.text}")
            return False
            
        except Exception as e:
            logger.error(f"Exception Uploading GeoTIFF: {e}")
            return False

    def handle_style(self, workspace, style_name, sld_body, override=False):
        # 1. Check if the style exists
        check_url = f"{self.base_url}/workspaces/{workspace}/styles/{style_name}.json"
        check = requests.get(check_url, auth=self.auth, headers=self.headers_json)
        
        if check.status_code == 200:
            if not override:
                return True
            # Update (PUT)
            put_url = f"{self.base_url}/workspaces/{workspace}/styles/{style_name}"
            resp = requests.put(put_url, data=sld_body.encode('utf-8'), auth=self.auth, headers=self.headers_sld)
            return resp.status_code == 200

        # 2. Create (POST)
        post_url = f"{self.base_url}/workspaces/{workspace}/styles?name={style_name}"
        resp = requests.post(post_url, data=sld_body.encode('utf-8'), auth=self.auth, headers=self.headers_sld)
        return resp.status_code == 201

    def assign_style(self, workspace, layer_name, style_name):
        url = f"{self.base_url}/layers/{workspace}:{layer_name}"
        payload = {"layer": {"defaultStyle": {"name": f"{workspace}:{style_name}"}}}
        resp = requests.put(url, json=payload, auth=self.auth, headers=self.headers_json)
        return resp.status_code == 200

    def get_layer_bbox(self, workspace, layer_name):
        """Retrieves the Bounding Box from the published layer to pass it to IDRA."""
        try:
            url = f"{self.base_url}/layers/{workspace}:{layer_name}.json"
            r = requests.get(url, auth=self.auth, headers=self.headers_json)
            if r.status_code == 200:
                res_href = r.json()['layer']['resource']['href']
                res_r = requests.get(res_href, auth=self.auth, headers=self.headers_json)
                if res_r.status_code == 200:
                    data = res_r.json()
                    res = data.get('featureType') or data.get('coverage')
                    if res and 'latLonBoundingBox' in res:
                        bb = res['latLonBoundingBox']
                        return f"{bb['minx']},{bb['miny']},{bb['maxx']},{bb['maxy']}"
        except Exception:
            pass
        return "-180.0,-90.0,180.0,90.0"

    # get_geoserver_path has been removed because it is no longer needed with the API upload.
    # We directly use the local path of the publisher container.