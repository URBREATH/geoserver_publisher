import os
import io
import zipfile
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

from config import GEOSERVER_URL, GEOSERVER_USER, GEOSERVER_PASSWORD, REQUEST_TIMEOUT
from utils.logger import logger


# Extensions that must travel alongside a .shp in the ZIP bundle.
# Broader than the previous list to include .sbn/.sbx/.fix/.shp.xml/.ain/.aih.
SHAPEFILE_SIDECARS = (
    ".shp", ".shx", ".dbf", ".prj", ".cpg", ".qpj",
    ".sbn", ".sbx", ".fix", ".ain", ".aih", ".shp.xml",
)


class GeoServerClient:
    def __init__(self):
        self.base_url     = f"{GEOSERVER_URL}/rest"
        self.auth         = HTTPBasicAuth(GEOSERVER_USER, GEOSERVER_PASSWORD)
        self.headers_json = {"Content-type": "application/json", "Accept": "application/json"}
        self.headers_sld  = {"Content-type": "application/vnd.ogc.sld+xml", "Accept": "application/json"}

    # -----------------------------------------------------------------
    # Low-level HTTP helpers
    # -----------------------------------------------------------------
    def _get(self, url):
        return requests.get(url, auth=self.auth, headers=self.headers_json, timeout=REQUEST_TIMEOUT)

    def _post_json(self, url, payload):
        return requests.post(url, json=payload, auth=self.auth, headers=self.headers_json,
                             timeout=REQUEST_TIMEOUT)

    def _put(self, url, data, content_type):
        return requests.put(url, data=data, auth=self.auth,
                            headers={"Content-type": content_type},
                            timeout=REQUEST_TIMEOUT)

    @staticmethod
    def _is_ok(resp):
        return resp.status_code in (200, 201)

    @staticmethod
    def _is_already_exists(resp):
        return resp.status_code == 500 and "already exists" in resp.text

    # -----------------------------------------------------------------
    # Workspace
    # -----------------------------------------------------------------
    def ensure_workspace(self, workspace):
        ws = quote(workspace, safe="")
        url = f"{self.base_url}/workspaces/{ws}"
        resp = self._get(url)

        if resp.status_code == 200:
            return True

        if resp.status_code == 404:
            logger.info(f"Workspace '{workspace}' not found. Creating...")
            payload = {"workspace": {"name": workspace}}
            create_resp = self._post_json(f"{self.base_url}/workspaces", payload)
            return create_resp.status_code == 201

        logger.error(f"Error checking workspace '{workspace}': {resp.status_code} - {resp.text}")
        return False

    # -----------------------------------------------------------------
    # Shapefile ZIP bundle
    # -----------------------------------------------------------------
    @staticmethod
    def _build_shapefile_zip(shp_path):
        """Builds an in-memory ZIP containing the .shp and all its companion files."""
        base_root, _ = os.path.splitext(shp_path)
        base_name    = os.path.basename(base_root)

        mem_zip = io.BytesIO()
        found   = 0
        with zipfile.ZipFile(mem_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
            for ext in SHAPEFILE_SIDECARS:
                candidate = base_root + ext
                if os.path.exists(candidate):
                    zf.write(candidate, arcname=base_name + ext)
                    found += 1

        if found == 0:
            raise FileNotFoundError(f"No files found for shapefile base: {shp_path}")

        mem_zip.seek(0)
        return mem_zip

    # -----------------------------------------------------------------
    # Unified upload primitive
    # -----------------------------------------------------------------
    def _upload(self, workspace, store_name, store_kind, endpoint, content_type,
                data, configure="first", extra_params=""):
        """
        Generic upload.
          store_kind    : "datastores" or "coveragestores"
          endpoint      : "file.shp" / "file.geojson" / "file.gpkg" / "file.geotiff"
          content_type  : MIME type of the body
          data          : bytes, file object, or in-memory buffer
          configure     : "first" or "all"
          extra_params  : optional query string fragment (e.g. "&coverageName=foo")

        Returns True on success (including the "already exists" idempotency case).
        """
        ws     = quote(workspace,  safe="")
        store  = quote(store_name, safe="")
        url    = (f"{self.base_url}/workspaces/{ws}/{store_kind}/{store}/{endpoint}"
                  f"?configure={configure}{extra_params}")

        try:
            logger.info(f"Uploading to {store_kind}/{store_name} via {endpoint}...")
            resp = self._put(url, data=data, content_type=content_type)

            if self._is_ok(resp):
                return True
            if self._is_already_exists(resp):
                logger.warning(f"Store '{store_name}' already exists. Treating as success.")
                return True

            logger.error(f"Upload failed ({endpoint}): {resp.status_code} - {resp.text}")
            return False

        except requests.RequestException as e:
            logger.error(f"HTTP error during upload ({endpoint}): {e}")
            return False

    # -----------------------------------------------------------------
    # Public publish_* methods
    # -----------------------------------------------------------------
    def publish_shapefile(self, workspace, store_name, local_path):
        """Returns layer_name on success, None on failure."""
        try:
            zip_buffer = self._build_shapefile_zip(local_path)
        except Exception as e:
            logger.error(f"Error creating shapefile ZIP: {e}")
            return None

        ok = self._upload(workspace, store_name, "datastores", "file.shp",
                          "application/zip", zip_buffer)
        if not ok:
            return None
        return os.path.splitext(os.path.basename(local_path))[0]

    def publish_geojson(self, workspace, store_name, local_path):
        """Returns layer_name on success, None on failure."""
        try:
            with open(local_path, 'rb') as f:
                ok = self._upload(workspace, store_name, "datastores", "file.geojson",
                                  "application/json", f)
        except OSError as e:
            logger.error(f"Cannot open GeoJSON file {local_path}: {e}")
            return None
        if not ok:
            return None
        return os.path.splitext(os.path.basename(local_path))[0]

    def publish_geotiff(self, workspace, store_name, local_path):
        """Returns layer_name on success, None on failure."""
        extra = f"&coverageName={quote(store_name, safe='')}"
        try:
            with open(local_path, 'rb') as f:
                ok = self._upload(workspace, store_name, "coveragestores", "file.geotiff",
                                  "image/tiff", f, extra_params=extra)
        except OSError as e:
            logger.error(f"Cannot open GeoTIFF file {local_path}: {e}")
            return None
        return store_name if ok else None

    def publish_geopackage(self, workspace, store_name, local_path):
        """
        Uses configure=all so ALL feature tables in the GPKG get published,
        not just the first one. Returns the list of real layer names fetched
        from GeoServer after the upload (layer names in a GPKG are derived
        from the feature table names, not from store_name).
        Returns [] on failure.
        """
        try:
            with open(local_path, 'rb') as f:
                ok = self._upload(workspace, store_name, "datastores", "file.gpkg",
                                  "application/octet-stream", f, configure="all")
        except OSError as e:
            logger.error(f"Cannot open GeoPackage file {local_path}: {e}")
            return []
        if not ok:
            return []

        layers = self.get_datastore_layers(workspace, store_name)
        if not layers:
            logger.warning(f"No layers discovered in store '{store_name}', "
                           f"falling back to store_name as layer name.")
            return [store_name]

        logger.info(f"GeoPackage '{store_name}' published with layers: {layers}")
        return layers

    # -----------------------------------------------------------------
    # Dispatcher — used by main.py
    # -----------------------------------------------------------------
    def publish_file(self, workspace, store_name, local_path):
        """
        Routes a local file to the right publisher based on its extension.
        Returns a list of layer names (possibly empty) so callers can treat
        single-layer and multi-layer sources uniformly.
        """
        ext = os.path.splitext(local_path)[1].lower()

        if ext == '.shp':
            name = self.publish_shapefile(workspace, store_name, local_path)
            return [name] if name else []

        if ext == '.geojson':
            name = self.publish_geojson(workspace, store_name, local_path)
            return [name] if name else []

        if ext in ('.tif', '.tiff'):
            name = self.publish_geotiff(workspace, store_name, local_path)
            return [name] if name else []

        if ext == '.gpkg':
            return self.publish_geopackage(workspace, store_name, local_path)

        logger.warning(f"Unsupported geographic extension: {ext}")
        return []

    # -----------------------------------------------------------------
    # Introspection
    # -----------------------------------------------------------------
    def get_datastore_layers(self, workspace, store_name):
        """Returns the list of featureType names configured inside a datastore."""
        ws    = quote(workspace,  safe="")
        store = quote(store_name, safe="")
        url   = f"{self.base_url}/workspaces/{ws}/datastores/{store}/featuretypes.json"
        try:
            resp = self._get(url)
            if resp.status_code == 200:
                data = resp.json()
                ft = data.get("featureTypes") or {}
                items = ft.get("featureType") or []
                return [item["name"] for item in items if "name" in item]
            logger.error(f"Could not list layers for store '{store_name}': "
                         f"{resp.status_code} - {resp.text}")
        except (requests.RequestException, ValueError) as e:
            logger.error(f"Error fetching layers for store '{store_name}': {e}")
        return []

    def get_layer_bbox(self, workspace, layer_name):
        """Retrieves the latLon bounding box for a published layer."""
        ws    = quote(workspace,  safe="")
        layer = quote(layer_name, safe="")
        default = "-180.0,-90.0,180.0,90.0"
        try:
            url = f"{self.base_url}/layers/{ws}:{layer}.json"
            r = self._get(url)
            if r.status_code != 200:
                return default
            res_href = r.json()['layer']['resource']['href']
            res_r = self._get(res_href)
            if res_r.status_code != 200:
                return default
            data = res_r.json()
            res = data.get('featureType') or data.get('coverage')
            if res and 'latLonBoundingBox' in res:
                bb = res['latLonBoundingBox']
                return f"{bb['minx']},{bb['miny']},{bb['maxx']},{bb['maxy']}"
        except (requests.RequestException, ValueError, KeyError) as e:
            logger.warning(f"Could not retrieve bbox for {workspace}:{layer_name}: {e}")
        return default

    # -----------------------------------------------------------------
    # Styles
    # -----------------------------------------------------------------
    def handle_style(self, workspace, style_name, sld_body, override=False):
        ws     = quote(workspace,  safe="")
        style  = quote(style_name, safe="")
        check_url = f"{self.base_url}/workspaces/{ws}/styles/{style}.json"
        try:
            check = self._get(check_url)
            if check.status_code == 200:
                if not override:
                    return True
                put_url = f"{self.base_url}/workspaces/{ws}/styles/{style}"
                resp = requests.put(put_url, data=sld_body.encode('utf-8'),
                                    auth=self.auth, headers=self.headers_sld,
                                    timeout=REQUEST_TIMEOUT)
                return resp.status_code == 200

            post_url = f"{self.base_url}/workspaces/{ws}/styles?name={style}"
            resp = requests.post(post_url, data=sld_body.encode('utf-8'),
                                 auth=self.auth, headers=self.headers_sld,
                                 timeout=REQUEST_TIMEOUT)
            return resp.status_code == 201
        except requests.RequestException as e:
            logger.error(f"Error handling style '{style_name}': {e}")
            return False

    def assign_style(self, workspace, layer_name, style_name):
        ws    = quote(workspace,  safe="")
        layer = quote(layer_name, safe="")
        url = f"{self.base_url}/layers/{ws}:{layer}"
        payload = {"layer": {"defaultStyle": {"name": style_name, "workspace": workspace}}}
        try:
            resp = requests.put(url, json=payload, auth=self.auth,
                                headers=self.headers_json, timeout=REQUEST_TIMEOUT)
            return resp.status_code == 200
        except requests.RequestException as e:
            logger.error(f"Error assigning style '{style_name}' to '{layer_name}': {e}")
            return False
