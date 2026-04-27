import os
import json
import time

from config import (TARGET_DIR, PUBLISH_INTERVAL_SECONDS,
                    CONFIG_FILE_NAME, PROCESSED_FILE_NAME, FAILURE_FILE_NAME)
from utils.logger import logger
from clients.minio_client import MinioWrapper
from clients.geoserver_client import GeoServerClient
from clients.idra_client import IdraClient, parse_date_from_key

# File extensions that can be published to GeoServer.
GEO_EXTENSIONS = ('.shp', '.geojson', '.tif', '.tiff', '.gpkg')


def _safe_local_path(data_path):
    """
    Safely joins TARGET_DIR with a user-provided relative path.
    Prevents absolute paths (data_path='/etc/passwd') from silently bypassing
    TARGET_DIR — which is what happens with a naive os.path.join.
    """
    clean = data_path.lstrip('/\\')
    return os.path.join(TARGET_DIR, clean)


def _process_layer(conf, geo):
    """
    Processes a single config entry.
    Returns:
        (success: bool, layers: list[str])
    Sets conf['error_log'] on failure.
    """
    workspace  = conf.get('workspace')
    store_name = conf.get('store_name')
    data_path  = conf.get('data_path')

    if not data_path:
        conf['error_log'] = "Missing data_path"
        return False, []

    is_geo = data_path.lower().endswith(GEO_EXTENSIONS)
    if not is_geo:
        # Non-geo file (e.g. PDF): nothing to do on GeoServer; caller may still
        # send it to IDRA as a raw download.
        return True, []

    if not workspace or not store_name:
        conf['error_log'] = "Missing workspace or store_name for geo file"
        return False, []

    if not geo.ensure_workspace(workspace):
        conf['error_log'] = "Workspace error"
        return False, []

    local_data = _safe_local_path(data_path)
    if not os.path.exists(local_data):
        conf['error_log'] = f"File missing: {local_data}"
        return False, []

    try:
        layers = geo.publish_file(workspace, store_name, local_data)
    except Exception as e:
        conf['error_log'] = f"GeoServer publish exception: {e}"
        return False, []

    if not layers:
        conf['error_log'] = "GeoServer publish failed"
        return False, []

    # Optional style handling (applied to every published layer).
    style_name = conf.get('style_name')
    sld_path   = conf.get('sld_path')
    if style_name and sld_path:
        local_sld = _safe_local_path(sld_path)
        if os.path.exists(local_sld):
            try:
                with open(local_sld, 'r', encoding='utf-8') as f:
                    sld_body = f.read()
                geo.handle_style(workspace, style_name, sld_body,
                                 conf.get('override_style', False))
                for lname in layers:
                    geo.assign_style(workspace, lname, style_name)
            except OSError as e:
                logger.warning(f"Could not read SLD {local_sld}: {e}")
        else:
            logger.warning(f"SLD file not found: {local_sld}")

    return True, layers


def _build_idra_resources(conf, layers, geo):
    """
    Builds the IDRA resource descriptors for a single config entry.
    Returns a list (one item per published layer, or one item for a raw file).
    """
    if not conf.get('write_on_catalogue', False):
        return []

    data_path = conf['data_path']
    is_geo    = data_path.lower().endswith(GEO_EXTENSIONS)

    if not is_geo:
        return [{
            "workspace":   conf.get('workspace'),
            "layer_name":  conf.get('store_name') or os.path.splitext(os.path.basename(data_path))[0],
            "data_path":   data_path,
            "sld_path":    None,
            "style_name":  None,
            "bbox":        "-180,-90,180,90",
            "is_geo":      False,
            "custom_desc": conf.get('description'),
        }]

    workspace = conf['workspace']
    return [{
        "workspace":   workspace,
        "layer_name":  lname,
        "data_path":   data_path,
        "sld_path":    conf.get('sld_path'),
        "style_name":  conf.get('style_name'),
        "bbox":        geo.get_layer_bbox(workspace, lname),
        "is_geo":      True,
        "custom_desc": conf.get('description'),
    } for lname in layers]


def _process_request(config_key, minio, geo, idra):
    """Processes a single _publish.json request found in MinIO."""
    try:
        raw_conf = minio.read_config(config_key)
    except json.JSONDecodeError:
        minio.move_to_corrupted(config_key)
        return

    if not raw_conf:
        logger.warning(f"Empty configuration for {config_key}, skipping.")
        return

    # Normalize both the modern ({analysis, data}) and legacy (bare list) shapes.
    if isinstance(raw_conf, list):
        logger.warning(f"Legacy list format in {config_key}; using generic analysis.")
        analysis_topic, layers_config = "Generic Import", raw_conf
    else:
        analysis_topic = raw_conf.get('analysis', 'Unknown Analysis')
        layers_config  = raw_conf.get('data', [])

    if not layers_config:
        logger.warning(f"No data entries in {config_key}")
        return

    # Context extraction.
    city     = config_key.split('/', 1)[0] if '/' in config_key else "Unknown"
    date_val = parse_date_from_key(config_key)

    success_items, failure_items, resources_to_publish = [], [], []

    for conf in layers_config:
        ok, layers = _process_layer(conf, geo)
        if ok:
            resources_to_publish.extend(_build_idra_resources(conf, layers, geo))
            success_items.append(conf)
        else:
            failure_items.append(conf)

    if resources_to_publish:
        logger.info(f"Publishing IDRA bundle '{analysis_topic}' with "
                    f"{len(resources_to_publish)} resource(s)...")
        try:
            idra.publish_bundle(analysis_topic, city, date_val, resources_to_publish)
        except Exception as e:
            logger.error(f"IDRA bundle error: {e}")

    # Finalize MinIO: write outcome files and remove the trigger.
    if success_items:
        minio.save_json(
            config_key.replace(CONFIG_FILE_NAME, PROCESSED_FILE_NAME),
            {"analysis": analysis_topic, "data": success_items},
        )
    if failure_items:
        minio.save_json(
            config_key.replace(CONFIG_FILE_NAME, FAILURE_FILE_NAME),
            failure_items,
        )
    minio.delete_file(config_key)


def run_cycle(minio, geo, idra):
    logger.info("--- Starting scan cycle ---")
    pending = minio.find_pending_requests()
    if not pending:
        return
    logger.info(f"Found {len(pending)} request(s).")
    for key in pending:
        try:
            _process_request(key, minio, geo, idra)
        except Exception as e:
            logger.error(f"Error processing {key}: {e}", exc_info=True)


def main():
    try:
        minio_svc = MinioWrapper()
        geo_svc   = GeoServerClient()
        idra_svc  = IdraClient()
    except Exception as e:
        logger.critical(f"Critical startup error: {e}")
        return

    while True:
        try:
            run_cycle(minio_svc, geo_svc, idra_svc)
        except Exception as e:
            logger.error(f"Loop error: {e}", exc_info=True)
        time.sleep(PUBLISH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
