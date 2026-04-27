import os
import time
import json
import re
from datetime import datetime 
from config import TARGET_DIR, PUBLISH_INTERVAL_SECONDS, PROCESSED_FILE_NAME, CONFIG_FILE_NAME
from utils.logger import logger
from clients.minio_client import MinioWrapper
from clients.geoserver_client import GeoServerClient
from clients.idra_client import IdraClient

def run_cycle(minio, geo, idra):
    logger.info("--- Starting scan cycle ---")
    requests_list = minio.find_pending_requests()
    
    if not requests_list: return

    logger.info(f"Found {len(requests_list)} requests.")
    
    for config_key in requests_list:
        try:
            raw_conf = minio.read_config(config_key)
        except json.JSONDecodeError:
            minio.move_to_corrupted(config_key)
            continue

        if not raw_conf: 
            logger.warning(f"Empty configuration for {config_key}, skipping.")
            continue

        # --- GESTIONE NUOVA STRUTTURA JSON ---
        if isinstance(raw_conf, list):
             logger.warning(f"Legacy list format detected for {config_key}. Expecting dict with 'analysis' and 'data'.")
             analysis_topic = "Generic Import"
             layers_config = raw_conf
        else:
             analysis_topic = raw_conf.get('analysis', 'Unknown Analysis')
             layers_config = raw_conf.get('data', [])

        if not layers_config:
            logger.warning(f"No data found in {config_key}")
            continue

        # --- ESTRAZIONE DATA/CITTA DAL PATH ---
        parts = config_key.split('/')
        city = parts[0] if len(parts) > 0 else "Unknown"

        date_val = datetime.now().strftime("%Y-%m-%d")
        # Regex 1: YYYY-MM-DD
        date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', config_key)
        if date_match:
            date_val = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
        else:
            # Regex 2: YYYYMMDD
            date_match_compact = re.search(r'(20\d{2}|19\d{2})(\d{2})(\d{2})', config_key)
            if date_match_compact:
                date_val = f"{date_match_compact.group(1)}-{date_match_compact.group(2)}-{date_match_compact.group(3)}"
        # ---------------------------------------

        success_items = []
        failure_items = []
        
        # Lista accumulatore per IDRA
        resources_to_publish = []

        # 1. CICLO DI PREPARAZIONE (MinIO -> GeoServer)
        for conf in layers_config:
            workspace = conf.get('workspace')
            store_name = conf.get('store_name')
            data_path = conf.get('data_path')
            write_catalogue = conf.get('write_on_catalogue', False)
            
            if not data_path:
                conf['error_log'] = "Missing data_path"
                failure_items.append(conf)
                continue

            # Determiniamo SUBITO se il file è geografico in base all'estensione
            is_geo = data_path.lower().endswith(('.shp', '.geojson', '.tif', '.tiff', '.gpkg'))
            
            # Se è un file non geografico (es. PDF), possiamo usare un nome generico se manca store_name
            layer_name = store_name or "raw_file"
            
            # --- LOGICA PER FILE GEOGRAFICI ---
            if is_geo:
                if not workspace or not store_name:
                    conf['error_log'] = "Missing workspace or store_name for geo file"
                    failure_items.append(conf)
                    continue

                # Check Locali e GeoServer
                if not geo.ensure_workspace(workspace):
                    conf['error_log'] = "Workspace error"
                    failure_items.append(conf)
                    continue

                local_data = os.path.join(TARGET_DIR, data_path)
                if not os.path.exists(local_data):
                    conf['error_log'] = f"File missing: {local_data}"
                    failure_items.append(conf)
                    continue

                # Pubblicazione GeoServer
                published = False
                try:
                    if data_path.endswith('.shp') or data_path.endswith('.geojson'):
                        published = geo.publish_datastore(workspace, store_name, local_data)
                        if data_path.endswith('.shp'):
                            layer_name = os.path.splitext(os.path.basename(data_path))[0]
                    elif data_path.endswith(('.tif', '.tiff')):
                        published = geo.publish_coveragestore(workspace, store_name, local_data)
                    elif data_path.endswith('.gpkg'):
                        published = geo.publish_geopackage(workspace, store_name, local_data)
                except Exception as e:
                    conf['error_log'] = str(e)
                    failure_items.append(conf)
                    continue

                if not published:
                    conf['error_log'] = "GeoServer publish failed"
                    failure_items.append(conf)
                    continue

                # Gestione Stili
                style_name = conf.get('style_name')
                sld_path = conf.get('sld_path')
                if style_name and sld_path:
                    local_sld = os.path.join(TARGET_DIR, sld_path)
                    if os.path.exists(local_sld):
                        with open(local_sld, 'r') as f: sld_body = f.read()
                        geo.handle_style(workspace, style_name, sld_body, conf.get('override_style', False))
                        geo.assign_style(workspace, layer_name, style_name)

            # --- RACCOLTA DATI PER IDRA (Per tutti i file) ---
            if write_catalogue:
                # Se è geo prendiamo la BBOX, altrimenti diamo i limiti del mondo
                bbox = geo.get_layer_bbox(workspace, layer_name) if is_geo else "-180,-90,180,90"
                
                # Aggiungiamo alla lista del "Bundle"
                resources_to_publish.append({
                    "workspace": workspace,
                    "layer_name": layer_name,
                    "data_path": data_path,
                    "sld_path": conf.get('sld_path'),
                    "style_name": conf.get('style_name'),
                    "bbox": bbox,
                    "is_geo": is_geo,
                    "custom_desc": conf.get('description') # Passiamo la custom description al JSON
                })

            success_items.append(conf)

        # 2. CHIAMATA UNICA A IDRA (Se ci sono risorse da pubblicare)
        if resources_to_publish:
            try:
                logger.info(f"Publishing bundle '{analysis_topic}' with {len(resources_to_publish)} resources to IDRA...")
                idra.publish_bundle(analysis_topic, city, date_val, resources_to_publish)
            except Exception as e:
                logger.error(f"IDRA Bundle Error: {e}")

        # 3. Finalizzazione JSON
        if success_items:
            # Salviamo la struttura completa processata
            final_json = {"analysis": analysis_topic, "data": success_items}
            minio.save_json(config_key.replace(CONFIG_FILE_NAME, PROCESSED_FILE_NAME), final_json)
            
        if failure_items:
            minio.save_json(config_key.replace(CONFIG_FILE_NAME, "_failures.json"), failure_items)

        minio.delete_file(config_key)

if __name__ == "__main__":
    try:
        minio_svc = MinioWrapper()
        geo_svc = GeoServerClient()
        idra_svc = IdraClient()
        
        while True:
            try:
                run_cycle(minio_svc, geo_svc, idra_svc)
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except Exception as e:
        logger.critical(f"Critical startup error: {e}")