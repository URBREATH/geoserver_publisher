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
        # Se è una lista vecchia maniera, la adattiamo (retrocompatibilità opzionale)
        if isinstance(raw_conf, list):
             # Se arriva una lista piatta, non abbiamo il campo 'analysis' globale
             # Potremmo saltarla o gestirla come 'generic'. 
             # Per ora assumiamo arrivi la nuova struttura dict.
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
            
            if not workspace or not store_name or not data_path:
                conf['error_log'] = "Missing mandatory fields"
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
            is_geo = False
            layer_name = store_name # Default
            
            try:
                if data_path.endswith('.shp') or data_path.endswith('.geojson'):
                    is_geo = True
                    published = geo.publish_datastore(workspace, store_name, local_data)
                    if data_path.endswith('.shp'):
                        layer_name = os.path.splitext(os.path.basename(data_path))[0]
                elif data_path.endswith(('.tif', '.tiff')):
                    is_geo = True
                    published = geo.publish_coveragestore(workspace, store_name, local_data)
                else:
                    published = True # Non-geo file
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
            if is_geo and style_name and sld_path:
                local_sld = os.path.join(TARGET_DIR, sld_path)
                if os.path.exists(local_sld):
                    with open(local_sld, 'r') as f: sld_body = f.read()
                    geo.handle_style(workspace, style_name, sld_body, conf.get('override_style', False))
                    geo.assign_style(workspace, layer_name, style_name)

            # Raccogliamo i dati per IDRA
            if write_catalogue:
                bbox = geo.get_layer_bbox(workspace, layer_name) if is_geo else "-180,-90,180,90"
                
                # Aggiungiamo alla lista del "Bundle"
                resources_to_publish.append({
                    "workspace": workspace,
                    "layer_name": layer_name,
                    "data_path": data_path,
                    "sld_path": sld_path,
                    "style_name": style_name,
                    "bbox": bbox,
                    "is_geo": is_geo
                })

            success_items.append(conf)

        # 2. CHIAMATA UNICA A IDRA (Se ci sono risorse da pubblicare)
        if resources_to_publish:
            try:
                logger.info(f"Publishing bundle '{analysis_topic}' with {len(resources_to_publish)} resources to IDRA...")
                idra.publish_bundle(analysis_topic, city, date_val, resources_to_publish)
            except Exception as e:
                logger.error(f"IDRA Bundle Error: {e}")
                # Nota: qui potremmo decidere se segnare tutto come fallito o solo loggare.
                # Per ora consideriamo i file "processati" (GeoServer ok) ma IDRA ko.

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