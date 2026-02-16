import os
import json
import requests
import re
import mimetypes
from datetime import datetime
from config import (IDRA_URL, GEOSERVER_PUBLIC_URL, DISTRIBUTION_TEMPLATE_PATH, 
                    DATASET_TEMPLATE_PATH, MINIO_PROXY_URL, MINIO_BUCKET)
from utils.logger import logger

class IdraClient:
    def __init__(self):
        self.enabled = bool(IDRA_URL)
        self.dist_templates = self._load_json(DISTRIBUTION_TEMPLATE_PATH)
        self.dataset_templates = self._load_json(DATASET_TEMPLATE_PATH)
        mimetypes.init() 

    def _load_json(self, path):
        if not os.path.exists(path):
            return []
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                return data if isinstance(data, list) else [data]
        except Exception as e:
            logger.error(f"Error loading template {path}: {e}")
            return []

    def _match_distribution(self, filename):
        """FUZZY MATCHING (Logica a token come richiesto prima)"""
        if not filename: return None, {}
        
        filename_tokens = set(re.split(r'[_\-\.]+', filename.lower()))
        best_tmpl = None
        best_score = 0
        
        for tmpl in self.dist_templates:
            pattern_str = tmpl.get('file_pattern', '')
            if not pattern_str: continue
            
            pattern_clean = re.sub(r'\{.*?\}', '', pattern_str)
            pattern_tokens = {t for t in set(re.split(r'[_\-\.]+', pattern_clean.lower())) if t}

            if not pattern_tokens: continue

            common_tokens = filename_tokens.intersection(pattern_tokens)
            score = len(common_tokens) / len(pattern_tokens)
            
            if score > 0.40 and score > best_score:
                best_score = score
                best_tmpl = tmpl

        return best_tmpl, {}

    def _find_dataset_template(self, analysis_name):
        """Trova il template del dataset basandosi sul campo 'analysis' del JSON"""
        if not analysis_name: return None
        for ds_tmpl in self.dataset_templates:
            # Confronta con il campo KPI del template
            if ds_tmpl.get('KPI').lower().strip() == analysis_name.lower().strip():
                return ds_tmpl
        return None

    def _upsert_resource(self, endpoint, payload, resource_id):
        """Metodo helper per chiamare IDRA"""
        base_api = f"{IDRA_URL}/api/{endpoint}"
        try:
            get_resp = requests.get(f"{base_api}/{resource_id}")
            if get_resp.status_code == 200:
                logger.info(f"Resource {resource_id} exists. Skipping.")
                return True
            
            logger.info(f"Creating IDRA resource: {resource_id}")
            post_resp = requests.post(base_api, json=payload)
            
            if post_resp.status_code in [200, 201]:
                return True
            if post_resp.status_code == 409:
                return True # Conflitto gestito

            logger.error(f"FAIL CREATE {resource_id}: {post_resp.status_code} - {post_resp.text}")
            return False
        except Exception as e:
            logger.error(f"IDRA Request Error {resource_id}: {e}")
            return False

    def publish_bundle(self, analysis_topic, city, date_val, resources_list):
        """
        Pubblica UN dataset per l'analisi indicata e N distribuzioni (una per ogni file).
        
        :param analysis_topic: Stringa 'analysis' dal JSON (es. "urban heat islands")
        :param city: Nome città (dal path)
        :param date_val: Data formattata YYYY-MM-DD
        :param resources_list: Lista di dizionari con i dettagli dei layer processati
        """
        if not self.enabled: return True
        
        # 1. Preparazione Contesto
        date_dmy = date_val
        try:
            parts = date_val.split('-')
            if len(parts) == 3: date_dmy = f"{parts[2]}-{parts[1]}-{parts[0]}"
        except: pass

        context = {
            "city": city,
            "date": date_val,
            "date_dmy": date_dmy,
            "KPI": analysis_topic
        }

        # 2. Setup Dataset Base
        ds_tmpl = self._find_dataset_template(analysis_topic)
        
        # ID Univoco per il Dataset (Timestamp generato una volta per tutto il bundle)
        timestamp_suffix = datetime.now().strftime("%Y%m%d-%H%M%S")
        dataset_unique_id = f"{city}_{analysis_topic.replace(' ', '_')}_{timestamp_suffix}"
        
        # Metadati Dataset
        final_dataset_title = f"{city} {analysis_topic} {date_dmy}"
        final_dataset_desc = f"Dataset for {analysis_topic} in {city}"
        keywords = []
        author_name, author_email, theme, publisher = None, None, None, None

        if ds_tmpl:
            if 'dataset_title' in ds_tmpl: final_dataset_title = ds_tmpl['dataset_title'].format(**context)
            if 'description' in ds_tmpl: final_dataset_desc = ds_tmpl['description'].format(**context)
            if 'keywords' in ds_tmpl:
                raw_kw = ds_tmpl['keywords']
                keywords = [k.format(**context) for k in raw_kw] if isinstance(raw_kw, list) else [raw_kw.format(**context)]
            author_name = ds_tmpl.get('author_name')
            author_email = ds_tmpl.get('author_email')
            theme = ds_tmpl.get('theme')
            publisher = ds_tmpl.get('publisher')

        # 3. Creazione Distribuzioni (Ciclo sui file)
        dist_ids = []

        for res in resources_list:
            data_path = res['data_path']
            layer_name = res['layer_name'] # Store name su GeoServer
            workspace = res['workspace']
            sld_path = res.get('sld_path')
            style_name = res.get('style_name')
            bbox = res.get('bbox')
            is_geo = res.get('is_geo', True)
            
            filename = os.path.basename(data_path)
            
            # Fuzzy match sul singolo file per trovare il titolo specifico della distribution
            dist_tmpl, _ = self._match_distribution(filename)
            
            # Helper interno per aggiungere distribution
            def add_single_dist(suffix, url, fmt, title_suffix=""):
                dist_id = f"{dataset_unique_id}_{layer_name}_{suffix}"
                
                # Titolo della distribuzione
                d_title = dist_tmpl.get('dataset_title', filename) if dist_tmpl else filename
                if title_suffix: d_title += f" ({title_suffix})"
                
                # Descrizione della distribuzione
                d_desc = dist_tmpl.get('description', '').format(**context) if dist_tmpl else final_dataset_desc

                body = {
                    "id": dist_id,
                    "title": d_title,
                    "description": d_desc,
                    "downloadURL": url,
                    "accessURL": url,
                    "format": fmt
                }
                if dist_tmpl and 'license' in dist_tmpl:
                    body["license"] = dist_tmpl['license']

                if self._upsert_resource("distributiondcatap", body, dist_id):
                    dist_ids.append(dist_id)

            # URL
            raw_data_url = f"{MINIO_PROXY_URL}/browser/{MINIO_BUCKET}/{data_path}"
            
            # Logica Geo vs Raw
            if not is_geo:
                mime = dist_tmpl.get('format') if dist_tmpl else mimetypes.guess_type(data_path)[0]
                add_single_dist("download", raw_data_url, mime or "application/octet-stream")
            else:
                # 1. Raw Data
                fmt_raw = dist_tmpl.get('format', "application/octet-stream") if dist_tmpl else "application/octet-stream"
                add_single_dist("raw_data", raw_data_url, fmt_raw, "Raw Data")
                
                # 2. SLD (se presente)
                if sld_path:
                    raw_sld_url = f"{MINIO_PROXY_URL}/browser/{MINIO_BUCKET}/{sld_path}"
                    add_single_dist("style", raw_sld_url, "text/xml", "SLD Style")
                
                # 3. WMS
                wms_url = (f"{GEOSERVER_PUBLIC_URL}/{workspace}/wms?service=WMS&version=1.1.1"
                           f"&request=GetMap&layers={workspace}:{layer_name}&styles={style_name or ''}"
                           f"&bbox={bbox}&width=768&height=330&srs=EPSG:4326&format=image/png")
                add_single_dist("wms", wms_url, "image/png", "WMS Visualization")

        # 4. Pubblicazione Dataset (collegando tutte le distribution ID)
        dataset_full_id = f"{city}:{dataset_unique_id}" # IDRA richiede spesso un prefix
        
        ds_body = {
            "id": dataset_full_id,
            "title": final_dataset_title,
            "description": final_dataset_desc,
            "datasetDescription": [final_dataset_desc],
            "datasetDistribution": dist_ids, # QUI colleghiamo tutte le 6 (o N) distribuzioni
            "spatial": resources_list[0].get('bbox', "-180,-90,180,90") if resources_list else "",
            "temporal": date_val, 
            "keyword": keywords,
            "author": author_name,
            "author_email": author_email,
            "theme": [theme] if theme else []
        }
        if publisher: ds_body['publisher_name'] = publisher

        return self._upsert_resource("dataset", ds_body, dataset_full_id)