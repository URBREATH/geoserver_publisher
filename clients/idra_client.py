import os
import re
import json
import mimetypes
from urllib.parse import quote
from datetime import datetime

import requests

from config import (IDRA_URL, GEOSERVER_PUBLIC_URL, REQUEST_TIMEOUT,
                    DISTRIBUTION_TEMPLATE_PATH, DATASET_TEMPLATE_PATH,
                    MINIO_PROXY_URL, MINIO_BUCKET)
from utils.logger import logger


# Minimum token-similarity score to consider a distribution template a match.
FUZZY_MATCH_THRESHOLD = 0.40

# Regex used to tokenize filenames for fuzzy matching.
_TOKEN_SPLIT = re.compile(r'[_\-\.]+')

# Matches both "YYYY-MM-DD" and "YYYYMMDD", with reasonable month/day bounds.
_DATE_DASHED  = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
_DATE_COMPACT = re.compile(r'((?:19|20)\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])')


def _encode_minio_url(path):
    """Safely encode a path fragment for use inside a MinIO browser URL."""
    return quote(path, safe="/")


def parse_date_from_key(config_key):
    """Extract a YYYY-MM-DD date from an object key, else return today's date."""
    m = _DATE_DASHED.search(config_key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = _DATE_COMPACT.search(config_key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return datetime.now().strftime("%Y-%m-%d")


def to_dmy(iso_date):
    """'YYYY-MM-DD' -> 'DD-MM-YYYY'. Returns the input unchanged on malformed input."""
    parts = iso_date.split('-')
    if len(parts) == 3 and all(parts):
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return iso_date


class IdraClient:
    def __init__(self):
        self.enabled          = bool(IDRA_URL)
        self.dist_templates   = self._load_json(DISTRIBUTION_TEMPLATE_PATH)
        self.dataset_templates = self._load_json(DATASET_TEMPLATE_PATH)
        mimetypes.init()

        if not self.enabled:
            logger.info("IDRA is disabled (IDRA_URL not set).")

    @staticmethod
    def _load_json(path):
        if not os.path.exists(path):
            logger.warning(f"Template file not found: {path}")
            return []
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error loading template {path}: {e}")
            return []

    # -----------------------------------------------------------------
    # Template matching
    # -----------------------------------------------------------------
    def _match_distribution(self, filename):
        """Token-based fuzzy match between a filename and the distribution templates."""
        if not filename:
            return None

        filename_tokens = {t for t in _TOKEN_SPLIT.split(filename.lower()) if t}
        best_tmpl, best_score = None, 0.0

        for tmpl in self.dist_templates:
            pattern = tmpl.get('file_pattern', '')
            if not pattern:
                continue
            pattern_clean  = re.sub(r'\{.*?\}', '', pattern).lower()
            pattern_tokens = {t for t in _TOKEN_SPLIT.split(pattern_clean) if t}
            if not pattern_tokens:
                continue
            score = len(filename_tokens & pattern_tokens) / len(pattern_tokens)
            if score > FUZZY_MATCH_THRESHOLD and score > best_score:
                best_tmpl, best_score = tmpl, score

        return best_tmpl

    def _find_dataset_template(self, analysis_name):
        """Match a dataset template by its KPI field (case-insensitive)."""
        if not analysis_name:
            return None
        target = analysis_name.lower().strip()
        for tmpl in self.dataset_templates:
            # Guard: KPI could be missing or null in a user-edited template.
            kpi = tmpl.get('KPI')
            if isinstance(kpi, str) and kpi.lower().strip() == target:
                return tmpl
        return None

    # -----------------------------------------------------------------
    # HTTP
    # -----------------------------------------------------------------
    def _upsert_resource(self, endpoint, payload, resource_id):
        """Create a resource if it doesn't exist; treat 409 as success (idempotent)."""
        base_api = f"{IDRA_URL}/api/{endpoint}"
        try:
            # Check existence first to avoid noisy 409s.
            existing_id = quote(resource_id, safe="")
            check = requests.get(f"{base_api}/{existing_id}", timeout=REQUEST_TIMEOUT)
            if check.status_code == 200:
                logger.info(f"IDRA resource '{resource_id}' already exists. Skipping.")
                return True

            logger.info(f"Creating IDRA resource: {resource_id}")
            resp = requests.post(base_api, json=payload, timeout=REQUEST_TIMEOUT)

            if resp.status_code in (200, 201, 409):
                return True

            logger.error(f"IDRA create failed for {resource_id}: "
                         f"{resp.status_code} - {resp.text}")
            return False
        except requests.RequestException as e:
            logger.error(f"IDRA request error for {resource_id}: {e}")
            return False

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------
    def publish_bundle(self, analysis_topic, city, date_val, resources_list):
        """
        Publishes ONE dataset for the given analysis, plus one distribution per
        file/layer. Returns True on success.
        """
        if not self.enabled or not resources_list:
            return True

        # --- Context for template formatting ---
        context = {
            "city":     city,
            "date":     date_val,
            "date_dmy": to_dmy(date_val),
            "KPI":      analysis_topic,
        }

        ds_tmpl = self._find_dataset_template(analysis_topic)

        # Unique, timestamped ID for the dataset (deterministic within one bundle).
        timestamp_suffix   = datetime.now().strftime("%Y%m%d-%H%M%S")
        dataset_unique_id  = f"{city}_{analysis_topic.replace(' ', '_')}_{timestamp_suffix}"

        # --- Dataset-level metadata (template takes precedence if available) ---
        title = f"{city} {analysis_topic} {context['date_dmy']}"
        desc  = f"Dataset for {analysis_topic} in {city}"
        keywords, author_name, author_email, theme, publisher = [], None, None, None, None

        if ds_tmpl:
            if 'dataset_title' in ds_tmpl:
                title = ds_tmpl['dataset_title'].format(**context)
            if 'description' in ds_tmpl:
                desc = ds_tmpl['description'].format(**context)
            raw_kw = ds_tmpl.get('keywords')
            if isinstance(raw_kw, list):
                keywords = [k.format(**context) for k in raw_kw]
            elif isinstance(raw_kw, str):
                keywords = [raw_kw.format(**context)]
            author_name  = ds_tmpl.get('author_name')
            author_email = ds_tmpl.get('author_email')
            theme        = ds_tmpl.get('theme')
            publisher    = ds_tmpl.get('publisher')

        # --- Create one distribution per resource (raw + optional SLD + optional WMS) ---
        dist_ids = []
        for res in resources_list:
            dist_ids.extend(self._publish_resource_distributions(
                res, dataset_unique_id, context, desc,
            ))

        # --- Create (or update) the dataset record ---
        dataset_full_id = f"{city}:{dataset_unique_id}"
        ds_body = {
            "id":                   dataset_full_id,
            "title":                title,
            "description":          desc,
            "datasetDescription":   [desc],
            "datasetDistribution":  dist_ids,
            "spatial":              resources_list[0].get('bbox', "-180,-90,180,90"),
            "temporal":             date_val,
            "keyword":              keywords,
            "author":               author_name,
            "author_email":         author_email,
            "theme":                [theme] if theme else [],
        }
        if publisher:
            ds_body['publisher_name'] = publisher

        return self._upsert_resource("dataset", ds_body, dataset_full_id)

    # -----------------------------------------------------------------
    # Per-resource distribution generation
    # -----------------------------------------------------------------
    def _publish_resource_distributions(self, res, dataset_unique_id,
                                        context, fallback_desc):
        """Creates 1..3 distributions for a resource and returns their IDs."""
        data_path   = res['data_path']
        layer_name  = res['layer_name']
        workspace   = res['workspace']
        sld_path    = res.get('sld_path')
        style_name  = res.get('style_name')
        bbox        = res.get('bbox', "-180,-90,180,90")
        is_geo      = res.get('is_geo', True)
        custom_desc = res.get('custom_desc')

        filename = os.path.basename(data_path)
        dist_tmpl = self._match_distribution(filename)

        created_ids = []

        def _add(suffix, url, fmt, title_suffix=""):
            dist_id = f"{dataset_unique_id}_{layer_name}_{suffix}"

            # Title: template > custom_desc > filename
            title = custom_desc or filename
            if dist_tmpl and 'dataset_title' in dist_tmpl:
                title = dist_tmpl['dataset_title']
            if title_suffix:
                title = f"{title} ({title_suffix})"

            # Description: template > custom_desc > dataset description
            d_desc = custom_desc or fallback_desc
            if dist_tmpl and 'description' in dist_tmpl:
                d_desc = dist_tmpl['description'].format(**context)

            body = {
                "id":          dist_id,
                "title":       title,
                "description": d_desc,
                "downloadURL": url,
                "accessURL":   url,
                "format":      fmt,
            }
            if dist_tmpl and 'license' in dist_tmpl:
                body["license"] = dist_tmpl['license']

            if self._upsert_resource("distributiondcatap", body, dist_id):
                created_ids.append(dist_id)

        raw_data_url = f"{MINIO_PROXY_URL}/browser/{MINIO_BUCKET}/{_encode_minio_url(data_path)}"

        if not is_geo:
            # Non-geo resource: single download distribution, MIME from template or guess.
            mime = (dist_tmpl.get('format') if dist_tmpl else None) \
                or mimetypes.guess_type(data_path)[0] \
                or "application/octet-stream"
            _add("download", raw_data_url, mime)
            return created_ids

        # Geographic resource -> up to 3 distributions.
        fmt_raw = (dist_tmpl.get('format') if dist_tmpl else None) or "application/octet-stream"
        _add("raw_data", raw_data_url, fmt_raw, "Raw Data")

        if sld_path:
            sld_url = f"{MINIO_PROXY_URL}/browser/{MINIO_BUCKET}/{_encode_minio_url(sld_path)}"
            _add("style", sld_url, "application/vnd.ogc.sld+xml", "SLD Style")

        wms_url = (
            f"{GEOSERVER_PUBLIC_URL}/{quote(workspace, safe='')}/wms"
            f"?service=WMS&version=1.1.1&request=GetMap"
            f"&layers={quote(workspace, safe='')}:{quote(layer_name, safe='')}"
            f"&styles={quote(style_name or '', safe='')}"
            f"&bbox={bbox}&width=768&height=330&srs=EPSG:4326&format=image/png"
        )
        _add("wms", wms_url, "image/png", "WMS Visualization")

        return created_ids
