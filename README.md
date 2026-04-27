# GeoServer & IDRA MinIO Auto-Publisher

Provided by: ENG

## Description
Automated service that bridges **MinIO** with **GeoServer** and the **IDRA Data Catalogue**.

The service continuously scans a MinIO bucket for trigger files (`_publish.json`). When a request is detected, it publishes the specified datasets to GeoServer (via REST API) and to the IDRA Catalogue, handles SLD styles, and creates the corresponding distribution links — all without any manual intervention.

---

## Supported file types

| Extension | GeoServer target | Notes |
|---|---|---|
| `.shp` | DataStore | Publisher zips the `.shp` together with its sidecar files (`.shx`, `.dbf`, `.prj`, `.cpg`, `.qpj`, `.sbn/.sbx`, `.fix`, `.shp.xml`) and uploads as `application/zip`. Layer name = filename stem. |
| `.geojson` | DataStore | Uploaded as `application/json` via the `file.geojson` endpoint. Layer name = filename stem. |
| `.tif` / `.tiff` | CoverageStore | Uploaded as `image/tiff`. Layer name = `store_name`. |
| `.gpkg` | DataStore | Uses `configure=all` — **every** feature table inside the GeoPackage becomes a layer. Layer names are derived from the feature table names and are queried back from GeoServer after upload. |
| anything else (e.g. `.pdf`) | — | Skipped on GeoServer; still published to IDRA as a raw downloadable resource when `write_on_catalogue=true`. |

---

## How to publish data

**Step 1:** Upload your data files (e.g. `.tif`, `.shp`, `.gpkg`, `.pdf`, `.sld`) to the MinIO bucket.
**Step 2:** Upload a file named exactly `_publish.json` in the same folder, containing the publishing instructions.

The service will detect the `_publish.json`, process the files, and rename the trigger to `_published.json` on success. Errors are written to `_failures.json`; malformed JSON is moved aside to `_corrupted.json` to break infinite retry loops.

### `_publish.json` structure

| Field | Required | Description |
|---|---|---|
| `data_path` | **yes** | Relative path to the file inside the MinIO bucket. |
| `workspace` | *yes (geo)* | GeoServer workspace (required for `.shp`, `.geojson`, `.tif`, `.gpkg`). |
| `store_name` | *yes (geo)* | Name of the GeoServer store to create or update. |
| `write_on_catalogue` | no | `true` to push this file to IDRA. |
| `description` | no | Custom description (recommended for non-geographic files such as PDFs). |
| `style_name` | no | Name of the SLD style in GeoServer. |
| `sld_path` | no | Relative path to the `.sld` file in MinIO. |
| `override_style` | no | `true` to overwrite an existing SLD in GeoServer. |

### Recommended folder layout

`City/Analysis_Topic/Timestamp-City-AnalysisType/filename.ext`

Example:
`Cluj-Napoca/Urban heat islands/20220500T000000-ClujNapoca-suhi-sentinel-s2/20220500T000000_ClujNapoca_suhi_sentinel_s2__diff_from_rural.tif`

---

### Examples

#### 1. GeoTIFF with SLD (GeoServer + IDRA)
```json
{
  "analysis": "Urban Heat Islands",
  "data": [{
    "workspace":   "ClujNapoca_Urban_heat_islands",
    "store_name":  "ClujNapoca-suhi-diff-from-rural",
    "data_path":   "Cluj-Napoca/Urban heat islands/.../diff_from_rural.tif",
    "style_name":  "heat_style",
    "sld_path":    "Cluj-Napoca/styles/heat_colors.sld",
    "write_on_catalogue": true,
    "description": "Urban Heat Island raster — difference from rural."
  }]
}
```

#### 2. Shapefile — GeoServer only
```json
{
  "analysis": "City Infrastructure",
  "data": [{
    "workspace":  "infrastructure",
    "store_name": "roads_network",
    "data_path":  "Milano/Infrastructure/20240101-Milano-Roads/roads_network.shp",
    "write_on_catalogue": false
  }]
}
```

#### 3. Multi-layer GeoPackage (auto-discovery of layers)
```json
{
  "analysis": "Green Inventory",
  "data": [{
    "workspace":  "green_areas",
    "store_name": "tallinn_green_2025",
    "data_path":  "Tallinn/3-30-300/2025-11-21/green_inventory.gpkg",
    "write_on_catalogue": true,
    "description": "Multi-layer green inventory for Tallinn."
  }]
}
```
All feature tables inside the GPKG are published automatically, each becoming a separate IDRA distribution.

#### 4. PDF — IDRA only (GeoServer is skipped)
```json
{
  "analysis": "Water Infiltration",
  "data": [{
    "data_path":  "Roma/Water Infiltration/20231015-Roma-Water/water_analysis_report.pdf",
    "write_on_catalogue": true,
    "description": "Final technical report on water infiltration."
  }]
}
```

#### 5. Mixed bundle (geo + PDF under the same dataset)
```json
{
  "analysis": "3-30-300 Rule",
  "data": [
    {
      "workspace":  "green_areas",
      "store_name": "tree_coverage",
      "data_path":  "Torino/3-30-300/20240220-Torino-Trees/tree_coverage.tif",
      "write_on_catalogue": true
    },
    {
      "data_path":  "Torino/3-30-300/20240220-Torino-Trees/methodology.pdf",
      "write_on_catalogue": true,
      "description": "Methodology document."
    }
  ]
}
```

---

## Deployment

Designed to run as a long-lived container (see `Dockerfile`). This service does **not** download files from MinIO — it assumes the bucket is mirrored to `TARGET_DIR` by a sibling process such as:

```
mc mirror --watch minio/geodata /data
```

### Environment variables

| Variable | Description | Default |
|---|---|---|
| `TARGET_DIR` | Local mount point where MinIO data is mirrored. | `/data` |
| `GEOSERVER_URL` | Internal URL of GeoServer for REST uploads. | `http://geoserver:8080/geoserver` |
| `GEOSERVER_PUBLIC_URL` | Public URL of GeoServer (used by IDRA for WMS links). | same as `GEOSERVER_URL` |
| `GEOSERVER_USER` / `GEOSERVER_PASSWORD` | Admin credentials. | `admin` / `geoserver` |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_SECURE` | MinIO connection. | `minio:9000`, `minioadmin`, `minioadmin`, `geodata`, `false` |
| `MINIO_PROXY_URL` | Public MinIO URL (used in IDRA download links). | `http://localhost:9090` |
| `IDRA_URL` | IDRA Catalogue base URL. Leave empty to disable IDRA. | *(empty)* |
| `PUBLISH_INTERVAL_SECONDS` | Scan interval. | `30` |
| `REQUEST_TIMEOUT` | HTTP timeout for every outbound call (seconds). | `120` |

---

## External references
- [GeoServer REST API](https://docs.geoserver.org/stable/en/user/rest/index.html)
- [MinIO Python SDK](https://min.io/docs/minio/linux/developers/python/API.html)
- [IDRA](https://github.com/Engineering-Research-and-Development/idra)
