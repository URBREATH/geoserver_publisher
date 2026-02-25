# GeoServer & IDRA MinIO Auto-Publisher

Provided by: ENG

## Description
This tool is an automated service designed to bridge a **MinIO** object storage system with **GeoServer** and the **IDRA Data Catalogue**. 

It continuously monitors a MinIO bucket for trigger files (`_publish.json`). When a request is detected, the service automatically publishes the specified datasets to GeoServer (via REST API) and to the IDRA Catalogue, applying styles and creating download links without any manual intervention.

---

## How to Publish Data (Usage Guide)

Using the publisher is incredibly simple. You do not need to interact with GeoServer or IDRA directly. You only need to upload your data to MinIO and provide a simple JSON instruction file.

**Step 1:** Upload your data files (e.g., `.tif`, `.shp`, `.pdf`, `.sld`) to your MinIO bucket.
**Step 2:** In the same MinIO folder, upload a file named exactly `_publish.json` containing your publishing instructions.

The service will detect the `_publish.json`, process the files, and automatically rename the file to `_published.json` once done.

### The `_publish.json` Structure

The JSON must contain an `analysis` name (used to group datasets in IDRA) and a `data` array listing the files you want to publish.

| Field | Required | Description |
|---|---|---|
| `data_path` | **Yes** | Relative path to the file inside the MinIO bucket. |
| `workspace` | *Yes (for Geo)* | Target GeoServer workspace (required for .shp, .tif). |
| `store_name` | *Yes (for Geo)* | Desired name for the GeoServer DataStore/CoverageStore. |
| `write_on_catalogue`| No | Set to `true` to publish this file to the IDRA Catalogue. |
| `description` | No | Custom description for the file (highly recommended for PDFs). |
| `style_name` | No | Name of the style in GeoServer. |
| `sld_path` | No | Relative path to the `.sld` style file in MinIO. |
| `override_style` | No | Set to `true` to overwrite an existing style in GeoServer. |

---

### Real Use-Case Examples

#### 1. Publish a GeoTIFF with SLD Style (GeoServer + IDRA)
This is the standard use case for geographic maps. The tool will upload the TIF to GeoServer, upload and apply the SLD style, and create a dataset in IDRA with WMS and download links.

```json
{
  "analysis": "Urban Heat Islands",
  "data": [
    {
      "workspace": "climate_workspace",
      "store_name": "heat_map_2023",
      "data_path": "climate/heat_map_2023.tif",
      "style_name": "heat_style",
      "sld_path": "styles/heat_colors.sld",
      "write_on_catalogue": true,
      "description": "Urban Heat Island Raster Map"
    }
  ]
}
```

#### 2. Publish a Shapefile to GeoServer ONLY (No IDRA)
If you only need the layer in GeoServer for internal use and don't want it on the public open data catalogue.

```json
{
  "analysis": "City Infrastructure",
  "data": [
    {
      "workspace": "infrastructure",
      "store_name": "roads_network",
      "data_path": "vector/roads_network.shp",
      "write_on_catalogue": false
    }
  ]
}
```

#### 3. Publish a Non-Geographical File (e.g., PDF) to IDRA ONLY
*Note: GeoServer does not support PDFs. The tool is smart enough to skip GeoServer entirely and push the PDF directly to IDRA as a downloadable resource.*

```json
{
  "analysis": "Water Infiltration",
  "data": [
    {
      "data_path": "reports/water_analysis_report.pdf",
      "write_on_catalogue": true,
      "description": "Final Technical Report for Water Infiltration"
    }
  ]
}
```

#### 4. Publish a Mixed Bundle (Geodata + PDF)
Group multiple files into a single "Bundle" under the same IDRA Dataset.

```json
{
  "analysis": "3-30-300 Rule",
  "data": [
    {
      "workspace": "green_areas",
      "store_name": "tree_coverage",
      "data_path": "green/tree_coverage.tif",
      "write_on_catalogue": true
    },
    {
      "data_path": "green/methodology.pdf",
      "write_on_catalogue": true,
      "description": "Methodology Document"
    }
  ]
}
```

---

## Deployment & Installation

The service is designed to run as a persistent background process, ideally within a Docker container.

### Prerequisites
1. **GeoServer & MinIO**: Both must be running and accessible over the network.
2. **Local Data Directory (`TARGET_DIR`)**: A local folder inside the container where MinIO files are synchronized.
3. **Data Synchronization Tool**: This service does *not* download files from MinIO. You must run a separate background process (like `mc mirror --watch minio/geodata /data`) to continuously sync the MinIO bucket to the container's `TARGET_DIR`.

### Environment Variables

Configure the tool using the following environment variables (e.g., in your `docker-compose.yml`):

| Variable | Description | Default / Example |
|---|---|---|
| `TARGET_DIR` | Local mount point where MinIO data is synced. | `/data` |
| `GEOSERVER_URL` | Internal URL of GeoServer for API uploads. | `http://geoserver:8080/geoserver` |
| `GEOSERVER_USER` | Admin username for GeoServer. | `admin` |
| `GEOSERVER_PASSWORD`| Admin password for GeoServer. | `geoserver` |
| `MINIO_ENDPOINT` | MinIO server address and port. | `minio:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key. | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key. | `minioadmin` |
| `MINIO_BUCKET` | The MinIO bucket to monitor. | `geodata` |
| `MINIO_PROXY_URL` | Public URL for MinIO file downloads (used by IDRA). | `http://localhost:9090` |
| `IDRA_URL` | Base URL of the IDRA Catalogue API. | `http://idra:8080/idra` |
| `PUBLISH_INTERVAL_SECONDS`| Scan interval for new trigger files. | `30` |

---

## Technical Details & Built Image

- **Registry URL**: `https://registry.urbreath.tech`
- **Image Name**: `geoserver-publisher`
- **Version**: `2.2.2`

### External Resources
- **GeoServer REST API**: [https://docs.geoserver.org/stable/en/user/rest/index.html](https://docs.geoserver.org/stable/en/user/rest/index.html)
- **MinIO Python Client**: [https://min.io/docs/minio/linux/developers/python/API.html](https://min.io/docs/minio/linux/developers/python/API.html)
- **IDRA Repository**: [https://github.com/Engineering-Research-and-Development/idra](https://github.com/Engineering-Research-and-Development/idra)