# GeoServer MinIO Auto-Publisher

Provided by: ENG

## Description

This tool is an automated publishing service designed to bridge a **MinIO** object storage system with a **GeoServer** instance. Its primary function is to continuously monitor a MinIO bucket for specific trigger files (`_publish.json`) and automatically publish the corresponding geospatial datasets as new layers in GeoServer.

The service operates by detecting these JSON request files, parsing their contents to determine the workspace, store name, and data path, and then using the GeoServer REST API to create the necessary DataStore (for vector data like Shapefiles) or CoverageStore (for raster data like GeoTIFFs). To ensure idempotency, once a dataset is successfully published, the trigger file is renamed to `_published.json`, preventing duplicate publications.

This entire workflow is configured through environment variables, making the tool highly portable and ideal for deployment in containerized environments like Docker. It effectively creates a "hot folder" in the cloud, streamlining the process of updating and adding new geospatial layers to GeoServer without manual intervention.

## Installation Prerequisites

Before deploying the service, ensure the following requirements are met:

- **Running GeoServer Instance**: A GeoServer instance must be running and accessible over the network from where this service will be deployed.
    
- **Running MinIO Instance**: A MinIO server (or other S3-compatible object storage) must be available and accessible.
    
- **Shared Data Volume**: This is a critical requirement. The service needs a local directory (`TARGET_DIR`) to which data from MinIO is synced. **This same physical storage location must be mounted and accessible by the GeoServer container** at its data root path (e.g., `/opt/geoserver_data`). This allows the publisher to reference file paths that GeoServer can resolve.
    
- **Data Synchronization Mechanism**: The service itself does **not** pull data from MinIO. You must have a separate process to synchronize files from your MinIO bucket to the local `TARGET_DIR`. A common tool for this is `mc mirror`. The publisher script simply waits for the files specified in `_publish.json` to appear in the local directory.
    

## Installation Instructions

The service is designed to run as a persistent process, ideally within a Docker container.

### 1. Configure Environment Variables

The service is configured entirely through environment variables. These must be set in the environment where the script is run (e.g., in a `docker-compose.yml` file or via `docker run -e`).

|Variable|Description|Example|
|---|---|---|
|`TARGET_DIR`|The local mount point where data from MinIO is synchronized.|`/data`|
|`GEOSERVER_URL`|The base URL of the GeoServer instance.|`http://geoserver:8080/geoserver`|
|`GEOSERVER_USER`|Admin username for the GeoServer REST API.|`admin`|
|`GEOSERVER_PASSWORD`|Password for the GeoServer admin user.|`geoserver`|
|`MINIO_ENDPOINT`|The MinIO server endpoint, including the port.|`minio:9000`|
|`MINIO_ACCESS_KEY`|The access key for MinIO.|`minioadmin`|
|`MINIO_SECRET_KEY`|The secret key for MinIO.|`minioadmin`|
|`MINIO_BUCKET`|The name of the MinIO bucket to monitor for new requests.|`geodata`|
|`MINIO_SECURE`|Set to `true` to use HTTPS for MinIO, otherwise `false`.|`false`|

### 2. Set Up Data Synchronization

Configure an external tool like `mc mirror` to continuously sync your MinIO bucket to the `TARGET_DIR` on the host machine, which is mounted into the service's container.

Example `mc mirror` command:

```
mc mirror --watch minio/geodata /path/on/host/data
```

### 3. Define a Publishing Request

To publish a new layer, upload your data file(s) to MinIO. In the same directory, upload a JSON file named `_publish.json` with the following structure:

**Example `_publish.json` for a Shapefile:**

```
{
  "workspace": "geology",
  "store_name": "fault_lines",
  "data_path": "vector/faults/fault_lines.shp"
}
```

**Example `_publish.json` for a GeoTIFF:**

```
{
  "workspace": "dem",
  "store_name": "alps_elevation",
  "data_path": "raster/alps_elevation.tif"
}
```

- `workspace`: The target GeoServer workspace. It must exist.
    
- `store_name`: The desired name for the new DataStore or CoverageStore.
    
- `data_path`: The relative path to the main data file within the bucket.
    

### 4. Run the Service

Deploy and run the service (e.g., as a Docker container), ensuring it has access to the defined environment variables and the shared volume. The service will start, connect to MinIO, and begin scanning for `_publish.json` files to process.

## Built Image Registry

Provide details of the registry where the built image of this tool is stored.

- **Registry URL**: `https://registry.urbreath.tech`
    
- **Image Name**: `geoserver-publisher`
    
- **Version**: `0.0.6`
    

## External technical resources

List any external resources, such as documentation, libraries, or other resources.

- **GeoServer REST API Documentation**: [https://docs.geoserver.org/stable/en/user/rest/index.html](https://docs.geoserver.org/stable/en/user/rest/index.html "null")
    
- **MinIO Python Client API Reference**: [https://min.io/docs/minio/linux/developers/python/API.html](https://min.io/docs/minio/linux/developers/python/API.html "null")
    
- **Requests Library**: [https://requests.readthedocs.io/en/latest/](https://requests.readthedocs.io/en/latest/ "null")
    