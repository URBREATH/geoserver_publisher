import json
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from config import (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, 
                    MINIO_BUCKET, MINIO_SECURE, CONFIG_FILE_NAME, PROCESSED_FILE_NAME)
from utils.logger import logger

class MinioWrapper:
    def __init__(self):
        try:
            self.client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE
            )
            logger.info(f"Connected to MinIO: {MINIO_ENDPOINT}")
        except Exception as e:
            logger.error(f"MinIO init error: {e}")
            raise e

    def find_pending_requests(self):
        configs = []
        try:
            objects = self.client.list_objects(MINIO_BUCKET, recursive=True)
            for obj in objects:
                if obj.object_name.endswith(CONFIG_FILE_NAME):
                    configs.append(obj.object_name)
        except S3Error as e:
            logger.error(f"MinIO list error: {e}")
        return configs

    def read_config(self, object_name):
        """Reads the JSON file. Returns the raw data (list or dict)."""
        try:
            response = self.client.get_object(MINIO_BUCKET, object_name)
            data = response.read()
            config = json.loads(data.decode('utf-8'))
            response.close()
            response.release_conn()
            
            # --- FIX: Removed the logic that truncated the list to the first element ---
            # Now it returns exactly what it finds in the JSON (list or dict)
            return config
            
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in {object_name}: {e}")
            raise e # We re-raise the error to handle it in the main (by moving the file)
        except Exception as e:
            logger.error(f"Error reading config {object_name}: {e}")
            return None

    def mark_as_processed(self, object_name):
        try:
            new_name = object_name.replace(CONFIG_FILE_NAME, PROCESSED_FILE_NAME)
            source = CopySource(MINIO_BUCKET, object_name)
            self.client.copy_object(MINIO_BUCKET, new_name, source)
            self.client.remove_object(MINIO_BUCKET, object_name)
            logger.info(f"Renamed: {object_name} -> {new_name}")
            return True
        except Exception as e:
            logger.error(f"Error renaming {object_name}: {e}")
            return False

    def move_to_corrupted(self, object_name):
        """Moves a malformed file to avoid infinite loops."""
        try:
            new_name = object_name.replace(CONFIG_FILE_NAME, "_corrupted.json")
            source = CopySource(MINIO_BUCKET, object_name)
            self.client.copy_object(MINIO_BUCKET, new_name, source)
            self.client.remove_object(MINIO_BUCKET, object_name)
            logger.warning(f"Corrupted file moved: {object_name} -> {new_name}")
        except Exception as e:
            logger.error(f"Error moving corrupted file {object_name}: {e}")

    def save_json(self, object_name, data):
        """Saves a Python object as JSON on MinIO."""
        try:
            json_bytes = json.dumps(data, indent=2).encode('utf-8')
            from io import BytesIO
            self.client.put_object(
                MINIO_BUCKET, object_name, BytesIO(json_bytes), len(json_bytes),
                content_type='application/json'
            )
            return True
        except Exception as e:
            logger.error(f"Error saving JSON {object_name}: {e}")
            return False

    def delete_file(self, object_name):
        try:
            self.client.remove_object(MINIO_BUCKET, object_name)
        except Exception as e:
            logger.error(f"Error deleting {object_name}: {e}")