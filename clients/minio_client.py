import json
from io import BytesIO

from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource

from config import (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
                    MINIO_BUCKET, MINIO_SECURE,
                    CONFIG_FILE_NAME, CORRUPTED_FILE_NAME)
from utils.logger import logger


class MinioWrapper:
    def __init__(self):
        try:
            self.client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=MINIO_SECURE,
            )
            logger.info(f"Connected to MinIO: {MINIO_ENDPOINT}")
        except Exception as e:
            logger.error(f"MinIO init error: {e}")
            raise

    def find_pending_requests(self):
        """Lists all _publish.json files in the bucket."""
        try:
            return [
                obj.object_name
                for obj in self.client.list_objects(MINIO_BUCKET, recursive=True)
                if obj.object_name.endswith(CONFIG_FILE_NAME)
            ]
        except S3Error as e:
            logger.error(f"MinIO list error: {e}")
            return []

    def read_config(self, object_name):
        """
        Reads a JSON config from MinIO.
        Returns the parsed object (dict or list), or None on generic errors.
        Re-raises json.JSONDecodeError so the caller can quarantine the file.
        """
        response = None
        try:
            response = self.client.get_object(MINIO_BUCKET, object_name)
            return json.loads(response.read().decode('utf-8'))
        except json.JSONDecodeError as e:
            logger.error(f"Malformed JSON in {object_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading config {object_name}: {e}")
            return None
        finally:
            # Always release the connection, even on JSONDecodeError.
            if response is not None:
                try:
                    response.close()
                    response.release_conn()
                except Exception:
                    pass

    def move_to_corrupted(self, object_name):
        """Moves a malformed file aside so it stops being picked up in the scan loop."""
        new_name = object_name.replace(CONFIG_FILE_NAME, CORRUPTED_FILE_NAME)
        try:
            source = CopySource(MINIO_BUCKET, object_name)
            self.client.copy_object(MINIO_BUCKET, new_name, source)
            self.client.remove_object(MINIO_BUCKET, object_name)
            logger.warning(f"Corrupted file moved: {object_name} -> {new_name}")
        except Exception as e:
            logger.error(f"Error moving corrupted file {object_name}: {e}")

    def save_json(self, object_name, data):
        """Stores a Python object as JSON in MinIO."""
        try:
            json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            self.client.put_object(
                MINIO_BUCKET, object_name,
                BytesIO(json_bytes), len(json_bytes),
                content_type='application/json',
            )
            return True
        except Exception as e:
            logger.error(f"Error saving JSON {object_name}: {e}")
            return False

    def delete_file(self, object_name):
        try:
            self.client.remove_object(MINIO_BUCKET, object_name)
            return True
        except Exception as e:
            logger.error(f"Error deleting {object_name}: {e}")
            return False
