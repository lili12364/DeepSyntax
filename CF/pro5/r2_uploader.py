import boto3
from botocore.config import Config
import logging
import requests
from io import BytesIO
from typing import Optional
import os
from urllib.parse import urlparse
from CF.cf_config import (
    CF_R2_ACCESS_KEY_ID, CF_R2_SECRET_ACCESS_KEY, CF_R2_BUCKET_NAME,
    CF_R2_ENDPOINT_URL, R2_IMAGES_PATH, TEMP_DIR
)
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class R2Uploader:
    """Upload images and files to Cloudflare R2"""

    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=CF_R2_ENDPOINT_URL,
            aws_access_key_id=CF_R2_ACCESS_KEY_ID,
            aws_secret_access_key=CF_R2_SECRET_ACCESS_KEY,
            region_name='us-east-1',
            config=Config(
                signature_version='s3v4',
                s3={'addressing_style': 'path'}
            )
        )
        self.bucket_name = CF_R2_BUCKET_NAME

        Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and has a proper scheme"""
        if not url:
            return False
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def upload_image_from_url(self, image_url: str, filename: str, r2_path: str = R2_IMAGES_PATH) -> Optional[str]:
        """Download image from URL and upload to R2"""
        if not image_url:
            logger.warning(f"Empty image URL for {filename}")
            return None

        if not self._is_valid_url(image_url):
            logger.error(f"Invalid image URL for {filename}: {image_url}")
            return None

        try:
            logger.info(f"Downloading image from {image_url}")
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()

            r2_key = f"{r2_path}/{filename}"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=r2_key,
                Body=response.content,
                ContentType='image/jpeg'
            )

            logger.info(f"Uploaded image to r2://{self.bucket_name}/{r2_key}")
            return r2_key

        except Exception as e:
            logger.error(f"Error uploading image {filename}: {str(e)}")
            return None

    def upload_local_file(self, local_path: str, r2_path: str, filename: str = None) -> Optional[str]:
        """Upload a local file to R2"""
        try:
            if not os.path.exists(local_path):
                logger.warning(f"Local file not found: {local_path}")
                return None

            if not filename:
                filename = os.path.basename(local_path)

            r2_key = f"{r2_path}/{filename}"

            logger.info(f"Uploading local file {local_path} to R2")
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                r2_key
            )

            logger.info(f"Uploaded file to r2://{self.bucket_name}/{r2_key}")
            return r2_key

        except Exception as e:
            logger.error(f"Error uploading file {local_path}: {str(e)}")
            return None

    def list_objects(self, r2_path: str) -> list:
        """List objects in R2 path"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=r2_path
            )
            return response.get('Contents', [])
        except Exception as e:
            logger.error(f"Error listing objects: {str(e)}")
            return []

    def get_r2_url(self, r2_key: str) -> str:
        """Generate R2 URL for a key"""
        return f"r2://{self.bucket_name}/{r2_key}"

    def generate_presigned_url(self, r2_key: str, expiration: int = 3600) -> Optional[str]:
        """Generate a presigned URL for an R2 object"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': r2_key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return None

    def test_connection(self) -> bool:
        """Test R2 connection"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info("R2 connection successful")
            return True
        except Exception as e:
            logger.error(f"R2 connection failed: {str(e)}")
            return False
