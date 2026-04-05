# === MODULE PURPOSE ===
# Minimal S3-compatible client for uploading/downloading ML model files.
# Works with AWS S3, MinIO, Alibaba Cloud OSS (S3-compatible mode).
# Uses boto3 (sync SDK) wrapped in asyncio.to_thread() for async usage.

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class S3Client:
    """S3-compatible storage client for model file management."""

    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket

    def _get_client(self):
        """Create a boto3 S3 client (not thread-safe, create per-call)."""
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
        )

    async def upload_file(self, local_path: Path, s3_key: str) -> str:
        """Upload a local file to S3.

        Args:
            local_path: Path to the local file.
            s3_key: S3 object key (e.g. "models/full_20260406.lgb").

        Returns:
            S3 URI string (s3://bucket/key).

        Raises:
            FileNotFoundError: If local_path doesn't exist.
            Exception: On S3 upload failure.
        """
        if not local_path.exists():
            raise FileNotFoundError(f"File not found: {local_path}")

        def _upload():
            client = self._get_client()
            client.upload_file(str(local_path), self._bucket, s3_key)

        await asyncio.to_thread(_upload)
        uri = f"s3://{self._bucket}/{s3_key}"
        logger.info("Uploaded %s → %s", local_path.name, uri)
        return uri

    async def download_file(self, s3_key: str, local_path: Path) -> Path:
        """Download a file from S3.

        Args:
            s3_key: S3 object key.
            local_path: Local destination path.

        Returns:
            Path to the downloaded file.
        """
        local_path.parent.mkdir(parents=True, exist_ok=True)

        def _download():
            client = self._get_client()
            client.download_file(self._bucket, s3_key, str(local_path))

        await asyncio.to_thread(_download)
        logger.info("Downloaded s3://%s/%s → %s", self._bucket, s3_key, local_path)
        return local_path

    async def list_models(self, prefix: str = "models/") -> list[dict]:
        """List model files in S3 bucket.

        Args:
            prefix: S3 key prefix to filter by.

        Returns:
            List of dicts with keys: key, size, last_modified.
        """

        def _list():
            client = self._get_client()
            resp = client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
            contents = resp.get("Contents", [])
            return [
                {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                }
                for obj in contents
            ]

        return await asyncio.to_thread(_list)


def create_s3_client_from_config() -> S3Client | None:
    """Create S3Client from config. Returns None if not configured."""
    from src.common.config import get_s3_config

    config = get_s3_config()
    if not config:
        return None

    endpoint_url = config.get("endpoint_url", "")
    access_key = config.get("access_key", "")
    secret_key = config.get("secret_key", "")
    bucket = config.get("bucket", "")

    if not all([endpoint_url, access_key, secret_key, bucket]):
        logger.warning("S3 config incomplete, skipping S3 integration")
        return None

    return S3Client(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
    )
