"""
GCP storage implementation
"""
import logging
from pathlib import Path
from typing import Optional, Any

from google.cloud import storage
from google.cloud.storage import Bucket, Client
from google.oauth2 import service_account

from gaon.config.models import StorageConfig, SourceConfig
from gaon.storage.base import BaseStorage
from gaon.config.config import get_config

# Set up logging
logger = logging.getLogger(__name__)

class GCPStorage(BaseStorage):
    """Google Cloud Storage implementation."""
    
    def __init__(self, storage_config: StorageConfig):
        """Initialize GCP storage with configuration.
        
        Args:
            storage_config: Storage configuration containing bucket and credentials info
        """
        self._client: Optional[Client] = None
        self._bucket: Optional[Bucket] = None
        super().__init__(storage_config)

    def _validate_storage_access(self) -> None:
        """Validate read/write access to the bucket by performing test operations.
        
        Raises:
            ValueError: If the required permissions are not available
        """
        test_blob_name = "test_access_validation"
        test_content = b"Testing storage access"
        
        try:
            # Test write access by creating a blob
            logger.debug("Testing write access...")
            blob = self._bucket.blob(test_blob_name)
            blob.upload_from_string(test_content)
            logger.debug("Write access confirmed")
            
            # Test read access by downloading the blob
            logger.debug("Testing read access...")
            downloaded_content = blob.download_as_bytes()
            if downloaded_content != test_content:
                raise ValueError("Data integrity check failed")
            logger.debug("Read access confirmed")
            
            # Clean up the test blob
            logger.debug("Cleaning up test blob...")
            blob.delete()
            logger.debug("Test blob cleaned up successfully")
            
            logger.info("Successfully validated read/write access to GCP storage")
            
        except Exception as e:
            raise ValueError(f"Storage access validation failed: {str(e)}")

    def _initialize(self) -> None:
        """Initialize GCP storage client and bucket.
        
        Raises:
            ValueError: If credentials path is invalid or bucket is not accessible
        """
        if not self.storage_config.credentials_path:
            raise ValueError("GCP credentials path must be provided")
        
        credentials_path = Path(self.storage_config.credentials_path)
        if not credentials_path.exists():
            raise ValueError(f"GCP credentials file not found at: {credentials_path}")
            
        try:
            # Initialize GCP client with credentials
            logger.debug("Initializing GCP client...")
            self._client = storage.Client.from_service_account_json(
                str(credentials_path)
            )
            logger.debug("Successfully initialized GCP client")
            
            # Get and validate bucket access
            logger.debug(f"Connecting to bucket: {self.storage_config.bucket_name}")
            self._bucket = self._client.bucket(self.storage_config.bucket_name)
            if not self._bucket.exists():
                raise ValueError(
                    f"Bucket '{self.storage_config.bucket_name}' does not exist or is not accessible"
                )
            logger.debug(f"Successfully connected to bucket: {self.storage_config.bucket_name}")
                
        except Exception as e:
            raise ValueError(f"Failed to initialize GCP storage: {str(e)}")

    def upload(self, source_config: Any, date_prefix: str, file_path: Path, remote_name: Optional[str] = None) -> str:
        """Upload a file to GCP storage.
        
        Args:
            source_config: Source configuration
            date_prefix: Date prefix for the remote path
            file_path: Path to the file to upload
            remote_name: Optional name for the remote file (if different from local)
            
        Returns:
            str: Remote path where the file was uploaded
            
        Raises:
            ValueError: If upload fails
        """
        try:
            # Get client name from config
            config = get_config()
            client_name = config.client
            
            # Construct remote path: bucket/client/source/date/file
            remote_path = f"{client_name}/{source_config.name}/{date_prefix}"
            if remote_name:
                remote_path = f"{remote_path}/{remote_name}"
            else:
                remote_path = f"{remote_path}/{file_path.name}"
            
            # Upload file
            blob = self._bucket.blob(remote_path)
            blob.upload_from_filename(str(file_path))
            
            logger.debug(f"Successfully uploaded file to: {remote_path}")
            return remote_path
            
        except Exception as e:
            raise ValueError(f"Failed to upload file to GCP: {str(e)}")
