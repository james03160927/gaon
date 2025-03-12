import logging
from pathlib import Path
from typing import Optional

from google.cloud import storage
from google.cloud.storage import Bucket, Client
from google.oauth2 import service_account

from gaon.config import StorageConfig, SourceConfig
from gaon.storage.base import BaseStorage

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
            credentials = service_account.Credentials.from_service_account_file(
                str(credentials_path)
            )
            self._client = storage.Client(credentials=credentials)
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

    def upload(self, source_config: SourceConfig, date_prefix: str, local_file: Path) -> str:
        """Upload a file to GCP storage.
        
        Args:
            source_config: Source configuration containing source name and type
            date_prefix: Date prefix string in format yyyy-mm-dd_HH
            local_file: Local file path to upload
            
        Returns:
            str: Remote storage path where file was uploaded
            
        Raises:
            ValueError: If file upload fails
        """
        if not local_file.exists():
            raise ValueError(f"Local file not found: {local_file}")
            
        remote_path = self._build_remote_path(
            source_config.name,
            date_prefix,
            local_file.name
        )
        
        try:
            blob = self._bucket.blob(remote_path)
            blob.upload_from_filename(str(local_file))
            logger.debug(f"Successfully uploaded file to: {remote_path}")
            return remote_path
        except Exception as e:
            raise ValueError(f"Failed to upload file to GCP: {str(e)}")
