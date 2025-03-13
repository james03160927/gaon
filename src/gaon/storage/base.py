from abc import ABC, abstractmethod
from typing import Optional
from pathlib import Path

from gaon.config.models import StorageConfig, SourceConfig


class BaseStorage(ABC):
    """Base class for storage implementations."""
    
    def __init__(self, storage_config: StorageConfig):
        """Initialize storage with configuration.
        
        Args:
            storage_config: Storage configuration containing bucket and credentials info
        """
        self.storage_config = storage_config
        self._initialize()
    
    @abstractmethod
    def _initialize(self) -> None:
        """Initialize storage client and validate configuration."""
        pass
    
    @abstractmethod
    def upload(self, source_config: SourceConfig, date_prefix: str, local_file: Path) -> str:
        """Upload a file to storage.
        
        Args:
            source_config: Source configuration containing source name and type
            date_prefix: Date prefix string in format yyyy-mm-dd_HH
            local_file: Local file path to upload
            
        Returns:
            str: Remote storage path where file was uploaded
        """
        pass
    
    def _build_remote_path(self, source_name: str, date_prefix: str, filename: str) -> str:
        """Build remote storage path following the format:
        <bucket>/<datasource_name>/<yyyy-mm-dd_HH>/<dataset_name>.csv
        
        Args:
            source_name: Name of the data source
            date_prefix: Date prefix string in format yyyy-mm-dd_HH
            filename: Name of the file to upload (already properly formatted)
            
        Returns:
            str: Complete remote storage path
        """
        return f"{source_name}/{date_prefix}/{filename}" 