"""
Base integration class for Gaon
"""
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, Optional
import logging

from gaon.config.models import SourceConfig

logger = logging.getLogger(__name__)

class BaseIntegrate(ABC):
    """Base class for data integration implementations.
    
    This class provides common functionality for all data integration implementations:
    1. Configuration management
    2. Time range validation
    3. Temporary file handling
    4. Abstract methods for client initialization and data extraction
    """
    
    def __init__(self, source_config: SourceConfig):
        """Initialize integration with configuration.
        
        Args:
            source_config: Source configuration containing connection and extraction parameters
        """
        self.source_config = source_config
        self._client: Optional[Any] = None
        self._initialize()
    
    @abstractmethod
    def _initialize(self) -> None:
        """Initialize client based on source configuration.
        
        This method should:
        1. Create appropriate client based on source_type
        2. Validate connection parameters
        3. Test connection if possible
        
        Raises:
            ValueError: If initialization fails
        """
        pass
    
    @abstractmethod
    def extract(self, start_time: datetime, end_time: datetime) -> Generator[Path, None, None]:
        """Extract data from source in batches.
        
        This method should:
        1. Use source_config.batch_size to determine batch size
        2. Extract data for the given time range
        3. Save each batch to a temporary file
        4. Yield the path to each temporary file
        5. Clean up temporary files after they're processed
        
        Args:
            start_time: Start time for data extraction
            end_time: End time for data extraction
            
        Yields:
            Path: Path to temporary file containing batch data
            
        Raises:
            ValueError: If extraction fails
        """
        pass
    
    def _validate_time_range(self, start_time: datetime, end_time: datetime) -> None:
        """Validate the extraction time range.
        
        Args:
            start_time: Start time for data extraction
            end_time: End time for data extraction
            
        Raises:
            ValueError: If time range is invalid
        """
        if start_time >= end_time:
            raise ValueError("Start time must be before end time")
        
        if start_time < self.source_config.start_time:
            raise ValueError(
                f"Start time {start_time} is before source start time {self.source_config.start_time}"
            )
            
        if end_time > self.source_config.end_time:
            raise ValueError(
                f"End time {end_time} is after source end time {self.source_config.end_time}"
            )
    
    def _create_temp_file(self, batch_number: int) -> Path:
        """Create a temporary file for batch data.
        
        Args:
            batch_number: Batch number for naming
            
        Returns:
            Path: Path to temporary file
        """
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        
        temp_file = temp_dir / f"{self.source_config.name}_batch_{batch_number}.csv"
        logger.info(f"Created temporary file at: {temp_file.absolute()}")
        return temp_file