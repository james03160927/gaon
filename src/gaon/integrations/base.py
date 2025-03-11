from abc import ABC, abstractmethod
from typing import List, Generator, Dict, Any
from datetime import datetime
import pandas as pd

class DataSourceIntegration(ABC):
    """Base class for all data source integrations"""
    
    @abstractmethod
    def get_available_tables(self) -> List[str]:
        """Return list of available tables for this data source"""
        pass
    
    @abstractmethod
    def setup(self, selected_tables: List[str]) -> None:
        """Setup the integration with selected tables"""
        pass
    
    @abstractmethod
    def get_required_credentials(self) -> dict:
        """Get credentials required for this integration"""
        pass

    @abstractmethod
    def retrieve_data(
        self,
        table_name: str,
        batch_size: int = 1000,
        last_processed_id: Any = None
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Retrieve data from the source in batches
        
        Args:
            table_name: Name of the table to retrieve data from
            batch_size: Number of records to retrieve in each batch
            last_processed_id: ID of the last processed record for pagination
        
        Returns:
            Generator yielding DataFrames containing the data
        """
        pass

    def get_primary_key(self, table_name: str) -> str:
        """Get the primary key column name for a table"""
        raise NotImplementedError(f"Primary key not defined for table {table_name}")

class GCPStorageConfig:
    def __init__(
        self,
        client_name: str,
        bucket_name: str,
        data_source: str,
        credentials_path: str = None
    ):
        self.client_name = client_name
        self.bucket_name = bucket_name
        self.data_source = data_source
        self.credentials_path = credentials_path

    def get_blob_path(self, dataset_name: str, timestamp: datetime = None) -> str:
        """Generate the GCP blob path for a dataset"""
        if timestamp is None:
            timestamp = datetime.now()
        
        timestamp_str = timestamp.strftime("%Y-%m-%d_%H")
        return f"{self.client_name}/{self.data_source}/{timestamp_str}/{dataset_name}.csv"