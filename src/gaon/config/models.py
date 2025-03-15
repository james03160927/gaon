"""
Configuration models for Gaon
"""
from datetime import datetime
from typing import Dict, List, Literal, Union, Any, Optional
from pathlib import Path
from enum import Enum

from pydantic import BaseModel

class StorageConfig(BaseModel):
    """Storage configuration."""
    bucket_name: str
    credentials_path: str

class SQLSourceConfig(BaseModel):
    """SQL source specific configuration."""
    dsn: str

class HubspotObjectType(str, Enum):
    """Supported Hubspot object types."""
    CONTACTS = "contacts"
    COMPANIES = "companies"
    DEALS = "deals"
    TICKETS = "tickets"

class HubspotObjectConfig(BaseModel):
    """Configuration for a specific Hubspot object type."""
    enabled: bool = True
    properties: List[str]

class HubspotSourceConfig(BaseModel):
    """Hubspot source specific configuration."""
    api_key: str
    objects: Dict[HubspotObjectType, HubspotObjectConfig]

# Union type for all possible source configurations
SourceConfigType = Union[SQLSourceConfig, HubspotSourceConfig]

class SourceConfig(BaseModel):
    """Source configuration."""
    name: str
    source_type: Literal["sql", "hubspot"]  # Add more types as needed
    batch_size: int = 1000
    start_time: datetime
    end_time: datetime
    cadence: Literal["daily", "hourly"] = "daily"
    source_config: SourceConfigType  # This will be different based on source_type

    def get_source_config(self) -> Union[SQLSourceConfig, HubspotSourceConfig]:
        """Get the typed source configuration based on source_type.
        
        Returns:
            Union[SQLSourceConfig, HubspotSourceConfig]: The typed source configuration
            
        Raises:
            ValueError: If source_type is not supported
        """
        if self.source_type == "sql":
            return SQLSourceConfig(**self.source_config.model_dump())
        elif self.source_type == "hubspot":
            return HubspotSourceConfig(**self.source_config.model_dump())
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

class Config(BaseModel):
    """Main configuration."""
    storage: StorageConfig
    sources: list[SourceConfig]
    client: str = "default"

    def __init__(self, **data):
        """Initialize configuration from dictionary.
        
        Args:
            config_dict: Dictionary containing configuration
        """
        super().__init__(**data)
        self.client = data.get("client", "default")
        self.storage = StorageConfig(**data.get("storage", {}))
        self.sources = [SourceConfig(**s) for s in data.get("sources", [])] 