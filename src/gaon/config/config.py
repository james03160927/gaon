"""
Configuration management for Gaon
"""
import json
import logging
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field

# Set up logging
logger = logging.getLogger(__name__)

class StorageConfig(BaseModel):
    bucket_name: str
    credentials_path: Path

class SourceConfig(BaseModel):
    name: str
    source_type: str
    dsn: str

class Config(BaseModel):
    """Main configuration class."""
    storage: StorageConfig
    sources: List[SourceConfig]

    @classmethod
    def from_json(cls, config_path: Path) -> "Config":
        """Load config from a JSON file"""
        logger.debug(f"Loading config from: {config_path}")
            
        if not config_path.exists():
            logger.error(f"Config file not found: {config_path}")
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        with open(config_path) as f:
            logger.debug("Reading config file contents")
            config_data = json.load(f)
            logger.debug(f"Loaded raw config data: {json.dumps(config_data, indent=2)}")
            
            logger.debug("Validating config data against schema")
            config = cls.model_validate(config_data)
            logger.debug("Config validation successful")
            return config

# Global config instance
_config: Optional[Config] = None

def get_config() -> Config:
    """Get the current configuration.
    
    Returns:
        Config: Current configuration
        
    Raises:
        ValueError: If configuration hasn't been loaded
    """
    if _config is None:
        raise ValueError("Configuration not loaded. Call load_config first.")
    return _config

def load_config(config_path: Path) -> Config:
    """Load configuration from a file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Config: Loaded configuration
        
    Raises:
        ValueError: If the configuration is invalid
    """
    global _config
    
    if not config_path.exists():
        raise ValueError(f"Config file not found: {config_path}")
        
    try:
        with open(config_path) as f:
            config_data = json.load(f)
        
        _config = Config(**config_data)
        return _config
        
    except Exception as e:
        raise ValueError(f"Failed to load config: {str(e)}")
