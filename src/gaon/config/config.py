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

class GaonConfig(BaseModel):
    storage: StorageConfig
    sources: List[SourceConfig]

    @classmethod
    def from_json(cls, config_path: Path) -> "GaonConfig":
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
_config: Optional[GaonConfig] = None

def get_config() -> GaonConfig:
    """Get the global config instance"""
    if _config is None:
        logger.error("Config not initialized. Call load_config first.")
        raise RuntimeError("Config not initialized. Call load_config first.")
    return _config

def load_config(config_path: Path) -> GaonConfig:
    """Load and validate the config file"""
    logger.info(f"Loading configuration from {config_path}")
    global _config
    _config = GaonConfig.from_json(config_path)
    logger.info("Configuration loaded successfully")
    return _config
