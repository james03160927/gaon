"""
Configuration loading and management for Gaon
"""
import json
import logging
from pathlib import Path
from typing import Optional

from gaon.config.models import Config

logger = logging.getLogger(__name__)

_config: Optional[Config] = None

def load_config(config_path: Path) -> None:
    """Load configuration from file.
    
    Args:
        config_path: Path to configuration file
        
    Raises:
        FileNotFoundError: If config file does not exist
        ValueError: If config is invalid
    """
    global _config
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
        
    try:
        with open(config_path) as f:
            config_data = json.load(f)
            
        _config = Config(**config_data)
        logger.debug("Configuration loaded successfully")
        
    except Exception as e:
        raise ValueError(f"Failed to load config: {str(e)}")

def get_config() -> Config:
    """Get the current configuration.
    
    Returns:
        Config: Current configuration
        
    Raises:
        RuntimeError: If configuration has not been loaded
    """
    if _config is None:
        raise RuntimeError("Configuration has not been loaded")
    return _config
