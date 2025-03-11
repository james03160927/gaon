from enum import Enum
from typing import Type
from .base import DataSourceIntegration
from .quickbooks.integration import QuickBooksIntegration
from .hubspot.integration import HubSpotIntegration

class DataSourceType(str, Enum):
    QUICKBOOKS_DESKTOP = "quickbooks_desktop"
    HUBSPOT = "hubspot"

class IntegrationFactory:
    _integrations = {
        DataSourceType.QUICKBOOKS_DESKTOP: QuickBooksIntegration,
        DataSourceType.HUBSPOT: HubSpotIntegration,
    }
    
    @classmethod
    def get_integration(cls, source_type: DataSourceType) -> DataSourceIntegration:
        """Get an instance of the specified integration type"""
        integration_class = cls._integrations.get(source_type)
        if not integration_class:
            raise ValueError(f"Unknown data source type: {source_type}")
        return integration_class()
    
    @classmethod
    def get_available_sources(cls) -> list[DataSourceType]:
        """Get list of available data source types"""
        return list(cls._integrations.keys()) 