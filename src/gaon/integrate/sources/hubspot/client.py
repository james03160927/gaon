"""
Hubspot client implementation
"""
import csv
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Optional

from hubspot import HubSpot
from hubspot.crm.objects import ApiException
from hubspot.crm.objects.models import PublicObjectSearchRequest
import certifi

from gaon.config.models import HubspotObjectType, HubspotSourceConfig, SourceConfig
from gaon.integrate.base import BaseIntegrate

logger = logging.getLogger(__name__)

# Map object types to their API clients and endpoints
OBJECT_TYPE_TO_API = {
    HubspotObjectType.CONTACTS: ("contacts", "search"),
    HubspotObjectType.COMPANIES: ("companies", "basic"),
    HubspotObjectType.DEALS: ("deals", "basic"),
    HubspotObjectType.TICKETS: ("tickets", "basic")
}

class HubspotClient(BaseIntegrate):
    """Hubspot client implementation."""
    
    def __init__(self, source_config: SourceConfig) -> None:
        """Initialize Hubspot client.
        
        Args:
            source_config: Source configuration containing Hubspot API settings
        """
        self._api_clients: Dict[HubspotObjectType, Any] = {}
        self._client: Optional[HubSpot] = None
        self._typed_config: Optional[HubspotSourceConfig] = None
        super().__init__(source_config)
    
    def _initialize(self) -> None:
        """Initialize Hubspot client and test connection."""
        try:
            self._typed_config = self.source_config.get_source_config()
            if not isinstance(self._typed_config, HubspotSourceConfig):
                raise ValueError("Invalid source configuration type")
            
            # Set SSL certificates
            os.environ["REQUESTS_CA_BUNDLE"] = os.environ["SSL_CERT_FILE"] = certifi.where()
            
            # Initialize Hubspot client
            self._client = HubSpot(access_token=self._typed_config.api_key)
            
            # Initialize API clients for enabled object types
            for object_type, object_config in self._typed_config.objects.items():
                if not object_config.enabled:
                    continue
                    
                api_info = OBJECT_TYPE_TO_API.get(object_type)
                if not api_info:
                    logger.warning(f"Unsupported object type: {object_type.value}")
                    continue

                api_path, api_type = api_info
                api_client = getattr(self._client.crm, api_path, None)
                if not api_client:
                    logger.warning(f"No API client available for {object_type.value}")
                    continue
                
                # Test connection
                self._test_connection(api_client, api_type)
                
                self._api_clients[object_type] = api_client
                logger.info(f"Initialized client for {object_type.value}")
            
            if not self._api_clients:
                raise ValueError("No enabled object types configured")
                
        except ApiException as e:
            raise ValueError(f"Failed to connect to Hubspot API: {str(e)}")
        except Exception as e:
            raise ValueError(f"Failed to initialize Hubspot integration: {str(e)}")
    
    def _test_connection(self, api_client: Any, api_type: str) -> None:
        """Test API connection.
        
        Args:
            api_client: The API client to test
            api_type: Type of API (basic or search)
        """
        if api_type == "search":
            search_request = PublicObjectSearchRequest(
                filter_groups=[], sorts=[], properties=[], limit=1, after=0
            )
            api_client.search_api.do_search(public_object_search_request=search_request)
        else:
            api_client.basic_api.get_page(properties=[], limit=1, after=0)
    
    def extract(self, start_time: datetime, end_time: datetime) -> Generator[Path, None, None]:
        """Extract data from Hubspot in batches.
        
        Args:
            start_time: Start time for data extraction
            end_time: End time for data extraction
            
        Yields:
            Path: Path to temporary file containing batch data
        """
        try:
            self._validate_time_range(start_time, end_time)
            
            for object_type, api_client in self._api_clients.items():
                object_config = self._typed_config.objects[object_type]
                if not object_config.enabled:
                    continue
                    
                logger.info(f"Extracting {object_type.value}")
                yield from self._extract_object_type(object_type, api_client, object_config)
                    
        except Exception as e:
            raise ValueError(f"Failed to extract data from Hubspot: {str(e)}")
    
    def _extract_object_type(self, object_type: HubspotObjectType, api_client: Any, 
                           object_config: Any) -> Generator[Path, None, None]:
        """Extract data for a specific object type.
        
        Args:
            object_type: Type of object to extract
            api_client: API client to use
            object_config: Configuration for the object type
            
        Yields:
            Path: Path to temporary file containing batch data
        """
        batch_number = 0
        after = 0
        
        while True:
            response = self._get_api_response(
                api_client, object_type, object_config, after, batch_number
            )
            
            if not response.results:
                break
            
            temp_file = self._create_temp_file(f"{object_type.value}_{batch_number}")
            self._write_batch_to_csv(response.results, temp_file)
            yield temp_file
            
            if not response.paging:
                break
                
            after = response.paging.next.after
            batch_number += 1
    
    def _get_api_response(self, api_client: Any, object_type: HubspotObjectType, 
                         object_config: Any, after: int, batch_number: int) -> Any:
        """Get response from Hubspot API.
        
        Args:
            api_client: API client to use
            object_type: Type of object to fetch
            object_config: Configuration for the object type
            after: Pagination offset
            batch_number: Current batch number
            
        Returns:
            API response containing results
        """
        api_info = OBJECT_TYPE_TO_API.get(object_type)
        if not api_info:
            raise ValueError(f"Unsupported object type: {object_type.value}")
            
        _, api_type = api_info
        properties = object_config.properties
        limit = self.source_config.batch_size
        
        if api_type == "basic":
            return api_client.basic_api.get_page(
                properties=properties,
                limit=limit,
                after=after
            )
        
        search_request = PublicObjectSearchRequest(
            sorts=[{"propertyName": "lastmodifieddate", "direction": "ASCENDING"}],
            properties=properties,
            limit=limit,
            after=after
        )
        return api_client.search_api.do_search(public_object_search_request=search_request)
    
    def _write_batch_to_csv(self, results: list[dict[str, Any]], file_path: Path) -> None:
        """Write batch results to CSV file."""
        if not results:
            logger.warning(f"No results to write to CSV file: {file_path.absolute()}")
            return
            
        fieldnames = sorted(set().union(*(result.properties.keys() for result in results)))
        
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(result.properties for result in results)