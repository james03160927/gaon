from typing import List
from ..base import DataSourceIntegration

class HubSpotIntegration(DataSourceIntegration):
    def __init__(self):
        self.api_key = None
        
    def get_available_tables(self) -> List[str]:
        return ["Contacts", "Companies", "Deals", "Tickets"]
    
    def get_required_credentials(self) -> dict:
        return {
            "api_key": {
                "type": "string",
                "description": "HubSpot API Key",
                "is_secret": True
            }
        }
    
    def setup(self, selected_tables: List[str]) -> None:
        if not self.api_key:
            raise ValueError("API key must be set before setup")
            
        # Here you would implement the actual HubSpot API connection logic
        # For now, we'll just print the configuration
        print(f"Setting up HubSpot integration with API key: ****{self.api_key[-4:]}")
        print("Selected tables:")
        for table in selected_tables:
            print(f"  - {table}")
    
    def set_credentials(self, api_key: str) -> None:
        """Set the API key for HubSpot connection"""
        self.api_key = api_key 