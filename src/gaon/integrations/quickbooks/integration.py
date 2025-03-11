from typing import List, Generator, Any
import pandas as pd
import pyodbc
from ..base import DataSourceIntegration

class QuickBooksIntegration(DataSourceIntegration):
    def __init__(self):
        self.dsn = None
        self.conn = None
        self._primary_keys = {
            "Customers": "ListID",
            "Invoices": "TxnID",
            "Items": "ListID",
            "Vendors": "ListID"
        }
    
    def get_available_tables(self) -> List[str]:
        return list(self._primary_keys.keys())
    
    def get_required_credentials(self) -> dict:
        return {
            "dsn": {
                "type": "string",
                "description": "DSN name for QuickBooks connection",
                "is_secret": False
            }
        }
    
    def setup(self, selected_tables: List[str]) -> None:
        if not self.dsn:
            raise ValueError("DSN must be set before setup")
        
        # Test connection
        self.conn = pyodbc.connect(f"DSN={self.dsn}")
        print(f"Successfully connected to QuickBooks using DSN: {self.dsn}")
        print("Selected tables:")
        for table in selected_tables:
            print(f"  - {table}")
    
    def set_credentials(self, dsn: str) -> None:
        """Set the DSN for QuickBooks connection"""
        self.dsn = dsn
    
    def get_primary_key(self, table_name: str) -> str:
        """Get the primary key column for a table"""
        if table_name not in self._primary_keys:
            raise ValueError(f"Unknown table: {table_name}")
        return self._primary_keys[table_name]
    
    def retrieve_data(
        self,
        table_name: str,
        batch_size: int = 1000,
        last_processed_id: Any = None
    ) -> Generator[pd.DataFrame, None, None]:
        """Retrieve data from QuickBooks in batches"""
        if not self.conn:
            self.conn = pyodbc.connect(f"DSN={self.dsn}")
        
        primary_key = self.get_primary_key(table_name)
        
        while True:
            # Build query with pagination
            query = f"SELECT * FROM {table_name}"
            if last_processed_id:
                query += f" WHERE {primary_key} > ?"
                params = [last_processed_id]
            else:
                params = []
            
            query += f" ORDER BY {primary_key} LIMIT {batch_size}"
            
            # Fetch batch
            df = pd.read_sql(query, self.conn, params=params)
            
            if df.empty:
                break
                
            yield df
            
            if len(df) < batch_size:
                break
                
            last_processed_id = df[primary_key].iloc[-1] 