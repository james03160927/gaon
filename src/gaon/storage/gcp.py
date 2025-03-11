from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from typing import Optional
import io

class GCPStorageHandler:
    def __init__(self, bucket_name: str, credentials_path: Optional[str] = None):
        """
        Initialize GCP Storage handler
        
        Args:
            bucket_name: Name of the GCP bucket
            credentials_path: Path to service account credentials JSON file
        """
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            self.client = storage.Client(credentials=credentials)
        else:
            self.client = storage.Client()
        
        self.bucket = self.client.bucket(bucket_name)
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        blob_path: str,
        if_exists: str = 'fail'
    ) -> None:
        """
        Upload a DataFrame to GCP Storage
        
        Args:
            df: DataFrame to upload
            blob_path: Path within the bucket where the file should be stored
            if_exists: What to do if the blob exists ('fail', 'replace', 'append')
        """
        blob = self.bucket.blob(blob_path)
        
        if blob.exists() and if_exists == 'fail':
            raise FileExistsError(f"Blob {blob_path} already exists")
        
        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        if if_exists == 'append' and blob.exists():
            # Read existing content
            existing_content = blob.download_as_text()
            # Append new content without header
            csv_buffer.seek(0)
            lines = csv_buffer.readlines()
            content = existing_content + ''.join(lines[1:])
        else:
            content = csv_buffer.getvalue()
        
        # Upload to GCP
        blob.upload_from_string(content, content_type='text/csv') 