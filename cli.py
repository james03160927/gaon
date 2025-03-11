import typer
from typing import List, Optional
from gaon.integrations.factory import DataSourceType, IntegrationFactory
from gaon.storage.gcp import GCPStorageHandler
from gaon.integrations.base import GCPStorageConfig
import pandas as pd

app = typer.Typer()

def get_table_selection(available_tables: List[str]) -> List[str]:
    """Helper function to get table selection from user"""
    selected_tables = []
    for table in available_tables:
        if typer.confirm(f"Include table '{table}'?"):
            selected_tables.append(table)
    return selected_tables

@app.command()
def integrate(
    source: DataSourceType = typer.Argument(
        ...,
        case_sensitive=False,
        help="The data source to integrate with (quickbooks_desktop or hubspot)"
    ),
    client: str = typer.Option(
        ...,
        "--client",
        "-c",
        help="Client name for data organization"
    ),
    bucket: str = typer.Option(
        ...,
        "--bucket",
        "-b",
        help="GCP bucket name for data storage"
    ),
    batch_size: int = typer.Option(
        1000,
        "--batch-size",
        "-s",
        help="Number of records to process in each batch"
    ),
    gcp_credentials: Optional[str] = typer.Option(
        None,
        "--gcp-credentials",
        help="Path to GCP service account credentials JSON file"
    )
):
    """Setup integration with a data source and retrieve data"""
    # Initialize integration
    integration = IntegrationFactory.get_integration(source)
    
    # Get and set credentials
    creds = integration.get_required_credentials()
    for cred_name, cred_info in creds.items():
        value = typer.prompt(
            cred_info["description"],
            hide_input=cred_info["is_secret"]
        )
        getattr(integration, f"set_credentials")(value)
    
    # Get table selection
    available_tables = integration.get_available_tables()
    typer.echo("\nSelect tables to integrate:")
    selected_tables = get_table_selection(available_tables)
    
    # Setup the integration
    integration.setup(selected_tables)
    
    # Initialize GCP storage
    storage_config = GCPStorageConfig(
        client_name=client,
        bucket_name=bucket,
        data_source=source.value,
        credentials_path=gcp_credentials
    )
    storage_handler = GCPStorageHandler(bucket, gcp_credentials)
    
    # Process each selected table
    for table in selected_tables:
        typer.echo(f"\nProcessing table: {table}")
        last_id = None
        batch_num = 1
        
        for batch_df in integration.retrieve_data(table, batch_size, last_id):
            typer.echo(f"Processing batch {batch_num} ({len(batch_df)} records)")
            
            # Upload to GCP
            blob_path = storage_config.get_blob_path(table)
            if batch_num == 1:
                if_exists = 'replace'
            else:
                if_exists = 'append'
            
            storage_handler.upload_dataframe(batch_df, blob_path, if_exists=if_exists)
            typer.echo(f"Uploaded batch {batch_num} to {blob_path}")
            
            batch_num += 1
            last_id = batch_df[integration.get_primary_key(table)].iloc[-1]

if __name__ == "__main__":
    app() 