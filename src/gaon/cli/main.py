"""
Main CLI implementation for Gaon
"""
import logging
from pathlib import Path
from typing import Optional, Annotated
from datetime import datetime

import typer
from rich.console import Console
from rich.logging import RichHandler

from gaon.config.config import load_config, get_config
from gaon.storage.gcp.storage import GCPStorage
from gaon.integrate.sources.hubspot.client import HubspotClient

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger("gaon")

# Create typer app instance
app = typer.Typer(
    help="Gaon - A data integration tool",
    add_completion=True,
)

# Create rich console for better output
console = Console()

@app.callback(invoke_without_command=True)
def entry(
    config: Annotated[
        Optional[Path],
        typer.Option(
            help="Path to the config file",
            envvar="GAON_CONFIG",
            show_default=True,
        )
    ] = Path("test_config.json"),
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v",
            help="Enable verbose output",
        )
    ] = False,
) -> None:
    """
    Gaon - A data integration tool for managing and processing data from various sources.
    """
    # Set log level based on verbose flag
    if verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    else:
        logger.setLevel(logging.INFO)

    logger.debug(f"Starting Gaon CLI with config: {config}")
    
    if config:
        try:
            load_config(config)
            logger.debug("Configuration loaded successfully")
        except Exception as e:
            logger.exception("Error loading config")
            console.print(f"[red]Error loading config:[/red] {str(e)}")
            raise typer.Exit(1)

@app.command()
def extract(
    source: Annotated[
        str,
        typer.Argument(
            help="Name of the source to extract data from",
        )
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be done without actually extracting data",
        )
    ] = False,
) -> None:
    """
    Extract data from a configured source.
    """
    try:
        logger.info(f"Starting data extraction for source: {source}")
        config = get_config()
        
        # Look up source configuration
        logger.debug("Looking up source configuration")
        source_config = next((s for s in config.sources if s.name == source), None)
        if not source_config:
            logger.error(f"Source '{source}' not found in config")
            console.print(f"[red]Error:[/red] Source '{source}' not found in config")
            raise typer.Exit(1)
        
        # Initialize appropriate client based on source type
        if source_config.source_type == "hubspot":
            client = HubspotClient(source_config)
        else:
            logger.error(f"Unsupported source type: {source_config.source_type}")
            console.print(f"[red]Error:[/red] Unsupported source type: {source_config.source_type}")
            raise typer.Exit(1)
        
        if dry_run:
            logger.info("Dry run mode - no data will be extracted")
            console.print("[yellow]Dry run mode - no data will be extracted[/yellow]")
            logger.info(f"Would extract data from {source} between {source_config.start_time} and {source_config.end_time}")
            return
        
        # Extract data
        logger.info(f"Extracting data from {source} between {source_config.start_time} and {source_config.end_time}")
        for temp_file in client.extract(source_config.start_time, source_config.end_time):
            logger.info(f"Extracted batch to: {temp_file}")
        
        logger.info(f"Successfully completed data extraction for {source}")
        
    except Exception as e:
        logger.exception("Error during data extraction")
        console.print(f"[red]Error during data extraction:[/red] {str(e)}")
        raise typer.Exit(1)

@app.command()
def upload(
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be done without uploading",
        )
    ] = False,
) -> None:
    """
    Upload test.csv file to GCP storage under example_source.
    """
    try:
        source_name = "example_source"
        logger.info(f"Starting upload for source: {source_name}")
        config = get_config()
        
        # Look up source configuration
        logger.debug("Looking up source configuration")
        source = next((s for s in config.sources if s.name == source_name), None)
        if not source:
            logger.error(f"Source '{source_name}' not found in config")
            console.print(f"[red]Error:[/red] Source '{source_name}' not found in config")
            raise typer.Exit(1)
        
        # Check if test.csv exists
        test_file = Path("test.csv")
        if not test_file.exists():
            logger.error("test.csv not found in current directory")
            console.print("[red]Error:[/red] test.csv not found in current directory")
            raise typer.Exit(1)
        
        # Initialize GCP storage
        logger.debug("Initializing GCP storage")
        storage = GCPStorage(config.storage)
        
        # Generate date prefix for the current time
        date_prefix = datetime.now().strftime("%Y-%m-%d_%H")
        
        logger.info(f"Uploading test.csv to [blue]{source.name}[/blue]")
        if dry_run:
            logger.info("Dry run mode - no files will be uploaded")
            console.print("[yellow]Dry run mode - no files will be uploaded[/yellow]")
            logger.info(f"Would upload {test_file} to: {source.name}/{date_prefix}/{test_file.name}")
            return
        
        # Upload the file
        remote_path = storage.upload(source, date_prefix, test_file)
        logger.info(f"Successfully uploaded file to: {remote_path}")
        
    except Exception as e:
        logger.exception("Error during upload")
        console.print(f"[red]Error during upload:[/red] {str(e)}")
        raise typer.Exit(1)

if __name__ == "__main__":
    app() 