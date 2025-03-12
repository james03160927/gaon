"""
Main CLI implementation for Gaon
"""
import logging
from pathlib import Path
from typing import Optional, Annotated

import typer
from rich.console import Console
from rich.logging import RichHandler

from gaon.config.config import load_config, get_config
from gaon.integrate.core import integrate_source

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
    ] = Path("config.json"),
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
def integrate(
    source_name: Annotated[
        str,
        typer.Option(
            "--source", "-s",
            help="Name of the source to integrate with",
            prompt="Enter source name",
        )
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be done without making changes",
        )
    ] = False,
) -> None:
    """
    Setup and run a data source integration.
    """
    try:
        logger.info(f"Starting integration for source: {source_name}")
        config = get_config()
        
        logger.debug("Looking up source configuration")
        source = next((s for s in config.sources if s.name == source_name), None)
        if not source:
            logger.error(f"Source '{source_name}' not found in config")
            console.print(f"[red]Error:[/red] Source '{source_name}' not found in config")
            raise typer.Exit(1)
        
        logger.info(f"Integrating with [blue]{source.name}[/blue] ({source.source_type})")
        if dry_run:
            logger.info("Dry run mode - no changes will be made")
            console.print("[yellow]Dry run mode - no changes will be made[/yellow]")
        
        logger.debug(f"Starting integration with source config: {source.model_dump_json(indent=2)}")
        integrate_source(source, dry_run=dry_run)
        logger.info("Integration completed successfully")
        
    except Exception as e:
        logger.exception("Error during integration")
        console.print(f"[red]Error during integration:[/red] {str(e)}")
        raise typer.Exit(1)

if __name__ == "__main__":
    app() 