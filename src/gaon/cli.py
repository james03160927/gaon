import typer
from typing import List
from gaon.integrations.factory import DataSourceType, IntegrationFactory
from typing_extensions import Annotated

app = typer.Typer()

def get_table_selection(available_tables: List[str]) -> List[str]:
    """Helper function to get table selection from user"""
    selected_tables = []
    for table in available_tables:
        if typer.confirm(f"Include table '{table}'?"):
            selected_tables.append(table)
    return selected_tables


@app.callback(invoke_without_command=True)
def entry():
    """The entry point for the gaon CLI"""
    pass

@app.command("integrate")
def (
    source: Annotated[
        DataSourceType,
        typer.Option(
            help="The data source to integrate with (quickbooks_desktop or hubspot)"
        )
    ] = DataSourceType.QUICKBOOKS_DESKTOP
):
    """Setup a new data source integration"""
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

if __name__ == "__main__":
    app() 