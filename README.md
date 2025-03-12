# Gaon

A data integration tool that supports multiple data sources including QuickBooks Desktop and HubSpot.

## Installation

```bash
pip install -e .
```

## Usage

1. Create a config file (e.g., `config.json`):
```json
{
    "storage": {
        "bucket_name": "your-bucket-name/client_x",
        "credentials_path": "path/to/gcp-credentials.json"
    },
    "sources": [
        {
            "name": "qb_client_1",
            "source_type": "quickbooks_desktop",
            "dsn": "QuickBooks_DSN_1"
        }
    ]
}
```

2. Run the CLI:
```bash
gaon integrate --source qb_client_1
```

## Development

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install development dependencies:
```bash
pip install -e ".[dev]"
```

3. Run tests:
```bash
pytest
``` 