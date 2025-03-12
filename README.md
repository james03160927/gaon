# Gaon

A data integration tool for managing and processing data from various sources.

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package installer)
- Git

### Windows Installation Steps

#### Option 1: Using the Installation Script
1. Clone the repository:
```cmd
git clone https://github.com/your-username/gaon.git
cd gaon
```

2. Run the installation script:
```cmd
install.bat
```

#### Option 2: Manual Installation
1. Clone the repository:
```cmd
git clone https://github.com/your-username/gaon.git
cd gaon
```

2. Create a virtual environment:
```cmd
python -m venv venv
```

3. Activate the virtual environment:
```cmd
venv\Scripts\activate
```

4. Install the package in development mode:
```cmd
pip install -e .
```

## Usage

1. Create a configuration file (e.g., `config.json`):
```json
{
    "storage": {
        "bucket_name": "your-bucket-name",
        "credentials_path": "path/to/credentials.json"
    },
    "sources": [
        {
            "name": "example_source",
            "source_type": "sql",
            "dsn": "Driver={ODBC Driver 17 for SQL Server};Server=server_name;Database=db_name;UID=username;PWD=password"
        }
    ]
}
```

2. Run the CLI:
```cmd
# Show help
gaon --help

# Run with config file
gaon --config config.json

# Run with verbose output
gaon --config config.json --verbose

# Run integration
gaon --config config.json integrate --source example_source
```

## Development

To run in development mode:

1. Clone the repository
2. Create and activate virtual environment
3. Install development dependencies:
```cmd
pip install -e .
```

## Troubleshooting

### Common Issues on Windows

1. If `gaon` command is not recognized:
   - Make sure you see `(venv)` at the start of your command prompt
   - If not, run: `venv\Scripts\activate`
   - After activation, try: `pip install -e .` again
   - Check if gaon is in PATH: `where gaon`
   - Try running from the Scripts directory: `venv\Scripts\gaon --help`

2. If virtual environment activation fails:
   - Run PowerShell as Administrator
   - Run: `Set-ExecutionPolicy RemoteSigned`
   - Try activation again

3. If you see "ModuleNotFoundError":
   - Make sure you're in the correct directory
   - Run: `pip install -e .` again
   - Check installed packages: `pip list | findstr gaon`

4. For database connection issues:
   - Install SQL Server ODBC Driver from Microsoft's website
   - Verify your connection string in the config file
   - Test connection using Windows ODBC Data Source Administrator

### Getting Help

For more detailed output, use the `--verbose` flag:
```cmd
gaon --verbose --config config.json
```

### Manual Package Location Check

If the command is still not found, you can check where the package is installed:
```cmd
# Check where Python is installed
where python

# Check where pip is installing packages
pip show gaon

# Check if gaon script exists
dir venv\Scripts\gaon*
``` 