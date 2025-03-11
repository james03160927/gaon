from setuptools import setup, find_packages

setup(
    name="gaon",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "typer",
        "pyodbc",
        "google-cloud-storage",
    ],
    entry_points={
        "console_scripts": [
            "gaon=gaon.cli:app",
        ],
    },
) 