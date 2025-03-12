from setuptools import setup, find_packages

setup(
    name="gaon",
    version="0.1.0",
    description="A data integration tool",
    author="James Kwon",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "typer>=0.9.0",
        "pyodbc>=4.0.39",
        "google-cloud-storage>=2.14.0",
        "pydantic>=2.6.0",
        "typing-extensions>=4.9.0",
    ],
    entry_points={
        "console_scripts": [
            "gaon=gaon.cli:app",
        ],
    },
)
