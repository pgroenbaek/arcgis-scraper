# arcgis-scraper

[![GitHub release (latest by date)](https://img.shields.io/github/v/release/pgroenbaek/arcgis-scraper?style=flat&label=Latest%20Version)](https://github.com/pgroenbaek/arcgis-scraper/releases)
[![Python 3.6+](https://img.shields.io/badge/Python-3.6%2B-blue?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![License MIT](https://img.shields.io/badge/License-MIT-lightgrey?style=flat&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA2NDAgNTEyIj4KICA8IS0tIEZvbnQgQXdlc29tZSBGcmVlIDYuNy4yIGJ5IEBmb250YXdlc29tZSAtIGh0dHBzOi8vZm9udGF3ZXNvbWUuY29tIExpY2Vuc2UgLSBodHRwczovL2ZvbnRhd2Vzb21lLmNvbS9saWNlbnNlL2ZyZWUgQ29weXJpZ2h0IDIwMjUgRm9udGljb25zLCBJbmMuIC0tPgogIDxwYXRoIGZpbGw9IndoaXRlIiBkPSJNMzg0IDMybDEyOCAwYzE3LjcgMCAzMiAxNC4zIDMyIDMycy0xNC4zIDMyLTMyIDMyTDM5OC40IDk2Yy01LjIgMjUuOC0yMi45IDQ3LjEtNDYuNCA1Ny4zTDM1MiA0NDhsMTYwIDBjMTcuNyAwIDMyIDE0LjMgMzIgMzJzLTE0LjMgMzItMzIgMzJsLTE5MiAwLTE5MiAwYy0xNy43IDAtMzItMTQuMy0zMi0zMnMxNC4zLTMyIDMyLTMybDE2MCAwIDAtMjk0LjdjLTIzLjUtMTAuMy00MS4yLTMxLjYtNDYuNC01Ny4zTDEyOCA5NmMtMTcuNyAwLTMyLTE0LjMtMzItMzJzMTQuMy0zMiAzMi0zMmwxMjggMGMxNC42LTE5LjQgMzcuOC0zMiA2NC0zMnM0OS40IDEyLjYgNjQgMzJ6bTU1LjYgMjg4bDE0NC45IDBMNTEyIDE5NS44IDQzOS42IDMyMHpNNTEyIDQxNmMtNjIuOSAwLTExNS4yLTM0LTEyNi03OC45Yy0yLjYtMTEgMS0yMi4zIDYuNy0zMi4xbDk1LjItMTYzLjJjNS04LjYgMTQuMi0xMy44IDI0LjEtMTMuOHMxOS4xIDUuMyAyNC4xIDEzLjhsOTUuMiAxNjMuMmM1LjcgOS44IDkuMyAyMS4xIDYuNyAzMi4xQzYyNy4yIDM4MiA1NzQuOSA0MTYgNTEyIDQxNnpNMTI2LjggMTk1LjhMNTQuNCAzMjBsMTQ0LjkgMEwxMjYuOCAxOTUuOHpNLjkgMzM3LjFjLTIuNi0xMSAxLTIyLjMgNi43LTMyLjFsOTUuMi0xNjMuMmM1LTguNiAxNC4yLTEzLjggMjQuMS0xMy44czE5LjEgNS4zIDI0LjEgMTMuOGw5NS4yIDE2My4yYzUuNyA5LjggOS4zIDIxLjEgNi43IDMyLjFDMjQyIDM4MiAxODkuNyA0MTYgMTI2LjggNDE2UzExLjcgMzgyIC45IDMzNy4xeiIvPgo8L3N2Zz4=&logoColor=%23ffffff)](/LICENSE)

A simple scraper to download data from ArcGIS FeatureServer and MapServer layers.

## Installation

The Python module can be installed in the following ways:

### Install from PyPI

```sh
pip install --upgrade arcgis-scraper
```

### Install from wheel

If you have downloaded a `.whl` file from the [Releases](https://github.com/pgroenbaek/arcgis-scraper/releases) page, install it with:

```sh
pip install path/to/arcgis_scraper‑<version>‑py3‑none‑any.whl
```

Replace `<version>` with the actual version number in the filename. For example:

```sh
pip install path/to/arcgis_scraper-1.0.1-py3-none-any.whl
```

### Install from source

```sh
git clone https://github.com/pgroenbaek/arcgis-scraper.git
pip install --upgrade ./arcgis-scraper
```

# Usage

```python
from arcgisscraper import ArcGISScraper

# Base URL for the ArcGIS REST services
base_url = "https://services1.arcgis.com/QcgJt0vVxSaqMKl7/arcgis/rest/services/"

# List of layers to scrape
query_urls = [
    "Adgangsveje_OD/FeatureServer/0/query",
    "Afsnitsmidter_OD/FeatureServer/0/query",
    "Basisspor_OD/FeatureServer/0/query",
]

# Initialize the scraper
scraper = ArcGISScraper(
    base_url=base_url,
    export_directory="./data",
    export_format="csv", # Options: "csv", "json", "parquet"
    max_requests_per_second=1.0,
    token=None, # Optional: token for secured services
    max_retries=3,
)

# Scrape multiple layers
scraper.scrape_layers(query_urls)

# Scrape a single layer with filtering and custom filename
scraper.scrape_layer(
    "Basisspor_OD/FeatureServer/0/query",
    filename="basisspor_filtered",
    where="SporType='Main'",
    out_fields="ID,Name,SporType"
)
```


## Running Tests

You can run tests manually or use `tox` to test across multiple Python versions.

### Run Tests Manually
First, install the required dependencies:

```sh
pip install pytest
```

Then, run tests with:

```sh
pytest
```


## Run Tests with `tox`

`tox` allows you to test across multiple Python environments.

### **1. Install `tox`**
```sh
pip install tox
```

### **2. Run Tests**
```sh
tox
```

This will execute tests in all specified Python versions.

### **3. `tox.ini` Configuration**
The `tox.ini` file should be in your project root:

```ini
[tox]
envlist = py36, py37, py38, py39, py310

[testenv]
deps = pytest
commands = pytest
```

Modify `envlist` to match the Python versions you want to support.

## License

This Python module was created by Peter Grønbæk Andersen and is licensed under the [MIT License](/LICENSE).
