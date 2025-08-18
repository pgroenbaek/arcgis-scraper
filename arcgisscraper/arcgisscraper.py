import os
import time
import requests
import pandas as pd
from typing import List, Dict, Optional


class ArcGISScraper:
    """
    ArcGIS REST API scraper.

    Features:
    - Scrape single or multiple layers
    - Configurable pagination and rate limiting
    - Token-based authentication for secured services
    - Exports to CSV, JSON, or Parquet
    - Automatic retries with exponential backoff
    - Metadata fetching (fields, geometry type, spatial reference)
    - Filtering (where clause, selected fields, geometry)
    """
    def __init__(self,
        base_url: str,
        export_directory: str = "./data",
        page_size: int = 1000,
        export_format: str = "csv",
        max_requests_per_second: float = 1.0,
        token: Optional[str] = None,
        max_retries: int = 5,
    ):
        """
        Initialize the ArcGISScraper.

        Args:
            base_url: Base ArcGIS REST service URL.
            export_directory: Directory where output files will be stored.
            page_size: Number of records to fetch per request.
            export_format: Output format (csv, json, parquet).
            max_requests_per_second: Rate limiting value.
            token: Optional authentication token for secured services.
            max_retries: Maximum number of retries for failed requests.
        """
        self.base_url = base_url.rstrip("/") + "/"
        self.export_directory = export_directory
        self.page_size = page_size
        self.export_format = export_format.lower()
        self.min_delay = 1.0 / max_requests_per_second if max_requests_per_second > 0 else 0
        self._last_request_time = 0.0
        self.token = token
        self.max_retries = max_retries

        if not os.path.exists(self.export_directory):
            os.makedirs(self.export_directory)

    def _rate_limit(self):
        """Apply rate limiting based on max_requests_per_second."""
        now = time.time()
        elapsed = now - self._last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self._last_request_time = time.time()

    def _request(self, url: str, params: Dict) -> Dict:
        """Perform a GET request with retry and exponential backoff."""
        attempt = 0
        while True:
            try:
                self._rate_limit()
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                attempt += 1
                if attempt > self.max_retries:
                    raise RuntimeError(f"Failed after {self.max_retries} retries: {e}")
                sleep_time = min(2 ** attempt, 60)
                print(f"Request failed ({e}), retrying in {sleep_time}s...")
                time.sleep(sleep_time)

    def fetch_metadata(self, query_url: str) -> Dict:
        """
        Fetch metadata for a given layer.

        Args:
            query_url: Layer query endpoint (e.g., "LayerName/FeatureServer/0/query").

        Returns:
            Dictionary containing metadata information about the layer.
        """
        layer_url = self.base_url + query_url.split("/query")[0]
        params = {"f": "json"}
        if self.token:
            params["token"] = self.token
        return self._request(layer_url, params)

    def _fetch_layer(self,
        query_url: str,
        where: str = "1=1",
        out_fields: str = "*",
        geometry: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch all features from a given layer with pagination.

        Args:
            query_url: Layer query endpoint.
            where: SQL where clause to filter results.
            out_fields: Comma-separated list of fields to return.
            geometry: Optional geometry filter.

        Returns:
            List of features in dictionary format.
        """
        layer_url = self.base_url + query_url
        params = {
            "where": where,
            "outFields": out_fields,
            "f": "json",
            "resultOffset": 0,
            "resultRecordCount": self.page_size,
        }
        if geometry:
            params["geometry"] = geometry
        if self.token:
            params["token"] = self.token

        all_features = []
        while True:
            data = self._request(layer_url, params)
            if "features" not in data or len(data["features"]) == 0:
                break
            all_features.extend(data["features"])
            params["resultOffset"] += self.page_size
        return all_features

    def _export(self, features: List[Dict], filename: str):
        """
        Export features to the configured format.

        Args:
            features: List of features to export.
            filename: Output filename (without extension).
        """
        if not features:
            print(f"No data to export for {filename}")
            return

        df = pd.json_normalize(features)
        filepath = os.path.join(self.export_directory, filename)

        if self.export_format == "csv":
            df.to_csv(filepath + ".csv", index=False)
        elif self.export_format == "json":
            df.to_json(filepath + ".json", orient="records", force_ascii=False)
        elif self.export_format == "parquet":
            df.to_parquet(filepath + ".parquet", index=False)
        else:
            raise ValueError(f"Unsupported export format: {self.export_format}")

        print(f"Exported {filename} ({len(df)} records)")

    def scrape_layer(self,
        query_url: str,
        filename: Optional[str] = None,
        where: str = "1=1",
        out_fields: str = "*",
        geometry: Optional[str] = None,
    ):
        """
        Scrape a single layer and export it to the configured format.

        Args:
            query_url: Layer query endpoint.
            filename: Optional output filename (default: layer name).
            where: SQL where clause to filter results.
            out_fields: Comma-separated list of fields to return.
            geometry: Optional geometry filter.
        """
        features = self._fetch_layer(query_url, where, out_fields, geometry)
        if not filename:
            filename = query_url.split("/")[0]
        self._export(features, filename)

    def scrape_layers(self, query_urls: List[str]):
        """
        Scrape multiple layers sequentially.

        Args:
            query_urls: List of query endpoints to scrape.
        """
        for idx, query_url in enumerate(query_urls, 1):
            print(f"Scraping {idx}/{len(query_urls)}: {query_url}")
            self.scrape_layer(query_url)


