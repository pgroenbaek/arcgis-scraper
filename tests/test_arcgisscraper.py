"""
This file is part of ArcGISScraper.

Copyright (C) 2025 Peter Grønbæk Andersen <peter@grnbk.io>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import pytest
import tempfile
import shutil
import requests
from unittest.mock import patch, MagicMock
import pandas as pd

from arcgisscraper import ArcGISScraper


@pytest.fixture
def temp_export_dir():
    tmpdir = tempfile.mkdtemp()
    yield tmpdir
    shutil.rmtree(tmpdir)


def test_init_creates_directory(temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com/arcgis/rest/services", export_directory=temp_export_dir)
    assert os.path.exists(scraper.export_directory)


def test_rate_limit_enforces_delay(temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com", export_directory=temp_export_dir, max_requests_per_second=2)
    scraper._last_request_time = 0
    scraper._rate_limit()


def test_request_success(monkeypatch, temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com", export_directory=temp_export_dir)

    mock_response = MagicMock()
    mock_response.json.return_value = {"features": []}
    mock_response.raise_for_status.return_value = None

    monkeypatch.setattr("requests.get", lambda *a, **k: mock_response)

    result = scraper._request("http://example.com", {})
    assert "features" in result


def test_request_retries_on_failure(monkeypatch, temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com", export_directory=temp_export_dir, max_retries=2)

    call_count = {"count": 0}

    def fake_get(*args, **kwargs):
        call_count["count"] += 1
        if call_count["count"] < 2:
            raise requests.exceptions.ConnectionError("Temporary failure")
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"features": []}
        mock_resp.raise_for_status.return_value = None
        return mock_resp

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("time.sleep", lambda s: None)

    result = scraper._request("http://example.com", {})
    assert isinstance(result, dict)
    assert call_count["count"] == 2


def test_fetch_metadata(monkeypatch, temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com/arcgis/rest/services", export_directory=temp_export_dir)

    monkeypatch.setattr(scraper, "_request", lambda url, params: {"name": "TestLayer"})

    result = scraper.fetch_metadata("Layer/FeatureServer/0/query")
    assert result["name"] == "TestLayer"


def test_fetch_layer(monkeypatch, temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com/", export_directory=temp_export_dir, page_size=2)

    features_batches = [
        {"features": [{"id": 1}, {"id": 2}]},
        {"features": [{"id": 3}]},
        {"features": []},
    ]

    call_count = {"i": 0}

    def fake_request(url, params):
        resp = features_batches[call_count["i"]]
        call_count["i"] += 1
        return resp

    monkeypatch.setattr(scraper, "_request", fake_request)

    result = scraper._fetch_layer("Layer/FeatureServer/0/query")
    assert len(result) == 3


def test_export_csv(temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com/", export_directory=temp_export_dir, export_format="csv")
    features = [{"attributes": {"id": 1}}, {"attributes": {"id": 2}}]

    scraper._export(features, "test_layer")
    filepath = os.path.join(temp_export_dir, "test_layer.csv")
    assert os.path.exists(filepath)
    df = pd.read_csv(filepath)
    assert len(df) == 2


def test_scrape_layer(monkeypatch, temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com/", export_directory=temp_export_dir)

    monkeypatch.setattr(scraper, "_fetch_layer", lambda *a, **k: [{"id": 1}])
    monkeypatch.setattr(scraper, "_export", lambda features, fn: setattr(scraper, "_export_called", True))

    scraper.scrape_layer("Layer/FeatureServer/0/query")
    assert hasattr(scraper, "_export_called")


def test_scrape_layers(monkeypatch, temp_export_dir):
    scraper = ArcGISScraper(base_url="http://example.com/", export_directory=temp_export_dir)

    monkeypatch.setattr(scraper, "scrape_layer", lambda q: setattr(scraper, "called", True))

    scraper.scrape_layers(["Layer/FeatureServer/0/query"])
    assert hasattr(scraper, "called")
