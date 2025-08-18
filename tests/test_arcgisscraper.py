import os
import pytest
import tempfile
import shutil
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

