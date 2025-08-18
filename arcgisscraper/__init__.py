"""
ArcGISScraper

This module provides a ArcGIS REST API scraper.

It allows users to download data from ArcGIS FeatureServer/MapServer layers with support for:
- Pagination and rate limiting (configurable via max_requests_per_second)
- Token-based authentication for secured services
- Automatic retries with exponential backoff on failed requests
- Metadata fetching (fields, geometry type, spatial reference)
- Filtering via SQL where clauses, field selection, and geometry filters
- Exporting data to CSV, JSON, or Parquet

Copyright (C) 2025 Peter Grønbæk Andersen <peter@grnbk.io>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.
"""
