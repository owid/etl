from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class SemanticSearchRequest(BaseModel):
    """JSON schema for semantic search request."""

    query: str
    limit: int = 10

    class Config:
        extra = "forbid"


class SemanticSearchResult(BaseModel):
    """JSON schema for individual semantic search result."""

    title: str
    indicator_id: int
    snippet: str
    score: float
    metadata: Dict[str, Any]
    # Additional fields for wizard app
    catalog_path: Optional[str] = None
    n_charts: int = 0
    description: Optional[str] = None

    class Config:
        extra = "forbid"
        json_schema_extra = {
            "example": {
                "title": "GDP per capita, PPP (constant 2017 international $)",
                "indicator_id": 123456,
                "snippet": "GDP per capita based on purchasing power parity (PPP). PPP GDP is gross domestic product converted to international dollars using purchasing power parity rates. An international dollar has the same purchasing power over GDP as the U.S. dollar has in the United States.",
                "score": 0.985,
                "metadata": {
                    "chart_count": 12,
                    "catalog_path": "grapher/worldbank_wdi/2023-05-29/wdi/ny_gdp_pcap_pp_kd",
                    "parquet_url": "https://catalog.ourworldindata.org/grapher/worldbank_wdi/2023-05-29/wdi.parquet",
                    "run_sql_template": "SELECT country, year, ny_gdp_pcap_pp_kd FROM 'https://catalog.ourworldindata.org/grapher/worldbank_wdi/2023-05-29/wdi.parquet' WHERE country = 'France' LIMIT 100",
                    "column": "ny_gdp_pcap_pp_kd",
                    "unit": "constant 2017 international $"
                },
                "catalog_path": "grapher/worldbank_wdi/2023-05-29/wdi/ny_gdp_pcap_pp_kd",
                "n_charts": 12,
                "description": "GDP per capita based on purchasing power parity (PPP). PPP GDP is gross domestic product converted to international dollars using purchasing power parity rates. An international dollar has the same purchasing power over GDP as the U.S. dollar has in the United States.",
            }
        }


class SemanticSearchResponse(BaseModel):
    """JSON schema for semantic search response."""

    results: List[SemanticSearchResult]
    query: str
    total_results: int

    class Config:
        extra = "forbid"
        json_schema_extra = {
            "example": {
                "results": [
                    {
                        "title": "GDP per capita, PPP (constant 2017 international $)",
                        "indicator_id": 123456,
                        "snippet": "GDP per capita based on purchasing power parity (PPP). PPP GDP is gross domestic product converted to international dollars using purchasing power parity rates. An international dollar has the same purchasing power over GDP as the U.S. dollar has in the United States.",
                        "score": 0.985,
                        "metadata": {
                            "chart_count": 12,
                            "catalog_path": "grapher/worldbank_wdi/2023-05-29/wdi/ny_gdp_pcap_pp_kd",
                            "parquet_url": "https://catalog.ourworldindata.org/grapher/worldbank_wdi/2023-05-29/wdi.parquet",
                            "run_sql_template": "SELECT country, year, ny_gdp_pcap_pp_kd FROM 'https://catalog.ourworldindata.org/grapher/worldbank_wdi/2023-05-29/wdi.parquet' WHERE country = 'France' LIMIT 100",
                            "column": "ny_gdp_pcap_pp_kd",
                            "unit": "constant 2017 international $"
                        },
                        "catalog_path": "grapher/worldbank_wdi/2023-05-29/wdi/ny_gdp_pcap_pp_kd",
                        "n_charts": 12,
                        "description": "GDP per capita based on purchasing power parity (PPP). PPP GDP is gross domestic product converted to international dollars using purchasing power parity rates. An international dollar has the same purchasing power over GDP as the U.S. dollar has in the United States.",
                    }
                ],
                "query": "gdp",
                "total_results": 1,
            }
        }
