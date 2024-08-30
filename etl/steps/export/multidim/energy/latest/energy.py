from pathlib import Path

from owid.catalog import utils

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

CURRENT_DIR = Path(__file__).parent


def run(dest_dir: str) -> None:
    # Load YAML file
    config = utils.dynamic_yaml_to_dict(utils.dynamic_yaml_load(CURRENT_DIR / "energy.yml"))

    config["views"] += create_views()

    # Upsert it to MySQL
    multidim.upsert_multidim_data_page("mdd-energy", config, get_engine())


def create_views() -> list[dict]:
    """
    Create views by mapping combinations of sources and metrics to indicator
    names from the grapher step.

    Get a list of all available indicators with
    ```
    SELECT
       SUBSTRING_INDEX(catalogPath, '#', -1) AS extracted_part
    FROM
       variables
    WHERE
       catalogPath LIKE 'grapher/energy/2024-06-20/energy_mix/energy_mix%';
    ```
    """
    sources = {
        # "all": "primary_energy",
        "fossil": "fossil_fuels",
        "coal": "coal",
        "oil": "oil",
        "gas": "gas",
        "low-carbon": "low_carbon_energy",
        "nuclear": "nuclear",
        "renewable": "renewables",
        "hydro": "hydro",
        "solar-wind": "solar_and_wind",
        "solar": "solar",
        "wind": "wind",
    }

    metrics = {
        "total": "twh",
        "per_capita": "per_capita__kwh",
        "share_total": "pct_equivalent_primary_energy",
        "proportional_change": "pct_growth",
        "absolute_change": "twh_growth",
    }

    views = []
    for source in sources:
        for metric in metrics:
            if metric == "per_capita":
                indicator = f"{sources[source]}_per_capita__kwh"
            else:
                indicator = f"{sources[source]}__{metrics[metric]}"

            if source in ("solar", "solar-wind", "wind", "nuclear", "hydro", "low-carbon", "renewable") and metric in (
                "total",
                "per_capita",
                "absolute_change",
            ):
                indicator += "__equivalent"

            views.append(
                {
                    "dimensions": {"source": source, "metric": metric},
                    "indicators": {
                        "y": f"grapher/energy/2024-06-20/energy_mix/energy_mix#{indicator}",
                    },
                }
            )

    return views
