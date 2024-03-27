"""Generate a faostat_*.meta.yml file for each faostat garden step, with metadata fetched from grapher.

NOTE: This script should only be used once, to generate the files for the first time.

In future updates we will need to adapt the scripts or etl steps to be able to easily update metadata.

"""
import argparse
import json

from owid import catalog
from structlog import get_logger

from etl import db
from etl.files import yaml_dump
from etl.paths import DATA_DIR, STEP_DIR

# Initialize logger.
log = get_logger()

# Latest faostat version.
VERSION = "2024-03-14"


def main():
    # List all faostat garden steps whose metadata may need to be improved.
    domains = [
        step.stem
        for step in list((STEP_DIR / f"data/garden/faostat/{VERSION}/").glob("faostat_*.py"))
        if step.stem not in ["faostat_metadata", "faostat_food_explorer"]
    ]

    # Initialize a dictionary that will contain, for each step, all variables metadata from grapher.
    metadata_dict = {}

    # Fetch all data from grapher database.
    for domain in domains:
        try:
            dataset_name = catalog.Dataset(DATA_DIR / f"garden/faostat/{VERSION}/{domain}").metadata.title
        except FileNotFoundError:
            log.error(
                f"ETL grapher step for {domain} could not be loaded. "
                f"Run `etl run {domain} --grapher` and try again."
            )
            continue
        try:
            dataset_id = db.get_dataset_id(dataset_name=dataset_name, version=VERSION)
        except AssertionError:
            log.error(
                f"Grapher dataset for {domain} could not be found in the database. "
                f"Run `etl run {domain} --grapher` and try again."
            )
            continue
        variables = db.get_variables_in_dataset(dataset_id=dataset_id, only_used_in_charts=True)
        if len(variables) > 0:
            # variables_in_charts[domain] = variables.set_index("shortName").T.to_dict()
            variables_dict = {}
            for variable in variables.to_dict(orient="records"):
                display = json.loads(variable["display"])
                variables_dict[variable["shortName"]] = {
                    "title": variable["name"],
                    "unit": variable["unit"],
                    "short_unit": variable["shortUnit"],
                    # "description_from_producer": variable["description"],
                    "processing_level": "major",
                    "display": {"name": display["name"], "shortUnit": variable["shortUnit"], "numDecimalPlaces": 2},
                    "presentation": {
                        "title_public": variable["titlePublic"],
                    },
                }
            metadata_dict[domain] = {
                "dataset": {"title": dataset_name, "update_period_days": 365},
                "definitions": {
                    "common": {
                        "processing_level": "major",
                        "presentation": {"topic_tags": ["Agricultural Production"], "attribution_short": "FAO"},
                    }
                },
                "tables": {f"{domain}_flat": {"title": dataset_name, "variables": variables_dict}},
            }

    # For each domain, create a yml metadata file.
    for domain, metadata in metadata_dict.items():
        metadata_file = STEP_DIR / f"data/garden/faostat/{VERSION}/{domain}.meta.yml"
        yaml_text = yaml_dump(metadata)
        metadata_file.write_text(yaml_text)  # type: ignore


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    args = parser.parse_args()
    main()
