"""Bespoke viz step that generates JSON files for the IHME GBD treemap visualization.

This step combines the GBD treemap datasets and generates:
* `metadata.json`, the feed's provenance, derived from the garden columns (see `etl.viz.bespoke`)
* A metadata JSON file with categories, dimensions, and time ranges
* Individual data JSON files per entity

The files are written to the step's output folder; the framework syncs that folder to the R2 path
of the environment being built, so the feed is served at
`<root>/v1/bespoke/ihme_gbd/latest/gbd_treemap_json/causes-of-death.metadata.json` and
`.../causes-of-death.<entityId>.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.
"""

import json

from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def create_metadata_json(tb_filtered: Table, source: str) -> tuple[dict, dict]:
    """Create the metadata JSON structure."""
    # Get unique values for mappings
    countries = sorted(tb_filtered["country"].unique())
    causes = sorted(tb_filtered["cause"].unique())
    categories = sorted(tb_filtered["broad_cause"].unique())

    # Create age group mapping with proper display names
    age_group_map = {
        "All ages": {"id": 1, "name": "All ages"},
        "<5 years": {"id": 2, "name": "Children under 5"},
        "5-14 years": {"id": 3, "name": "Children aged 5 to 14"},
        "15-49 years": {"id": 4, "name": "Adults aged 15 to 49"},
        "50-69 years": {"id": 5, "name": "Adults aged 50 to 69"},
        "70+ years": {"id": 6, "name": "Adults aged 70+"},
    }

    # Create entities list
    entities = [{"id": i + 1, "name": country} for i, country in enumerate(countries)]

    # Create age groups list
    age_groups = [{"id": v["id"], "name": v["name"]} for v in sorted(age_group_map.values(), key=lambda x: x["id"])]

    # Create sex mapping
    sex_map = {
        "Both": {"id": 1, "name": "Both sexes"},
        "Female": {"id": 2, "name": "Female"},
        "Male": {"id": 3, "name": "Male"},
    }

    # Create sex list
    sex_list = [{"id": v["id"], "name": v["name"]} for v in sorted(sex_map.values(), key=lambda x: x["id"])]

    # Create categories list
    categories_list = [{"id": i + 1, "name": cat} for i, cat in enumerate(categories)]
    category_name_to_id = {cat: i + 1 for i, cat in enumerate(categories)}

    # Optional descriptions for specific causes
    # General descriptions (apply to all age groups unless age-specific override exists)
    cause_descriptions = {
        "Heart diseases": "Heart attacks, strokes, and other cardiovascular diseases",
        "Chronic respiratory diseases": "COPD, Asthma, and others",
        "Neurological diseases": "Alzheimer's disease, Parkinson's disease, epilepsy, and others",
        "Digestive diseases": "Cirrhosis and others",
        "Respiratory infections": "Pneumonia, influenza, COVID-19 and others",
        "Neonatal deaths": "Babies who died in the first 28 days of life",
        "Other infectious diseases": "Meningitis, measles, whooping cough and others",
        "Other non-communicable diseases": "Congenital birth defects, urinary tract infections, endocrine disorders and others",
        "Maternal disorders": "Maternal hemorrhage, hypertensive disorders of pregnancy and others",
        "Other injuries": "Drowning, fire, foreign bodies and others",
        "Diarrheal diseases": "Rotavirus, cholera, and other diarrheal diseases",
    }

    # Age-specific descriptions that override general descriptions
    # Format: (cause, age_group) -> description
    age_specific_descriptions = {
        ("Other infectious diseases", "<5 years"): "iNTS infections, typhoid and others ",
        (
            "Other non-communicable diseases",
            "<5 years",
        ): "Cardiovascular diseases, digestive diseases, genetic blood disorders, and others",
        ("Other injuries", "<5 years"): "Animal contact, forces of naturue and others",
        # Add more age-specific descriptions as needed
    }

    # Create variables list with age group associations
    variables = []
    for i, cause in enumerate(causes):
        # Find which age groups this cause appears in
        age_groups_for_cause = tb_filtered[tb_filtered["cause"] == cause]["age"].unique()
        age_group_ids = [age_group_map[age]["id"] for age in age_groups_for_cause if age in age_group_map]

        # Get category
        broad_cause = tb_filtered[tb_filtered["cause"] == cause]["broad_cause"].iloc[0]
        category_id = category_name_to_id[broad_cause]

        # Build variable entry
        variable_entry = {
            "id": i + 1,
            "name": cause,
            "ageGroup": sorted(age_group_ids),
        }

        # Determine description based on age groups
        # If this cause appears in only one age group and has an age-specific description, use it
        # Otherwise, use the general description if it exists
        description = None
        if len(age_groups_for_cause) == 1:
            # Check for age-specific description
            age_key = (cause, age_groups_for_cause[0])
            if age_key in age_specific_descriptions:
                description = age_specific_descriptions[age_key]

        # Fall back to general description if no age-specific one found
        if description is None and cause in cause_descriptions:
            description = cause_descriptions[cause]

        # Only add description if one was found
        if description:
            variable_entry["description"] = description

        variable_entry["category"] = category_id

        variables.append(variable_entry)

    # Create metadata JSON
    metadata = {
        "timeRange": {"start": int(tb_filtered["year"].min()), "end": int(tb_filtered["year"].max())},
        "source": source,
        "categories": categories_list,
        "dimensions": {"entities": entities, "ageGroups": age_groups, "sexes": sex_list, "variables": variables},
    }

    return metadata, {
        "countries": countries,
        "causes": causes,
        "age_group_map": age_group_map,
        "sex_map": sex_map,
    }


def create_entity_data_json(tb_filtered: Table, country: str, mappings: dict) -> dict:
    """Create data JSON for a specific entity."""
    cause_to_id = {cause: i + 1 for i, cause in enumerate(mappings["causes"])}
    age_to_id = {age: mappings["age_group_map"][age]["id"] for age in mappings["age_group_map"].keys()}
    sex_to_id = {sex: mappings["sex_map"][sex]["id"] for sex in mappings["sex_map"].keys()}

    # Filter data for this entity
    tb_entity = tb_filtered[tb_filtered["country"] == country].copy()

    # Sort data for consistent output
    tb_sorted = tb_entity.sort_values(["year", "sex", "age", "cause"]).reset_index(drop=True)

    # Create data JSON for this entity
    data = {
        "values": tb_sorted["value"].tolist(),
        "variables": [cause_to_id[c] for c in tb_sorted["cause"]],
        "years": tb_sorted["year"].tolist(),
        "ageGroups": [age_to_id[a] for a in tb_sorted["age"]],
        "sexes": [sex_to_id[s] for s in tb_sorted["sex"]],
    }

    return data


def save_json(data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder.

    Args:
        data: Dictionary to save as JSON
        filename: Name of the file (e.g., "causes-of-death.1.json")
    """
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, indent=2)


def run() -> None:
    #
    # Load data.
    #
    log.info("Loading GBD treemap datasets.")
    ds_garden = paths.load_dataset("gbd_treemap")
    ds_garden_child = paths.load_dataset("gbd_child_treemap")

    tb = ds_garden["gbd_treemap"].reset_index()
    tb = tb[tb["age"] != "<5 years"]
    tb_child = ds_garden_child["gbd_child_treemap"].reset_index()
    tb = pr.concat([tb, tb_child], ignore_index=True)

    # Filter data for Number metric only (keep all sexes)
    tb_filtered = tb.loc[tb["metric"] == "Number"].copy()
    # test
    tb_filtered.set_index(["country", "year", "age", "sex", "broad_cause", "cause", "metric"], verify_integrity=True)

    #
    # Generate JSON files.
    #
    log.info("Creating metadata and data JSON files.")

    # The feed's provenance, from the origins of the data it is built on, rather than a source
    # string typed in here that goes stale the next time GBD is updated.
    feed_metadata = build_feed_metadata(
        title="Causes of death",
        columns={"Deaths": tb_filtered["value"]},
        update_period_days=ds_garden.metadata.update_period_days,
    )
    write_feed_metadata(paths.output_dir, feed_metadata)

    # Create metadata
    metadata, mappings = create_metadata_json(tb_filtered, source=feed_metadata["feed"]["citation"])

    #
    # Write the JSON files. The framework syncs the output folder to R2 afterwards.
    #
    log.info(f"Creating {len(mappings['countries']) + 1} JSON files.")

    save_json(metadata, "causes-of-death.metadata.json")

    country_to_id = {country: i + 1 for i, country in enumerate(mappings["countries"])}
    for country in tqdm(mappings["countries"], desc="Processing entities"):
        entity_id = country_to_id[country]
        data = create_entity_data_json(tb_filtered, country, mappings)
        save_json(data, f"causes-of-death.{entity_id}.json")

    log.info(
        f"""Successfully created {len(mappings["countries"]) + 1} files:
        - 1 metadata file
        - {len(mappings["countries"])} entity data files
        - {len(metadata["dimensions"]["variables"])} variables
        - {len(metadata["dimensions"]["ageGroups"])} age groups
        - {len(metadata["dimensions"]["sexes"])} sexes"""
    )
