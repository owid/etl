"""Load a meadow dataset and create a garden dataset."""

from shared import add_regional_aggregates, add_share_population

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
AGE_GROUPS_RANGES = {
    "All ages": [0, None],
    "<5 years": [0, 4],
    "5-14 years": [5, 14],
    "15-19 years": [15, 19],
    "15-49 years": [15, 49],
    "20-24 years": [20, 24],
    "25-29 years": [25, 29],
    "30-34 years": [30, 34],
    "35-39 years": [35, 39],
    "40-44 years": [40, 44],
    "45-49 years": [45, 49],
    "50-54 years": [50, 54],
    "50-69 years": [50, 69],
    "55-59 years": [55, 59],
    "60-64 years": [60, 64],
    "65-69 years": [65, 69],
    "70+ years": [70, None],
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_mental_health")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["gbd_mental_health"].reset_index()

    tb = add_regional_aggregates(
        tb,
        ds_regions,
        index_cols=["country", "year", "metric", "cause", "age", "sex"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
    )
    # Add a share of the population column
    tb = add_share_population(tb)
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Format the tables
    tb = tb.format(["country", "year", "cause", "metric", "sex", "age"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
