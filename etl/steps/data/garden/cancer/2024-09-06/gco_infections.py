"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gco_infections")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["gco_infections"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    aggregates = {"cases": "sum", "attr_cases": "sum"}
    tb = geo.add_regions_to_table(
        tb,
        index_columns=["country", "year", "sex", "agent", "cancer"],
        regions=REGIONS,
        aggregations=aggregates,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    tb["attr_cases_share"] = (tb["attr_cases"] / tb["cases"]) * 100

    tb = tb.format(["country", "year", "sex", "agent", "cancer"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
