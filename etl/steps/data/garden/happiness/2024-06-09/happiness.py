"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = geo.REGIONS


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, previous years and population data.
    ds_meadow = paths.load_dataset("happiness", version="2024-06-09")
    ds_prev_years = paths.load_dataset("happiness", channel="garden", version="2023-03-20")
    ds_population = paths.load_dataset("population", channel="garden")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table datasets.
    tb_this_year = ds_meadow["happiness"].reset_index()
    tb_prev_years = ds_prev_years["happiness"]
    tb_population = ds_population["population"]

    # combine meadow data with previous years
    tb_this_year["cantril_ladder_score"] = tb_this_year["ladder_score"]
    cols_overlap = ["country", "cantril_ladder_score", "year"]
    tb = pr.concat([tb_this_year[cols_overlap], tb_prev_years], ignore_index=True)

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Process data.
    #

    # add population to table
    tb = geo.add_population_to_dataframe(tb, tb_population)

    # add regions to table
    aggregations = {"population": "sum"}
    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggregations,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        year_col="date",
    )

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
