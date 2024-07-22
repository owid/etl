"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("world_mineral_statistics")
    tb = ds_meadow.read_table("world_mineral_statistics")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot table to have a column for each category.
    tb = tb.pivot(
        index=["country", "year", "commodity", "sub_commodity"],
        columns="category",
        values="value",
        join_column_levels_with="_",
    )

    # Improve the name of the commodities.
    tb["commodity"] = tb["commodity"].str.capitalize()

    # TODO: There are many issues to be handled:
    #  * Ensure that all sub-commodities have a "Total".
    #  * But it seems that these "Total" may not be reliable. For example, for commodity "Coal", the sub-commodity "Total" only has zeros. And there is another sub-commodity "Coal" that may be the actual total.
    #  * There are many overlapping historical regions.

    # Add regions and income groups to the table.
    REGIONS = {**geo.REGIONS, **{"World": {}}}
    tb = geo.add_regions_to_table(
        tb=tb,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        index_columns=["country", "year", "commodity", "sub_commodity"],
    )

    # Format table conveniently.
    tb = tb.format(["country", "year", "commodity", "sub_commodity"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
