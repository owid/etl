"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ceds_air_pollutants")

    # Read tables from meadow dataset.
    # NOTE: We keep the optimal types (which includes categoricals) for better performance, given the tables sizes.
    tb_detailed = ds_meadow.read("ceds_air_pollutants__detailed", safe_types=False)
    tb_bunkers = ds_meadow.read("ceds_air_pollutants__bunkers", safe_types=False)

    #
    # Process data.
    #
    # The "detailed" file contains emissions for each pollutant, country, sector, fuel, and year (a column for each year).
    # There is an additional column for units, but they are always the same for each pollutant.
    def sanity_check_inputs(tb_detailed: Table, tb_bunkers: Table) -> None:
        # TODO: Assert the exact name of expected columns in each table.
        error = "Each pollutant was expected to have just one unit."
        assert (
            tb_detailed.groupby("em", as_index=False, observed=True).agg({"units": "nunique"})["units"] == 1
        ).all(), error
        assert (
            tb_bunkers.groupby("em", as_index=False, observed=True).agg({"units": "nunique"})["units"] == 1
        ).all(), error
        error = "Detailed table was expected to have all countries in the bunkers table. This has changed (not important, simply check it and redefine this assertion)."
        assert set(tb_detailed["country"]) - set(tb_bunkers["iso"]) == set(), error
        error = "Bunkers table was expected to have all countries in the detailed table, except Palestine. This has changed (not important, simply check it redefine this assertion)."
        assert set(tb_bunkers["iso"]) - set(tb_detailed["country"]) == set(["pse"]), error

    # Sanity checks inputs.
    sanity_check_inputs(tb_detailed=tb_detailed, tb_bunkers=tb_bunkers)

    # For now, we do not need to keep fuel information.
    # Drop the fuel column and sum over all other dimensions.
    tb_detailed = (
        tb_detailed.drop(columns=["fuel"])
        .groupby(["em", "country", "sector", "units"], as_index=False, observed=True)
        .sum()
    )

    # We don't need the detailed sectorial information.
    # Instead, we want to map these detailed sectors into broader sector categories, e.g. "Energy", "Agriculture".
    # This mapping can be found on page 12 of their version comparison document:
    # https://github.com/JGCRI/CEDS/blob/master/documentation/Version_comparison_figures_v_2024_07_08_vs_v_2021_04_20.pdf

    # TODO: Create a mapping for sectors and another for pollutants.
    # TODO: Maybe after the remapping we don't need to keep categoricals.

    # Restructure detailed table to have year as a column.
    tb_detailed = tb_detailed.rename(
        columns={column: int(column[1:]) for column in tb_detailed.columns if column.startswith("x")}
    )
    tb_detailed = tb_detailed.melt(id_vars=["em", "country", "sector", "units"], var_name="year", value_name="value")

    # Restructure bunkers table to have year as a column.
    tb_bunkers = tb_bunkers.rename(columns={"iso": "country"}).rename(
        columns={column: int(column[1:]) for column in tb_bunkers.columns if column.startswith("x")}
    )
    tb_bunkers = tb_bunkers.melt(id_vars=["em", "country", "sector", "units"], var_name="year", value_name="value")

    # The bunkers table contains a "global" country. But note that, according to the README inside the bunkers zip folder,
    # * The "global" emissions in the detailed table contain bunker emission (international shipping, domestic aviation, and international aviation).
    # * The "global" emissions in the bunkers table (already contained in the detailed "global" emissions) are the difference between total shipping fuel consumption (as estimated by the International Maritime Organization and other sources) and fuel consumption as reported by IEA. This additional fuel cannot be allocated to specific iso's. This correction to total fuel consumption is modest in recent years, but becomes much larger in earlier years.
    # So, we can draw the following conclusions:
    # 1. We don't need to add bunker emissions to the detailed "global" emissions.
    # 2. We can rename the bunkers "global" emissions as "Other", given that these emissions are not allocated to any country. If this causes too much confusion, we can consider deleting them.
    tb_bunkers["country"] = tb_bunkers["country"].cat.rename_categories(lambda x: "Other" if x == "global" else x)

    # NOTE: Both the bunkers and the detailed tables contain international shipping, domestic aviation, and international aviation. However, the detailed table contains international aviation and shipping only for "global" (whereas the bunkers table contains them at the country level).
    # TODO: Assert that the details table contains international aviation and shipping only for "global".
    # TODO: Assert that domestic emissions in the details and bunkers table coincide (except for "global" and "pse").
    # TODO: If they coincide, remove domestic aviations from the bunkers table.
    tb_bunkers = tb_bunkers[tb_bunkers["sector"] != "1A3aii_Domestic-aviation"].reset_index(drop=True)

    # Combine tables.
    tb = pr.concat([tb_detailed, tb_bunkers], short_name=paths.short_name)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # TODO: Define a dictionary of pollutant: units, and remove the units column.
    tb = tb.drop(columns=["units"], errors="raise")

    # Improve table format.
    tb = tb.format(["em", "country", "sector", "year"])

    tb["value"].m

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
