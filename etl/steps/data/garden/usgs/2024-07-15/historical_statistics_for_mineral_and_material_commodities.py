"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Reference year for inflation adjustment.
REFERENCE_YEAR = 2022


def correct_unit_value_for_inflation(tb_unit_value: Table, tb_cpi: Table) -> Table:
    tb_unit_value = tb_unit_value.copy()
    # Correct for inflation, using a reference year (the latest year in the data).
    assert tb_unit_value["year"].max() == REFERENCE_YEAR
    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    tb_cpi = tb_cpi[["year", "all_items"]].reset_index(drop=True)

    reference_value = tb_cpi.loc[tb_cpi["year"] == REFERENCE_YEAR, "all_items"].values[0]
    # Adjust value so that it is 1 at the reference year.
    tb_cpi["all_items"] = tb_cpi["all_items"] / reference_value
    # The US consumer prices dataset has data only since 1913.
    # We need to correct for inflation since 1900.
    # TODO: For now, fill backwards, but check if there is better strategy.
    tb_unit_value = tb_unit_value.merge(tb_cpi.rename(columns={"all_items": "cpi"}), on="year", how="left")
    tb_unit_value = tb_unit_value.sort_values(["country", "year"]).reset_index(drop=True)
    tb_unit_value["cpi"] = tb_unit_value["cpi"].bfill()
    # Adjust unit value for inflation.
    tb_unit_value["unit_value"] = tb_unit_value["unit_value_current"] / tb_unit_value["cpi"]
    # Remove column for CPI.
    tb_unit_value = tb_unit_value.drop(columns=["cpi"])

    return tb_unit_value


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("historical_statistics_for_mineral_and_material_commodities")

    # Read table from meadow dataset.
    # NOTE: Since the data has spurious footnotes, like "W", some columns were stored as strings.
    # Later on we will remove these footnotes and store data as floats.
    tb = ds_meadow.read_table("historical_statistics_for_mineral_and_material_commodities")

    # Load US consumer prices dataset (to use CPI to adjust for inflation).
    ds_cpi = paths.load_dataset("us_consumer_prices")
    tb_cpi = ds_cpi.read_table("us_consumer_prices")

    #
    # Process data.
    #
    # Remove duplicated rows that have exactly the same data.
    tb = tb.drop_duplicates(
        subset=["commodity", "year", "production", "world_production", "unit_value_dollar_t"]
    ).reset_index(drop=True)

    ####################################################################################################################
    # Fix duplicated rows with different data.
    assert tb[tb.duplicated(subset=["commodity", "year"])].sort_values(by=["commodity", "year"])[
        ["commodity", "year"]
    ].values.tolist() == [["Cadmium", 2021], ["Nickel", 2019]]
    # It happens for Cadmium 2021. By looking at the latest PDF:
    # https://pubs.usgs.gov/periodicals/mcs2024/mcs2024-cadmium.pdf
    # I see that the latest row should correspond to 2022.
    # Manually fix this.
    tb.loc[(tb["commodity"] == "Cadmium") & (tb["year"] == 2021) & (tb["production"] == "212"), "year"] = 2022
    # But also Nickel 2019 is repeated with different values.
    # By looking at the latest PDF:
    # https://pubs.usgs.gov/periodicals/mcs2024/mcs2024-nickel.pdf
    # For example, imports 2020 coincides with the latest row. So it looks like the latest row should be 2020.
    tb.loc[(tb["commodity"] == "Nickel") & (tb["year"] == 2019) & (tb["world_production"] == 2510000.0), "year"] = 2020
    ####################################################################################################################

    # Select columns for US production.
    # NOTE: There are several other columns for production (e.g. "primary_production", "secondary_production", etc.).
    # For now, we'll only keep "production".
    tb_us_production = tb[["commodity", "year", "production"]].assign(**{"country": "United States"})
    # Remove spurious footnotes like "W" (and "nan", caused by saving the column as string).
    tb_us_production["production"] = map_series(
        tb_us_production["production"],
        mapping={"W": None, "nan": None},
        warn_on_missing_mappings=False,
        warn_on_unused_mappings=True,
    ).astype({"production": float})

    # Select columns for world production.
    # NOTE: There are 4 columns for world production, namely "world_production", "world_mine_production",
    # "world_mine_production__metal_content", and "world_refinery_production".
    # For now, we'll only keep "world_production".
    tb_world_production = (
        tb[["commodity", "year", "world_production"]]
        .rename(columns={"world_production": "production"}, errors="raise")
        .assign(**{"country": "World"})
        .astype({"production": float})
    )

    # Select columns for unit value.
    tb_unit_value = (
        tb[["commodity", "year", "unit_value_dollar_t"]]
        .assign(**{"country": "World"})
        .rename(columns={"unit_value_dollar_t": "unit_value_current"}, errors="raise")
    )
    # Remove spurious footnotes like "W" (and "nan", caused by saving the column as string).
    tb_unit_value["unit_value_current"] = (
        tb_unit_value["unit_value_current"].astype(str).replace("W", None).replace("nan", None).astype(float)
    )

    # Correct unit value for inflation.
    tb_unit_value = correct_unit_value_for_inflation(tb_unit_value=tb_unit_value, tb_cpi=tb_cpi)

    # Combine tables.
    tb_combined = pr.concat([tb_us_production, tb_world_production], ignore_index=True)
    tb_combined = tb_combined.merge(tb_unit_value, on=["commodity", "year", "country"], how="outer")

    # Remove empty rows.
    tb_combined = tb_combined.dropna(subset=["production", "unit_value"], how="all").reset_index(drop=True)

    # Format tables conveniently.
    tb_combined = tb_combined.format(["country", "year", "commodity"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()
