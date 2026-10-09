"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# World Bank region codes in the source file.
EXPECTED_REGION_CODES = {"EAS", "ECS", "LCN", "MEA", "NAC", "SAS", "SSF"}

# Poverty lines in the source file, and their labels in the output.
EXPECTED_POVERTY_LINES = {"300", "420", "830"}

# Historical estimates plus projections.
EXPECTED_YEARS = set(range(1981, 2051))


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("poverty_projections")

    # Read table from meadow dataset.
    tb = ds_meadow.read("poverty_projections")

    sanity_check_inputs(tb)

    #
    # Process data.
    #

    tb = calculate_regional_and_global_aggregates(tb=tb)
    tb = calculate_share_in_poverty_and_rename(tb=tb)

    # Make the povertyline a string with two decimal places.
    tb["povertyline"] = tb["povertyline"].apply(lambda x: f"{x:.2f}")

    # Rename poverty lines
    tb["povertyline"] = tb["povertyline"].replace({"3.00": "300", "4.20": "420", "8.30": "830"})

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year", "povertyline"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def calculate_regional_and_global_aggregates(tb):
    """
    Calculate regional and global aggregates for poverty projections.

    The data is shown by country, but it also include the regional column `region_code`.
    """

    tb = tb.copy()

    # Calculate the sum of pop and poorpop for each region_code, year, and povertyline.
    tb = (
        tb.groupby(["region_code", "year", "povertyline"], as_index=False)
        .agg(
            {
                "pop": "sum",
                "poorpop": "sum",
            }
        )
        .reset_index(drop=True)
    )

    # Calculate the global aggregates by summing across all regions.
    tb_global = (
        tb.groupby(["year", "povertyline"], as_index=False)
        .agg(
            {
                "pop": "sum",
                "poorpop": "sum",
            }
        )
        .assign(region_code="WLD")
    )

    # Concatenate the regional and global aggregates.
    tb = pr.concat([tb, tb_global], ignore_index=True)

    # Make these columns not in millions, but in absolute numbers.
    tb["pop"] *= 1_000_000
    tb["poorpop"] *= 1_000_000

    return tb


def calculate_share_in_poverty_and_rename(tb):
    """
    Calculate the share of the population living in poverty and rename columns.
    """
    tb = tb.copy()

    # Calculate the share of the population living in poverty.
    tb["headcount_ratio"] = tb["poorpop"] / tb["pop"] * 100

    # Rename columns for clarity.
    tb = tb.rename(
        columns={
            "region_code": "country",
            "poorpop": "headcount",
        }
    )

    # Keep relevant columns.
    tb = tb[["country", "year", "povertyline", "headcount_ratio", "headcount"]]

    return tb


def sanity_check_inputs(tb: Table) -> None:
    """Assert the source file's schema and value ranges before aggregating."""
    region_codes = set(tb["region_code"].astype(str))
    assert region_codes == EXPECTED_REGION_CODES, f"Unexpected region codes: {region_codes ^ EXPECTED_REGION_CODES}"
    poverty_lines = {f"{x:.2f}" for x in tb["povertyline"].unique()}
    assert poverty_lines == {"3.00", "4.20", "8.30"}, f"Unexpected poverty lines: {poverty_lines}"
    years = set(tb["year"].astype(int))
    assert years == EXPECTED_YEARS, f"Years missing or unexpected: {sorted(years ^ EXPECTED_YEARS)}"
    assert not tb[["pop", "poorpop"]].isna().any().any(), "NaN in pop or poorpop."
    assert (tb[["pop", "poorpop"]] >= 0).all().all(), "Negative pop or poorpop."
    assert (tb["poorpop"] <= tb["pop"]).all(), "More poor people than population in some country-years."


def sanity_check_outputs(tb: Table) -> None:
    """Assert the aggregates are complete, bounded and internally consistent."""
    assert set(tb["povertyline"]) == EXPECTED_POVERTY_LINES, f"Unexpected poverty lines: {set(tb['povertyline'])}"
    assert tb["country"].nunique() == len(EXPECTED_REGION_CODES) + 1, (
        f"Expected 7 regions + World: {sorted(tb['country'].unique())}"
    )
    assert "World" in set(tb["country"]), "World aggregate missing."
    series_years = tb.groupby(["country", "povertyline"], observed=True)["year"].nunique()
    assert series_years.eq(len(EXPECTED_YEARS)).all(), "Incomplete series."
    assert tb["headcount_ratio"].between(0, 100).all(), "Share in poverty outside [0, 100]."
    # A higher poverty line can never count fewer people as poor.
    rates = tb.pivot_table(index=["country", "year"], columns="povertyline", values="headcount_ratio", observed=True)
    assert ((rates["300"] <= rates["420"] + 1e-9) & (rates["420"] <= rates["830"] + 1e-9)).all(), (
        "Share in poverty falls as the poverty line rises."
    )
    # World is the sum of the regions by construction; a gap means a region went missing.
    world = tb[tb["country"] == "World"].set_index(["year", "povertyline"])["headcount"]
    regions = tb[tb["country"] != "World"].groupby(["year", "povertyline"], observed=True)["headcount"].sum()
    assert ((world - regions).abs() <= 1e3).all(), "World headcount differs from the sum of the regions."
    # Source counts are in millions; a lost conversion would leave World in the thousands.
    assert world.between(1e8, 6e9).all(), f"World headcount out of range: {world.min():.3g}–{world.max():.3g}"
