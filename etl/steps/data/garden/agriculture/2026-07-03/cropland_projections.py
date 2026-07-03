"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Labels of the three scenarios in the original data, and names of the output columns.
SCENARIOS = {
    "Business As Usual": "cropland_bau",
    "Toward Sustainability": "cropland_tss",
    "Stratified Societies": "cropland_sss",
}

# Years informed in the original data (2012 is the base year of the projections).
EXPECTED_YEARS = {2012, 2030, 2035, 2040, 2050}

# World totals of arable land (in million hectares) as published in Table 4.12 of the report (FAO, 2018).
# NOTE: The machine-readable data (used in this step) are raw outputs of the GAPS model, and differ slightly (by up to
# about 3.5%) from the figures printed in the report. We use the published figures only as a sanity check, with a
# tolerance, to ensure that the data is consistent with the report.
PUBLISHED_WORLD_TOTALS_MHA = {
    ("cropland_bau", 2012): 1567,
    ("cropland_tss", 2012): 1567,
    ("cropland_sss", 2012): 1567,
    ("cropland_bau", 2030): 1690,
    ("cropland_tss", 2030): 1594,
    ("cropland_sss", 2030): 1812,
    ("cropland_bau", 2050): 1732,
    ("cropland_tss", 2050): 1653,
    ("cropland_sss", 2050): 1892,
}
PUBLISHED_WORLD_TOTALS_RELATIVE_TOLERANCE = 0.035


def sanity_check_inputs(tb: Table) -> None:
    assert set(tb["scenario"]) == set(SCENARIOS), "Scenarios in the data have changed."
    assert set(tb["year"]) == EXPECTED_YEARS, "Years in the data have changed."
    arable = tb[tb["indicator"] == "Arable land"]
    assert len(arable) > 0, "No arable land data found."
    assert (arable["units"] == "ha").all(), "Arable land is expected to be in hectares."
    assert arable["value"].notnull().all(), "Arable land has missing values."
    assert (arable["value"] >= 0).all(), "Arable land has negative values."
    assert not arable.duplicated(subset=["country", "item", "element", "scenario", "year"]).any(), (
        "Arable land has duplicated rows."
    )


def sanity_check_outputs(tb: Table) -> None:
    # All countries and years should be informed for all scenarios.
    assert tb[list(SCENARIOS.values())].notnull().all().all(), "Unexpected missing values in the output."

    # The base year 2012 should be (almost) identical across scenarios.
    base = tb[tb["year"] == 2012]
    for column in ["cropland_tss", "cropland_sss"]:
        assert ((base[column] - base["cropland_bau"]).abs() / base["cropland_bau"] < 1e-3).all(), (
            f"The 2012 base year of {column} differs from the one of cropland_bau."
        )

    # World totals should be reasonably close to the ones published in Table 4.12 of the report.
    world = tb[tb["country"] == "World"]
    for (column, year), published_mha in PUBLISHED_WORLD_TOTALS_MHA.items():
        extracted_mha = world.loc[world["year"] == year, column].item() / 1e6
        assert abs(extracted_mha - published_mha) / published_mha < PUBLISHED_WORLD_TOTALS_RELATIVE_TOLERANCE, (
            f"World total of {column} in {year} ({extracted_mha:.0f} Mha) deviates from the published figure "
            f"({published_mha} Mha) more than expected."
        )


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cropland_projections")

    # Read table from meadow dataset.
    tb = ds_meadow.read("cropland_projections")

    #
    # Process data.
    #
    # Sanity checks.
    sanity_check_inputs(tb)

    # Select projections of arable land.
    # NOTE: Following FAOSTAT terminology, the source uses "arable land" for the physical area under temporary and
    # permanent crops, which FAOSTAT calls "cropland".
    tb = tb[tb["indicator"] == "Arable land"].reset_index(drop=True)

    # Add up the physical areas of all crops (given per crop and per production system, i.e. rainfed or irrigated) to
    # get the total cropland of each country, scenario and year.
    tb = tb.groupby(["country", "scenario", "year"], observed=True, as_index=False)["value"].sum()

    # Create a world total.
    # NOTE: The world total needs to be calculated before harmonizing countries, since it includes the areas of
    # "Rest of <region>" entities, which gather all countries that are not informed individually (and will be excluded
    # from the output).
    tb_world = tb.groupby(["scenario", "year"], observed=True, as_index=False)["value"].sum()
    tb_world["country"] = "World"

    # Harmonize country names (which, among others, excludes "Rest of <region>" entities).
    tb = paths.regions.harmonize_names(tb=tb)

    # Combine countries and world data.
    tb = pr.concat([tb, tb_world], ignore_index=True)

    # Create a column of total cropland for each scenario.
    tb = tb.pivot(index=["country", "year"], columns="scenario", values="value", join_column_levels_with="_")
    tb = tb.rename(columns=SCENARIOS, errors="raise")

    # Sanity checks.
    sanity_check_outputs(tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
