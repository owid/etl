"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from data, and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "barn": "share_of_hens_in_barns",
    "brown__pct": "share_of_brown_hens",
    "cage": "share_of_hens_in_cages",
    "cage_free": "number_of_hens_cage_free",
    "cages": "number_of_hens_in_cages",
    "commercial_egg_farms": "number_of_commercial_egg_farms",
    "free_range": "share_of_hens_free_range_not_organic",
    "organic": "share_of_hens_free_range_organic",
    "total_layers": "number_of_laying_hens",
    "unknown": "share_of_hens_in_unknown_housing",
    # The following columns will be used to extract sources and urls and add them to the source metadata description.
    # Afterwards, they will be removed.
    "available_at": "available_at",
    "source": "source",
    # "click_placements" : "click_placements",
    # "number_of_records": "number_of_records",
}


def fix_typos_in_original_metadata(tb: Table) -> Table:
    tb_fixed = tb.copy()

    # Ensure that certain columns are strings, to avoid issues when modifying a categorical column.
    tb_fixed = tb_fixed.astype({"source": str, "available_at": str})

    # The url for Uruguay is placed in "source" instead of "available_at".
    uruguay_mask = tb_fixed["country"] == "Uruguay"
    expected_text = " Available at: "
    assert (
        expected_text in tb_fixed[uruguay_mask]["source"].item()
    ), f"Expected '{expected_text}' in source for Uruguay."
    uruguay_url = tb_fixed[uruguay_mask]["source"].item().split(expected_text)[1]
    tb_fixed.loc[uruguay_mask, "available_at"] = uruguay_url
    tb_fixed.loc[uruguay_mask, "source"] = tb_fixed.loc[uruguay_mask, "source"].item().split(expected_text)[0]

    # Fix some typos in the metadata, and ensure sources end in a period.
    tb_fixed["source"] = (
        tb_fixed["source"]
        .str.replace("husbndry", "husbandry")
        .replace("(Ministry of Agriculture). '", "(Ministry of Agriculture)'")
        .replace("Regulation (EC) 617/2008)", "Regulation (EC) 617/2008")
        .replace(
            "IEC Annual Review 2019.",
            "International Egg Commission (IEC) Annual Review 2019 (data no longer public and only available to IEC members).",
        )
    )
    tb_fixed["source"] = [source if source.endswith(".") else source + "." for source in tb_fixed["source"]]

    # Copy metadata from the original table to the new one.
    tb_fixed = tb_fixed.copy_metadata(from_table=tb)

    return tb_fixed


def add_individual_sources_to_metadata(tb: Table) -> Table:
    tb = tb.copy()
    # Check that each country has only one source.
    assert (tb.groupby("country").agg({"source": "nunique"})["source"] == 1).all(), "Expected one source per country."

    # Gather the data source for each country.
    original_sources = (
        "- "
        + tb["country"].astype(str)
        + ": ["
        + tb["source"].astype(str)
        + "]("
        + tb["available_at"].astype(str)
        + ")"
    )
    # Check that each variable has only one origin.
    assert all(
        [len(tb[column].metadata.origins) == 1 for column in tb.columns if column not in ["country", "year"]]
    ), "Expected only one origin. Something has changed."
    # Take the origin from any of those variables.
    origin = tb[tb.columns[-1]].metadata.origins[0]
    # Add the full list of original sources to the variable origin.
    origin.citation_full = origin.citation_full + "\n" + "\n".join(original_sources)
    # Replace the origin of each variable with the new one that has the full list of original sources.
    for column in tb.columns:
        tb[column].metadata.origins = [origin]

    return tb


def clean_values(tb: Table) -> Table:
    tb = tb.copy()
    # Remove the spurious "%" symbols from some of the values in some columns.
    for column in tb.columns:
        if tb[column].astype(str).str.contains("%").any():
            tb[column] = tb[column].str.replace("%", "").astype(float)

    return tb


def run_sanity_checks_on_outputs(tb: Table) -> None:
    assert all([tb[column].min() >= 0 for column in tb.columns]), "All numbers should be >0"
    assert all([tb[column].max() <= 100 for column in tb.columns if "share" in column]), "Percentages should be <100"
    # Check that the percentages of the different laying hens housings add up to 100%.
    # Note: The share of brown hens is not related to all other shares about housing systems.
    assert (
        tb[
            [
                "share_of_hens_free_range_not_organic",
                "share_of_hens_free_range_organic",
                "share_of_hens_in_barns",
                "share_of_hens_in_cages",
                "share_of_hens_in_unknown_housing",
            ]
        ].sum(axis=1)
        < 101
    ).all()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow: Dataset = paths.load_dependency("global_hen_inventory")
    tb = ds_meadow["global_hen_inventory"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Fix some typos in the columns related to metadata.
    tb = fix_typos_in_original_metadata(tb=tb)

    # The sources and URLs of the data for each country are given as separate columns.
    # Gather them and add them to the source description of each variable.
    tb = add_individual_sources_to_metadata(tb=tb)

    # Drop unnecessary columns.
    tb = tb.drop(columns=["available_at", "source"])

    # Clean data (remove spurious "%" in the data).
    tb = clean_values(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Run sanity checks on outputs.
    run_sanity_checks_on_outputs(tb=tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
