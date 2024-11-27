"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("antimicrobial_usage")

    # Read table from meadow dataset.
    tb_class = ds_meadow["class"].reset_index()
    tb_aware = ds_meadow["aware"].reset_index()
    #
    # Process data.
    #
    tb_class = geo.harmonize_countries(df=tb_class, countries_file=paths.country_mapping_path)
    tb_aware = geo.harmonize_countries(df=tb_aware, countries_file=paths.country_mapping_path)
    # Create tb_notes for use in metadata
    tb_notes = tb_class[["country", "year", "antimicrobialclass", "notes"]].dropna(subset=["notes"])

    # Aggregate by antimicrobial class
    tb_class_agg = aggregate_antimicrobial_classes(tb_class)
    # Save the origins of the aggregated table to insert back in later
    origins = tb_class_agg["did"].metadata.origins
    # Drop columns that are not needed in the garden dataset.
    tb_class = tb_class.drop(
        columns=["whoregioncode", "whoregionname", "countryiso3", "incomeworldbankjune", "atc4", "notes"]
    )
    tb_aware = tb_aware.drop(columns=["whoregioncode", "whoregionname", "incomeworldbankjune", "aware", "notes"])

    tb_class = tb_class.format(["country", "year", "antimicrobialclass", "atc4name", "routeofadministration"])
    tb_aware = tb_aware.format(["country", "year", "awarelabel"])
    tb_class_agg = format_notes(tb_class_agg, tb_notes)
    # Insert back the origins
    tb_class_agg["did"].metadata.origins = origins
    tb_class_agg["ddd"].metadata.origins = origins
    tb_class_agg = tb_class_agg.format(
        ["country", "year", "antimicrobialclass", "description_processing"], short_name="class_aggregated"
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_class, tb_aware, tb_class_agg],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def aggregate_antimicrobial_classes(tb: Table) -> Table:
    """
    Aggregating by antimicrobial class, we want to combine antibacterials and antituberculosis, but also keep antituberculosis separately
    """
    tb = tb.copy(deep=True)
    # Convert the column to strings (if not already done)
    tb["antimicrobialclass"] = tb["antimicrobialclass"].astype(str)

    # Create a completely independent copy of antituberculosis rows and reset its index
    msk = tb["antimicrobialclass"] == "Drugs for the treatment of tuberculosis (ATC J04A)"
    tb_anti_tb = tb[msk].reset_index(drop=True)
    assert len(tb_anti_tb["antimicrobialclass"].unique()) == 1

    # Modify antimicrobialclass in tb
    tb["antimicrobialclass"] = tb["antimicrobialclass"].replace(
        {
            "Drugs for the treatment of tuberculosis (ATC J04A)": "Antibacterials (ATC J01, A07AA, P01AB, ATC J04A)",
            "Antibacterials (ATC J01, A07AA, P01AB)": "Antibacterials (ATC J01, A07AA, P01AB, ATC J04A)",
        },
    )
    assert len(tb["antimicrobialclass"].unique()) == 4
    tb = tb.groupby(["country", "year", "antimicrobialclass"], dropna=False)[["ddd", "did"]].sum().reset_index()
    assert len(tb["antimicrobialclass"].unique()) == 4
    # Add the antituberculosis data back to tb
    tb_anti_tb = (
        tb_anti_tb.groupby(["country", "year", "antimicrobialclass"], dropna=False)[["ddd", "did"]].sum().reset_index()
    )
    tb_combined = pr.concat([tb, tb_anti_tb])

    tb_combined.set_index(["country", "year", "antimicrobialclass"], verify_integrity=True)

    return tb_combined


def format_notes(tb: Table, tb_notes: Table) -> Table:
    """
    Format notes column
    """
    for note in tb_notes["notes"].unique():
        if pd.notna(note):
            msk = tb_notes["notes"] == note
            tb_note = tb_notes[msk]
            countries = tb_note["country"].unique()
            countries_formatted = combine_countries(countries)
            description_processing_string = f"In {countries_formatted}: {note}"
            tb_notes.loc[msk, "description_processing"] = description_processing_string
    # Creating onedescription processing for each antimicrobial class, the variable unit
    tb_desc = (
        tb_notes.dropna(subset=["description_processing"])  # Remove NaNs
        .groupby(["antimicrobialclass"])["description_processing"]
        .apply(lambda x: "; ".join(set(x)))  # Combine unique values
        .reset_index()
    )
    tb = pr.merge(tb, tb_desc, on=["antimicrobialclass"])

    return tb


def combine_countries(countries):
    # Combine countries into a string
    if not countries:
        return ""
    elif len(countries) == 1:
        return countries[0]
    elif len(countries) == 2:
        return " and ".join(countries)
    else:
        return ", ".join(countries[:-1]) + " and " + countries[-1]
