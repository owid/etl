"""Load a meadow dataset and create a garden dataset."""

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

    # Tidy notes column
    tb_class = tidy_notes(tb_class)
    # Aggregate by antimicrobial class
    tb_class_agg, tb_notes = aggregate_antimicrobial_classes(tb_class)
    # Save the origins of the aggregated table to insert back in later
    # Drop columns that are not needed in the garden dataset.
    tb_class = tb_class.drop(
        columns=["whoregioncode", "whoregionname", "countryiso3", "incomeworldbankjune", "atc4", "notes"]
    )
    tb_aware = tb_aware.drop(columns=["whoregioncode", "whoregionname", "incomeworldbankjune", "aware", "notes"])

    tb_class = tb_class.format(["country", "year", "antimicrobialclass", "atc4name", "routeofadministration"])
    tb_aware = tb_aware.format(["country", "year", "awarelabel"])
    tb_class_agg = pivot_aggregated_table(tb_class_agg, tb_notes)
    tb_class_agg = tb_class_agg.format(["country", "year"], short_name="class_aggregated")

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


def pivot_aggregated_table(tb_class_agg: Table, tb_notes: Table) -> Table:
    """
    Pivot the aggregated table to have a column for each antimicrobial class, then add the description_processing metadata
    """

    tb_notes_dict = {
        "Antibacterials (ATC J01, A07AA, P01AB)": "antibacterials",
        "Antimalarials (ATC P01B)": "anti_malarials",
        "Antimycotics and antifungals for systemic use (J02, D01B)": "antifungals",
        "Antivirals for systemic use (ATC J05)": "antivirals",
        "Drugs for the treatment of tuberculosis (ATC J04A)": "antituberculosis",
        "Antibacterials (ATC J01, A07AA, P01AB, ATC J04A)": "antibacterials_and_antituberculosis",
    }
    tb_notes["category"] = tb_notes["antimicrobialclass"].map(tb_notes_dict)
    tb_class_agg = tb_class_agg.copy(deep=True)
    tb_class_agg["antimicrobialclass"] = tb_class_agg["antimicrobialclass"].replace(tb_notes_dict)
    tb_class_agg = tb_class_agg.pivot(
        index=["country", "year"], columns="antimicrobialclass", values=["ddd", "did"], join_column_levels_with="_"
    )
    tb_class_agg = tb_class_agg.reset_index(drop=True)

    for key in tb_notes_dict.values():
        if f"ddd_{key}" in tb_class_agg.columns:
            tb_class_agg[f"ddd_{key}"].metadata.description_key = tb_notes["description_processing"][
                tb_notes["category"] == key
            ]
        if f"did_{key}" in tb_class_agg.columns:
            tb_class_agg[f"did_{key}"].metadata.description_key = tb_notes["description_processing"][
                tb_notes["category"] == key
            ]
    return tb_class_agg


def aggregate_antimicrobial_classes(tb: Table) -> Table:
    """
    Aggregating by antimicrobial class, we want to combine antibacterials and antituberculosis, but also keep antituberculosis separately
    """
    tb = tb.copy(deep=True)
    # Convert the column to strings (if not already done)
    tb["antimicrobialclass"] = tb["antimicrobialclass"].astype("string")

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
    expected_class_values = {
        "Antibacterials (ATC J01, A07AA, P01AB, ATC J04A)",
        "Antimalarials (ATC P01B)",
        "Antimycotics and antifungals for systemic use (J02, D01B)",
        "Antivirals for systemic use (ATC J05)",
    }
    actual_values = set(tb["antimicrobialclass"].unique())
    assert actual_values == expected_class_values
    # Format the notes tables before it's removed
    tb_notes = tb[["country", "year", "antimicrobialclass", "notes"]].dropna(subset=["notes"])
    tb_notes = format_notes(tb_notes)

    # Aggregate the data
    tb = tb.groupby(["country", "year", "antimicrobialclass"], dropna=False)[["ddd", "did"]].sum().reset_index()
    assert len(tb["antimicrobialclass"].unique()) == 4
    # Add the antituberculosis data back to tb
    tb_anti_tb = (
        tb_anti_tb.groupby(["country", "year", "antimicrobialclass"], dropna=False)[["ddd", "did"]].sum().reset_index()
    )
    tb_combined = pr.concat([tb, tb_anti_tb])

    tb_combined.format(["country", "year", "antimicrobialclass"])

    return tb_combined, tb_notes


def format_notes(tb_notes: Table) -> Table:
    """
    Format notes column
    """
    for note in tb_notes["notes"].unique():
        msk = tb_notes["notes"] == note
        tb_note = tb_notes[msk]
        countries = tb_note["country"].unique()
        countries_formatted = combine_countries(countries)
        description_processing_string = f"For {countries_formatted}: {note}"
        tb_notes.loc[msk, "description_processing"] = description_processing_string
    # Creating onedescription processing for each antimicrobial class, the variable unit
    tb_desc = (
        tb_notes.dropna(subset=["description_processing"])  # Remove NaNs
        .groupby(["antimicrobialclass"])["description_processing"]
        .apply(lambda x: list(set(x)))  # Combine unique values into a list
        .reset_index()
    )

    return tb_desc


def tidy_notes(tb_class: Table) -> Table:
    """
    Tidy notes column - improve the syntax and fix spelling errors
    """
    notes_dict = {
        "Only consumption in the community reported": "only antimicrobial consumption in the community is reported.",
        "For antimycotics and antifungals: only J02 reported": "for antimycotics and antifungals, only antimycotics for systemic use (ATC code J02) are reported.",
        "For antibiotics: only J01 and P01AB reported": "for antibiotics, only antibiotics for systemic use (ATC code J01) and nitroimidazole derivatives (ATC code P01AB) are reported.",
        "For antibiotics: only J01 reported": "for antibiotics, only antibiotics for systemic use (ATC code J01) are reported",
        "For antifungals: only use in the hospital reported": "for antifungals, only those used in hospitals are reported.",
        "Data incomplete since not collected from all sources of data": "data is incomplete since it's not collected from all sources.",
        "Only consumption in the public sector reported and this is estimated to reppresent less than 90% of the antimicrobial used in the country ": "only consumption in the public sector reported and this is estimated to represent less than 90% of total antimicrobial usage.",
        "Data incomplete: not all antibiotics reported systematically": "data is incomplete, not all antibiotics reported systematically.",
        "For antituberculosis medicines: data are incomplete": "data are incomplete for antituberculosis medicines.",
    }
    tb_class["notes"] = tb_class["notes"].replace(notes_dict)
    return tb_class


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
