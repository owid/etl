"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("family_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("family_database")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()

    columns_of_interest = [
        "Child poverty rate",
        "Crude divorce rate (divorces per 1000 people)",
        "Crude marriage rate (marriages per 1000 people)",
        "Employment rates (%) for all mothers (15-64 year olds) with at least one child under 15",
        "Employment rates (%) for partnered mothers (15-64 year olds) with at least one child under 15",
        "Employment rates (%) for sole-parent mothers (15-64 year olds) with at least one child under 15",
        "Length of paid maternity, parental and home care leave available to mothers in weeks",
        "Length of paid paternity and parental leave reserved for fathers in weeks",
        "Proportion (%) of children (aged 0-14) that live in households where all adults are in employment (working)",
        "Proportion (%) of children (aged 0-17) living in 'other' types of household",
        "Proportion (%) of children (aged 0-17) living with a single parent",
        "Proportion (%) of children (aged 0-17) living with two parents",
        "Proportion (%) of children aged 0-2 enrolled in formal childcare and pre-school",
        "Share of births outside of marriage (% of all births)",
        "Public social expenditure on services and in-kind benefits for families as a % of GDP",
        "Public social expenditure on cash benefits for families as a % of GDP",
        "Public social expenditure on tax breaks for families as a % of GDP",
        "Total public social expenditure on families as a % of GDP",
    ]

    tb = tb[["country", "year"] + columns_of_interest]
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
