"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("corn_yields")

    # Read table from meadow dataset.
    tb = ds_meadow.read("corn_yields")

    #
    # Process data.
    #
    # Sanity check.
    assert (tb["unit_description"] == "(MT/HA)").all(), "Unexpected units."
    assert (tb["commodity"] == "Corn").all(), "Unexpected commodity."
    assert (tb["attribute"] == "Yield").all(), "Unexpected attribute."

    # Remove unnecessary columns.
    tb = tb.drop(columns=["unit_description", "commodity", "attribute"])

    # Select relevant columns.
    tb = tb.rename(
        columns={column: column.split("_")[1] for column in tb.columns if column.startswith("_")}, errors="raise"
    )

    # Transpose table.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="yield")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # TODO: It seems that harmonize_names ignore the excluded_countries.json file.
    # TODO: It seems there are spurious zeros in the data (e.g. Malawi, Zambia and Mozambique in 1960 are exactly zero).

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
