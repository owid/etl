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

    # As mentioned in the meadow step, the download tool creates spurious zeros when downloading data for multiple countries. For example, if you go to:
    # https://apps.fas.usda.gov/psdonline/app/index.html#/app/advQuery
    # And select corn yields for Croatia, it only has data between 1992 and 1998. But if you fetch data from both Croatia and any other country with more years of data informed, e.g. Egypt, the resulting series for Croatia is filled with zeros from 1960 onwards.
    # DEBUG: Uncomment to inspect all countries that have at least one zero.
    # for country in sorted(set(tb["country"])):
    #     if not tb[(tb["country"]==country) & (tb["yield"] == 0)].empty:
    #         px.line(tb[tb["country"]==country], x="year", y="yield", title=country).show()
    # It seems safe to assume that most zeros in the data are spurious, and should therefore be removed.
    tb = tb[(tb["yield"] > 0)].reset_index(drop=True)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
