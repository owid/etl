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
    ds_meadow = paths.load_dataset("burden_estimates")
    snap = paths.load_snapshot("data_dictionary.csv")

    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["burden_estimates"].reset_index()
    tb = tb.drop(columns=["iso2", "iso3", "iso_numeric", "g_whoregion"])
    #
    # Process data.
    #
    tb = add_variable_description_from_producer(tb, dd)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_variable_description_from_producer(tb, dd):
    """Add variable description from the data dictionary to each variable."""
    columns = tb.columns.difference(["country", "year"])
    for col in columns:
        tb[col].metadata.description_from_producer = dd.loc[dd.variable_name == col, "definition"].values[0]
    return tb
