"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_data(tb: Table) -> Table:
    """
    Format years and column names.
    """

    # NOTE: On years
    # The way `year` is formatted – as a string variable often spanning two calendar years – won't work with our schema. We have to map the data to a single (integer) year.
    # For now, arbitrarily, I take the first year in these cases and convert to integer.

    # First year = first 4 characters of the year string
    tb["year"] = tb["year"].str[:4].astype(int)

    # ## Multi-dimesional poverty measures
    #
    # At least initially, we will be primarily concerned with the three measures that relate to overall multi-dimensional poverty:
    # - `Headcount ratio`: the share of population in multidimensional poverty
    # - `Intensity`: a measure of the average depth of poverty (of the poor only – NB, not like the World Bank's poverty gap index)
    # - `MPI`: the product of `Headcount ratio` and `Intensity`.
    #
    # These are multi-dimensional poverty measures – a weighted aggregation across many individual indicators.
    # Here I prepare this data as I would for uploading to OWID grapher and visualize it – including both `hot` and `cme` data in the same file.

    # Modify variable names
    tb = tb.replace({"M0": "mpi", "H": "share", "A": "intensity"})

    # filter for main multi-dimensional pov measures
    tb = tb[tb["measure"].isin(["mpi", "share", "intensity"])].reset_index(drop=True)

    # pivot to wide format
    tb = tb.pivot_table(index=["country", "year"], columns=["flav", "measure", "area_lab"], values="b").reset_index()

    # collapse multi-level index into single column names
    tb.columns = [" ".join(col).strip().replace(" ", "_") for col in tb.columns.values]

    # Format column names, making it all lowercase
    tb.columns = tb.columns.str.lower()  # type: ignore

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("multidimensional_poverty_index"))

    # Read table from meadow dataset.
    tb = ds_meadow["multidimensional_poverty_index"]

    #
    # Process data.
    #

    tb = process_data(tb)

    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Verify index and sort
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
