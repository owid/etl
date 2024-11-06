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
    ds_meadow = paths.load_dataset("population_explore")
    ds_omm = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["population_explore"].reset_index()
    tb_omm = ds_omm["population"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Format OMM column
    tb_omm = tb_omm[["country", "year", "population"]]
    tb_omm = tb_omm[tb_omm["country"].isin(tb["country"].unique())]
    tb_omm = tb_omm[(tb_omm["year"] >= 1800) & (tb_omm["year"] <= 1951)]
    tb_omm["population"] = tb_omm["population"].astype("Int64")

    # Merge
    tb = tb.merge(tb_omm, on=["country", "year"], suffixes=("_explore", "_omm"), how="outer")
    tb["diff"] = tb["population_explore"] - tb["population_omm"]

    # Format
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        formats=["csv", "feather"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
