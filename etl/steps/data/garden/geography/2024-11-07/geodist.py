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
    ds_meadow = paths.load_dataset("geodist")

    # Read table from meadow dataset.
    tb = ds_meadow["geodist"].reset_index()

    tb = tb.rename(
        columns={
            "iso_o": "country_origin",
            "iso_d": "country_dest",
            "dist": "dist_populous_city",
            "distcap": "dist_capital_city",
            "distw": "dist_weighted_arithmetic",
            "distwces": "dist_weighted_harmonic",
        },
        errors="raise",
    )

    # drop all rows where country_origin or country_dest is identical
    tb = tb[tb["country_origin"] != tb["country_dest"]]
    tb = tb.drop(columns=["smctry"])

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, country_col="country_origin", countries_file=paths.country_mapping_path)
    tb = geo.harmonize_countries(df=tb, country_col="country_dest", countries_file=paths.country_mapping_path)
    tb = tb.format(["country_origin", "country_dest"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
