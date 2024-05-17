"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fh")
    # ds_regions = paths.load_dataset("regions")
    # ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb_ratings = ds_meadow["fh_ratings"].reset_index()
    tb_scores = ds_meadow["fh_scores"].reset_index()

    #
    # Process data.
    #
    tb_ratings = geo.harmonize_countries(
        df=tb_ratings,
        countries_file=paths.country_mapping_path,
    )
    tb_scores = geo.harmonize_countries(
        df=tb_scores,
        countries_file=paths.country_mapping_path,
    )

    # Create indicator for electoral democracy
    tb_scores = add_electdem(tb_scores)

    # Merge
    tb = tb_ratings.merge(tb_scores, on=["country", "year"], how="outer")

    # Table list
    tables = [
        tb.format(["country", "year"], short_name=paths.short_name),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_electdem(tb: Table) -> Table:
    """Add electoral democracy indicator."""
    mask = (tb["electprocess_fh"] >= 7) & (tb["polrights_score_fh"] >= 20) & (tb["civlibs_score_fh"] >= 30)
    tb.loc[mask, "electdem_fh"] = 1
    tb.loc[
        ~mask & (tb[["electprocess_fh", "polrights_score_fh", "civlibs_score_fh"]].notna().all(axis=1)),
        "electdem_fh",
    ] = 0

    return tb
