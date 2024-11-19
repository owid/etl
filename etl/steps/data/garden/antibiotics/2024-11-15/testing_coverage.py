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
    ds_meadow = paths.load_dataset("testing_coverage")

    # Read table from meadow dataset.
    tb = ds_meadow["testing_coverage"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = format_specimen(tb)
    tb = tb.drop(columns=["min", "q1", "median", "q3", "max"])
    tb = tb.pivot(
        index=["country", "year"],
        columns="specimen",
        values=[
            "ctas_with_reported_bcis",
            "ctas_with_reported_bcis_with_ast__gt__80_bcis",
            "total_bcis",
            "total_bcis_with_ast",
        ],
        join_column_levels_with="_",
    )
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


def format_specimen(tb: Table) -> Table:
    """
    Format the syndrome column.
    """
    specimen_dict = {"BLOOD": "bloodstream", "STOOL": "stool", "URINE": "urine", "UROGENITAL": "gonorrhea"}
    tb["specimen"] = tb["specimen"].astype(str)
    tb["specimen"] = tb["specimen"].replace(specimen_dict)
    assert tb["specimen"].isin(specimen_dict.values()).all()

    return tb
