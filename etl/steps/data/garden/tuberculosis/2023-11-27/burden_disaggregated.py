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
    ds_meadow = paths.load_dataset("burden_disaggregated")

    # Read table from meadow dataset.
    tb = ds_meadow["burden_disaggregated"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.drop(columns=["measure", "unit"])
    tb = combining_sexes_for_all_age_groups(tb)
    tb = tb.set_index(["country", "year", "age_group", "sex", "risk_factor"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def combining_sexes_for_all_age_groups(tb: Table) -> Table:
    """
    Not all of the age-groups provided by the WHO have a value for both sexes, so we need to combine values for males and females to calculate these.
    """

    tb["age_group"] = tb["age_group"].astype("str")
    age_groups_with_both_sexes = tb[tb["sex"] == "a"]["age_group"].drop_duplicates().to_list()
    msk = tb["age_group"].isin(age_groups_with_both_sexes)
    tb_age = tb[~msk]
    tb_gr = tb_age.groupby(["country", "year", "age_group", "risk_factor"]).sum().reset_index()

    tb = pr.concat([tb, tb_gr], axis=0, ignore_index=True, copy=False)

    return tb
