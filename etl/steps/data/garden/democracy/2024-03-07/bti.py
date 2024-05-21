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
    ds_meadow = paths.load_dataset("bti")

    # Read table from meadow dataset.
    tb = ds_meadow["bti"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Sanity checks
    tb = check_pol_sys(tb)
    tb = check_regime(tb)
    tb = tb.drop(
        columns=[
            "pol_sys",
        ]
    )

    # Format
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


def check_pol_sys(tb: Table) -> Table:
    """Sanity-check the indicator.

    Some years looked off in the previous edition, this is a sanity check.
    """
    col_tmp = "pol_sys_check"

    tb.loc[
        (tb["electfreefair_bti"] >= 6)
        & (tb["electfreefair_bti"].notna())
        & (tb["effective_power_bti"] >= 4)
        & (tb["effective_power_bti"].notna())
        & (tb["freeassoc_bti"] >= 4)
        & (tb["freeassoc_bti"].notna())
        & (tb["freeexpr_bti"] >= 4)
        & (tb["freeexpr_bti"].notna())
        & (tb["sep_power_bti"] >= 4)
        & (tb["sep_power_bti"].notna())
        & (tb["civ_rights_bti"] >= 4)
        & (tb["civ_rights_bti"].notna())
        & (tb["state_basic_bti"] >= 3)
        & (tb["state_basic_bti"].notna()),
        col_tmp,
    ] = 1

    # Replace pol_sys_check = 0 if any condition is not met
    tb.loc[
        (tb["electfreefair_bti"] < 6)
        | (tb["effective_power_bti"] < 4)
        | (tb["freeassoc_bti"] < 4)
        | (tb["freeexpr_bti"] < 4)
        | (tb["sep_power_bti"] < 4)
        | (tb["civ_rights_bti"] < 4)
        | (tb["state_basic_bti"] < 3),
        col_tmp,
    ] = 0

    # print(tb[["pol_sys", "pol_sys_check"]].dropna().value_counts())

    assert (tb["pol_sys"] == tb[col_tmp]).all(), "Miss-labelled `pol_sys`."

    tb = tb.drop(columns=[col_tmp])

    return tb


def check_regime(tb: Table) -> Table:
    col_tmp = "regime_bti_check"
    tb.loc[(tb["pol_sys"] == 0) & (tb["democracy_bti"] >= 1) & (tb["democracy_bti"] < 4), col_tmp] = 5
    tb.loc[(tb["pol_sys"] == 0) & (tb["democracy_bti"] >= 4) & (tb["democracy_bti"] <= 10), col_tmp] = 4
    tb.loc[(tb["pol_sys"] == 1) & (tb["democracy_bti"] >= 1) & (tb["democracy_bti"] < 6), col_tmp] = 3
    tb.loc[(tb["pol_sys"] == 1) & (tb["democracy_bti"] >= 6) & (tb["democracy_bti"] < 8), col_tmp] = 2
    tb.loc[(tb["pol_sys"] == 1) & (tb["democracy_bti"] >= 8) & (tb["democracy_bti"] <= 10), col_tmp] = 1

    tb[col_tmp] = tb[col_tmp].astype("UInt8")

    assert (tb["regime_bti"] == tb[col_tmp]).all(), "Miss-labelled `regime_bti`."

    tb = tb.drop(columns=[col_tmp])
    return tb
