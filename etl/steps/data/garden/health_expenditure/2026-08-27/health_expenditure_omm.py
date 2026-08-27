"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define category to select from OECD Health Expenditure and Financing Database
CATEGORY_OECD = "Government/compulsory schemes"

# Coverage floor: the OECD series carries 61 reference areas, and the OMM keeps only
# countries present there. A drop means the filter or a merge regressed.
MIN_COUNTRIES = 61

# Upper bound for government/compulsory health spending as a share of GDP. The series
# peaks at 15.8% (United States, 2020), so 30 leaves generous headroom while still
# catching a unit slip or a botched splice.
MAX_SHARE_GDP = 30

# Countries present in both the OECD and the 1993 series but sharing no year with a
# value in both, so the retroactive splice has no anchor and they get no 1960-1991
# backcast. This is a property of the sources, not a bug — but a NEW country joining
# this set means an overlap disappeared, which is worth a look.
SPLICE_ANCHOR_EXCEPTIONS = {"Belgium", "Switzerland"}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_oecd = paths.load_dataset("health_expenditure")
    ds_oecd_1993 = paths.load_dataset("health_expenditure_1993")
    ds_lindert = paths.load_dataset("lindert")

    # Read table from meadow dataset.
    tb_oecd = ds_oecd.read("health_expenditure")
    tb_oecd_1993 = ds_oecd_1993.read("health_expenditure_1993")
    tb_lindert = ds_lindert.read("lindert")

    sanity_check_inputs(tb_oecd, tb_oecd_1993, tb_lindert)

    #
    # Process data.
    #
    # Select the right financing scheme we need from the OECD Health Expenditure and Financing Database
    tb_oecd = tb_oecd[tb_oecd["financing_scheme"] == CATEGORY_OECD].reset_index(drop=True)

    sanity_check_splice_anchors(tb_oecd, tb_oecd_1993)

    # Keep only the necessary columns
    tb_oecd = tb_oecd[["country", "year", "share_gdp"]]

    # Save the countries available in the OECD dataset
    countries_oecd = list(tb_oecd["country"].unique())

    # Merge the three tables
    tb = pr.merge(tb_oecd, tb_oecd_1993, on=["country", "year"], how="outer", suffixes=("", "_oecd_1993"))
    tb = pr.merge(tb, tb_lindert, on=["country", "year"], how="outer", suffixes=("", "_lindert"))

    # Rename share_gdp to share_gdp_oecd
    tb = tb.rename(columns={"share_gdp": "share_gdp_oecd"})

    # Keep only countries available in the OECD dataset
    tb = tb[tb["country"].isin(countries_oecd)].reset_index(drop=True)

    # Merge the three series, by applying the growth retroactively
    tb = create_estimations_from_growth(tb=tb, reference_var_suffix="_oecd_1993", to_adjust_var_suffix="_oecd")

    # Fill data from Lindert where there is no data in share_gdp
    tb["share_gdp"] = tb["share_gdp"].fillna(tb["share_gdp_lindert"])

    # Keep only the necessary columns
    tb = tb[["country", "year", "share_gdp"]]

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"], short_name="health_expenditure_omm")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_oecd.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb_oecd: Table, tb_oecd_1993: Table, tb_lindert: Table) -> None:
    """
    Check the three source tables before splicing them.
    """
    assert CATEGORY_OECD in set(tb_oecd["financing_scheme"]), (
        f"Financing scheme {CATEGORY_OECD!r} is missing from the OECD input. The filter in run() would "
        f"silently yield an empty table. Available: {sorted(set(tb_oecd['financing_scheme']))}"
    )
    for name, tb_input in [("OECD", tb_oecd), ("OECD 1993", tb_oecd_1993), ("Lindert", tb_lindert)]:
        assert not tb_input.empty, f"{name} input table is empty."
        assert {"country", "year", "share_gdp"} <= set(tb_input.columns), (
            f"{name} input is missing required columns. Has: {sorted(tb_input.columns)}"
        )


def sanity_check_splice_anchors(tb_oecd: Table, tb_oecd_1993: Table) -> None:
    """
    Check that countries in both the OECD and 1993 series share a year with a value in both.

    Without such a year there is no reference value, so the retroactive growth yields NaN and the
    country silently loses its 1960-1991 backcast. A shared year is only an anchor when share_gdp is
    non-null on both sides, which is what create_estimations_from_growth requires.
    """
    tb_oecd = tb_oecd[tb_oecd["share_gdp"].notna()]
    tb_oecd_1993 = tb_oecd_1993[tb_oecd_1993["share_gdp"].notna()]
    countries_both = set(tb_oecd["country"]) & set(tb_oecd_1993["country"])
    no_anchor = {
        country
        for country in countries_both
        if not (
            set(tb_oecd.loc[tb_oecd["country"] == country, "year"])
            & set(tb_oecd_1993.loc[tb_oecd_1993["country"] == country, "year"])
        )
    }
    unexpected = no_anchor - SPLICE_ANCHOR_EXCEPTIONS
    assert not unexpected, (
        f"Countries lost their splice anchor: {sorted(unexpected)}. They appear in both the OECD and "
        "1993 series but share no year with a value in both, so they will silently lose the 1960-1991 "
        "backcast. Confirm the overlap really disappeared upstream before adding them to "
        "SPLICE_ANCHOR_EXCEPTIONS."
    )


def sanity_check_outputs(tb: Table) -> None:
    """
    Check the spliced table right before formatting and saving.
    """
    assert tb["country"].nunique() >= MIN_COUNTRIES, (
        f"Country coverage shrank: {tb['country'].nunique()} < {MIN_COUNTRIES}. Possible filter or merge regression."
    )
    assert not tb["share_gdp"].isna().all(), "share_gdp is entirely NaN — the splice produced no values."
    share_gdp = tb["share_gdp"].dropna()
    assert share_gdp.min() >= 0 and share_gdp.max() < MAX_SHARE_GDP, (
        f"share_gdp out of [0, {MAX_SHARE_GDP}): min={share_gdp.min()}, max={share_gdp.max()}"
    )


def create_estimations_from_growth(tb: Table, reference_var_suffix: str, to_adjust_var_suffix: str) -> Table:
    """
    Adjust estimations of variables according to the growth of a reference variable.

    Parameters
    ----------
    tb : Table
        Table that contains both the reference variable (the one the growth is extracted from) and the variable to be adjusted (the one the growth is applied to).
    reference_var_suffix : str
        Suffix of the reference variable (the one the growth is extracted from). In this project, "_oecd_1993".
    to_adjust_var_suffix : str
        Suffix of the variable to be adjusted (the one the growth is applied to). In this project, "_oecd".

    Returns
    -------
    tb : Table
        Table with the adjusted variables.
    """

    # Save the original columns
    columns_list = list(tb.columns)

    # Sort by country and year
    tb = tb.sort_values(by=["country", "year"]).reset_index(drop=True)

    # Define the first year in common between the two series, share_gdp{reference_var_suffix} and share_gdp{to_adjust_var_suffix}
    # First, define all the years in common between the two series
    tb["years_in_common"] = tb.loc[
        tb[f"share_gdp{reference_var_suffix}"].notnull() & tb[f"share_gdp{to_adjust_var_suffix}"].notnull(), "year"
    ]

    # Define the first year in common
    tb["reference_year"] = tb.groupby("country")["years_in_common"].transform("min")

    # Get value from the reference variable in the reference year
    tb["reference_value"] = tb.groupby("country")[f"share_gdp{reference_var_suffix}"].transform(
        lambda x: (
            x.loc[tb["year"] == tb["reference_year"]].iloc[0]
            if not x.loc[tb["year"] == tb["reference_year"]].empty
            else None
        )
    )

    # The scalar is the previous value divided by the reference variable. This is the growth that will be applied retroactively to the variable to be adjusted.
    tb["share_gdp_scalar"] = tb[f"share_gdp{reference_var_suffix}"] / tb["reference_value"]

    # Get value to be adjusted in the reference year
    tb["to_adjust_value"] = tb.groupby("country")[f"share_gdp{to_adjust_var_suffix}"].transform(
        lambda x: (
            x.loc[tb["year"] == tb["reference_year"]].iloc[0]
            if not x.loc[tb["year"] == tb["reference_year"]].empty
            else None
        )
    )

    # The estimated values are the division between the reference value and the scalars. This is the variable to be adjusted effectively adjusted by the growth of the reference variable.
    tb["share_gdp_estimated"] = tb["to_adjust_value"] * tb["share_gdp_scalar"]

    # Rename the estimated variables without the suffix
    tb["share_gdp"] = tb[f"share_gdp{to_adjust_var_suffix}"].astype("Float64").fillna(tb["share_gdp_estimated"])

    # Keep only new variables
    if "share_gdp" not in columns_list:
        columns_list.append("share_gdp")

    tb = tb[columns_list]

    return tb
