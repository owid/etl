"""
This code combines the data of three different sources of GDP and GDP per capita:
    - World Bank (WDI), in 2021 PPPs (coverage from 1990 to the most recent year available)
    - Maddison Project Database, in 2011 PPPs (coverage from 1820 to the most recent year available)
    - Maddison Database, in 1990 PPPs (coverage from 1 CE to 2008)

The goal is to have a single dataset with GDP and GDP per capita estimations in the very long run (from 1 CE to the most current data).

The units of the variables are different in each source, so the data is processed by applying the growth of the Maddison Project Database between 1820 and 1990 retroactively to the World Bank data, and the growth of the Maddison Database between 1 to 1820 retroactively to the data already adjusted in the previous step.

The Maddison Database is a different project from Maddison Project Database: the former was produced by Angus Maddison, while the latter is the continuation of his work after his death. Only the Maddison Database includes world estimates from 1 CE to 1820.

"""

import owid.catalog.core.processing as pr
from owid.catalog.core import Table, warnings

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define constants: variables to process and references years where merge is done.
VAR_LIST = ["gdp", "gdp_per_capita"]
YEAR_WDI_MPD = 1990
YEAR_MPD_MD = 1820

# Define accuracy of data (in tens)
# 6 means that the data is accurate to the 10^6
ACCURACY_GDP = 6
ACCURACY_GDP_PER_CAPITA = 2


def run() -> None:
    #
    # Load inputs.
    #
    # Load dataset and tables

    # World Bank WDI
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi.read("wdi")

    # Maddison Project Database
    ds_mpd = paths.load_dataset("maddison_project_database")
    tb_mpd = ds_mpd.read("maddison_project_database")

    # Maddison Database
    ds_md = paths.load_dataset("maddison_database")
    tb_md = ds_md.read("maddison_database")

    sanity_check_inputs(tb_wdi, tb_mpd, tb_md)

    # The set of years the splice must preserve, computed from the inputs the same way the
    # processing selects them: Maddison Database up to 1820, Maddison Project Database up to 1990,
    # WDI (non-null World rows) after 1990.
    wdi_world = tb_wdi[tb_wdi["country"] == "World"].dropna(subset=["ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"])
    expected_years = (
        set(tb_md.loc[tb_md["year"] <= YEAR_MPD_MD, "year"])
        | set(tb_mpd.loc[(tb_mpd["country"] == "World") & (tb_mpd["year"] <= YEAR_WDI_MPD), "year"])
        | set(wdi_world.loc[wdi_world["year"] > YEAR_WDI_MPD, "year"])
    )

    #
    # Process data.
    tb = process_and_combine_datasets(tb_wdi, tb_mpd, tb_md)

    tb = tb.format(short_name=paths.short_name)

    sanity_check_outputs(tb, expected_years)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()


def process_and_combine_datasets(tb_wdi: Table, tb_mpd: Table, tb_md: Table) -> Table:
    """
    Process WDI, Maddison Project Database and Maddison Database to create a single dataset with GDP and GDP per capita estimations in the very long run.
    """

    # Sort by year to apply growth with the correct order
    tb_wdi = tb_wdi.sort_values(by=["year"]).reset_index(drop=True)
    tb_mpd = tb_mpd.sort_values(by=["year"]).reset_index(drop=True)
    tb_md = tb_md.sort_values(by=["year"]).reset_index(drop=True)

    #############################
    # FOR WDI
    # Select GDP and GDP pc in international-$ in 2017 prices
    tb_wdi = tb_wdi[["country", "year", "ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"]]
    tb_wdi = tb_wdi.rename(columns={"ny_gdp_mktp_pp_kd": "gdp", "ny_gdp_pcap_pp_kd": "gdp_per_capita"})

    # Filter "World" entity
    tb_wdi = tb_wdi[tb_wdi["country"] == "World"]

    # Drop empty World GDP estimations
    tb_wdi = tb_wdi.dropna().reset_index(drop=True)

    #############################
    # FOR MADDISON PROJECT DATABASE
    # Select only "World" entity
    tb_mpd = tb_mpd[tb_mpd["country"] == "World"].reset_index(drop=True)

    # Drop population, as it's not needed
    tb_mpd = tb_mpd.drop(columns=["population"])

    # Filter years until YEAR_WDI_MPD
    tb_mpd = tb_mpd[tb_mpd["year"] <= YEAR_WDI_MPD].reset_index(drop=True)

    #############################
    # FOR MADDISON DATABASE
    # Keep data until YEAR_MPD_MD
    tb_md = tb_md[tb_md["year"] <= YEAR_MPD_MD].reset_index(drop=True)

    # Drop population, as it's not needed
    tb_md = tb_md.drop(columns=["population"])

    #############################

    # Merge both MPD and WDI world estimations in different columns and add suffixes. This will be useful for the next step.
    tb = tb_mpd.merge(tb_wdi, on="year", how="left", suffixes=("_mpd", "_wdi"), short_name="gdp_historical")

    # Apply Maddison Project Database growth retroactively to YEAR_WDI_MPD WDI data
    tb = create_estimations_from_growth(
        tb=tb, var_list=VAR_LIST, reference_year=YEAR_WDI_MPD, reference_var_suffix="_mpd", to_adjust_var_suffix="_wdi"
    )

    # Concatenate this with original WDI data (data after YEAR_WDI_MPD)
    tb = pr.concat([tb, tb_wdi[tb_wdi["year"] > YEAR_WDI_MPD]], ignore_index=True)

    # Merge datasets to include Maddison Database, which will be used as reference for the next step
    tb = tb.merge(tb_md, on="year", how="outer", suffixes=("", "_md"), sort=True)

    # Apply Maddison Database growth retroactively to YEAR_MPD_MD estimations
    tb = create_estimations_from_growth(
        tb=tb, var_list=VAR_LIST, reference_year=YEAR_MPD_MD, reference_var_suffix="_md", to_adjust_var_suffix=""
    )

    # Round variables to address uncertainty on old estimations (previous to 1990)
    tb["gdp"] = tb["gdp"].round(-ACCURACY_GDP).where(tb["year"] < YEAR_WDI_MPD, tb["gdp"])
    tb["gdp_per_capita"] = (
        tb["gdp_per_capita"].round(-ACCURACY_GDP_PER_CAPITA).where(tb["year"] < YEAR_WDI_MPD, tb["gdp_per_capita"])
    )

    return tb


def create_estimations_from_growth(
    tb: Table, var_list: list, reference_year: int, reference_var_suffix: str, to_adjust_var_suffix: str
) -> Table:
    """
    Adjust estimations of variables according to the growth of a reference variable.

    Parameters
    ----------
    tb : Table
        Table that contains both the reference variable (the one the growth is extracted from) and the variable to be adjusted (the one the growth is applied to).
    var_list : list
        List of the variable types to be adjusted. In this project, ["gdp", "gdp_per_capita"]
    reference_year : int
        Reference year from which the growth will be applied retroactively.
    reference_var_suffix : str
        Suffix of the reference variable (the one the growth is extracted from). In this project, "_mpd" or "_md".
    to_adjust_var_suffix : str
        Suffix of the variable to be adjusted (the one the growth is applied to). In this project, "_wdi" or "".

    Returns
    -------
    tb : Table
        Table with the adjusted variables.
    """
    with warnings.ignore_warnings([warnings.DifferentValuesWarning]):
        for var in var_list:
            # Get value from the reference variable in the reference year
            reference_value = tb.loc[tb["year"] == reference_year, f"{var}{reference_var_suffix}"].iloc[0]

            # The scalar is the previous value divided by the reference variable. This is the growth that will be applied retroactively to the variable to be adjusted.
            tb[f"{var}_scalar"] = tb[f"{var}{reference_var_suffix}"] / reference_value

            # Get value to be adjusted in the reference year
            to_adjust_value = tb.loc[tb["year"] == reference_year, f"{var}{to_adjust_var_suffix}"].iloc[0]

            # The estimated values are the division between the reference value and the scalars. This is the variable to be adjusted effectively adjusted by the growth of the reference variable.
            tb[f"{var}_estimated"] = to_adjust_value * tb[f"{var}_scalar"]

            # Rename the estimated variables without the suffix
            tb[f"{var}"] = tb[f"{var}{to_adjust_var_suffix}"].astype("Float64").fillna(tb[f"{var}_estimated"])

    # Specify "World" entity for each row
    tb["country"] = "World"

    # Keep only new variables
    tb = tb[["country", "year"] + var_list]

    return tb


def sanity_check_inputs(tb_wdi: Table, tb_mpd: Table, tb_md: Table) -> None:
    """
    Assert the assumptions the splice makes about its inputs: the required series exist, the World
    entity is present, and each pair of sources overlaps (with positive values) at its reference year.
    """
    # WDI: required series and a usable World row at the WDI/MPD reference year.
    for col in ["ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"]:
        assert col in tb_wdi.columns, f"WDI is missing the required series {col}."
    wdi_ref = tb_wdi[(tb_wdi["country"] == "World") & (tb_wdi["year"] == YEAR_WDI_MPD)]
    assert len(wdi_ref) == 1, f"WDI must have exactly one World row for {YEAR_WDI_MPD}."
    assert (wdi_ref[["ny_gdp_mktp_pp_kd", "ny_gdp_pcap_pp_kd"]] > 0).all().all(), (
        f"WDI World GDP and GDP per capita must be present and positive in {YEAR_WDI_MPD} — "
        "without them the MPD growth cannot be spliced onto WDI."
    )

    # MPD: World rows at both reference years, with positive values.
    for year in [YEAR_WDI_MPD, YEAR_MPD_MD]:
        mpd_ref = tb_mpd[(tb_mpd["country"] == "World") & (tb_mpd["year"] == year)]
        assert len(mpd_ref) == 1, f"Maddison Project Database must have exactly one World row for {year}."
        assert (mpd_ref[["gdp", "gdp_per_capita"]] > 0).all().all(), (
            f"Maddison Project Database World GDP and GDP per capita must be present and positive in {year}."
        )

    # MD: a row at the MPD/MD reference year, with positive values (the table is World-only).
    md_ref = tb_md[tb_md["year"] == YEAR_MPD_MD]
    assert len(md_ref) == 1, f"Maddison Database must have exactly one row for {YEAR_MPD_MD}."
    assert (md_ref[["gdp", "gdp_per_capita"]] > 0).all().all(), (
        f"Maddison Database GDP and GDP per capita must be present and positive in {YEAR_MPD_MD}."
    )


def sanity_check_outputs(tb: Table, expected_years: set) -> None:
    """
    Assert the spliced output is complete and plausible: no year from any source window was silently
    dropped, no nulls, magnitudes in the right ballpark (a scale change upstream would land far
    outside these windows), and the pre-1990 rounding rule actually applied.
    """
    tb = tb.reset_index()

    # No silent drops: every year each source contributes must be in the output.
    missing_years = expected_years - set(tb["year"])
    assert not missing_years, (
        f"Years present in the inputs are missing from the spliced output: {sorted(missing_years)}"
    )

    # No nulls and only the World entity.
    assert set(tb["country"]) == {"World"}, "Output must contain only the World entity."
    assert tb[["gdp", "gdp_per_capita"]].notna().all().all(), "Output has null GDP or GDP per capita values."

    # Plausible magnitudes in 2021 international-$ — an upstream scale change (e.g. WDI switching
    # units) would land orders of magnitude outside these windows.
    assert tb["gdp"].between(1e11, 1e15).all(), "World GDP outside the plausible window (1e11–1e15 int-$)."
    assert tb["gdp_per_capita"].between(400, 40_000).all(), (
        "World GDP per capita outside the plausible window (400–40,000 int-$)."
    )
    # Implied world population must be plausible for every year (also catches the two series
    # getting out of step with each other at a splice point).
    implied_population = tb["gdp"] / tb["gdp_per_capita"]
    assert implied_population.between(1e8, 1.2e10).all(), (
        "GDP / GDP per capita implies an implausible world population — the two series are out of step."
    )

    # The pre-1990 rounding rule (uncertainty of old estimations) must have applied.
    pre_reference = tb[tb["year"] < YEAR_WDI_MPD]
    assert (pre_reference["gdp"] % 10**ACCURACY_GDP == 0).all(), (
        f"Pre-{YEAR_WDI_MPD} GDP values are not rounded to 10^{ACCURACY_GDP}."
    )
    assert (pre_reference["gdp_per_capita"] % 10**ACCURACY_GDP_PER_CAPITA == 0).all(), (
        f"Pre-{YEAR_WDI_MPD} GDP per capita values are not rounded to 10^{ACCURACY_GDP_PER_CAPITA}."
    )
