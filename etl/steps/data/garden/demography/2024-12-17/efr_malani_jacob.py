"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Origin
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_UN = ["country", "year", "age", "sex", "probability_of_survival"]
COLUMNS_HMD = ["country", "year", "age", "sex", "probability_of_death"]
# Years
YEAR_UN_START = 1950
YEAR_UN_END = 2023
# Ages
AGE_LAB_START = 15
AGE_REPR_END = 49
AGE_LAB_END = 65

# Additional origin metadata of the paper
origin = Origin(
    producer="Malani and Jacob",
    title="A New Measure of Surviving Children that Sheds Light on Long-term Trends in Fertility",
    citation_full="Malani, A., & Jacob, A. (2024). A New Measure of Surviving Children that Sheds Light on Long-term Trends in Fertility. https://doi.org/10.3386/w33175",
    date_published="2024-11-01",  # type: ignore
    description="""
The world has experienced a dramatic decline in total fertility rate (TFR) since the Industrial Revolution. Yet the consequences of this decline flow not merely from a reduction in births, but from a reduction in the number of surviving children. Authors propose a new measure of the number of surviving children per female, which authors call the effective fertility rate (EFR). EFR can be approximated as the product of TFR and the probability of survival. Moreover, TFR changes can be decomposed into changes that preserve EFR and those that change EFR. Authors specialized EFR to measure the number of daughters that survive to reproduce (reproductive EFR) and the number children that survive to become workers (labor EFR).

Authors use three data sets to shed light on EFR over time across locations. First, authors use data from 165 countries between 1950-2019 to show that one-third of the global decline in TFR during this period did not change labor EFR, suggesting that a substantial portion of fertility decline merely compensated for higher survival rates. Focusing on the change in labor EFR, at least 40% of variation cannot be explained by economic factors such as income, prices, education levels, structural transformation, an urbanization, leaving room for explanations like cultural change. Second, using historical demographic data on European countries since 1750, authors find that there was dramatic fluctuation in labor EFR in Europe around each of the World Wars, a phenomenon that is distinct from the demographic transition. However, prior to that fluctuation, EFRs were remarkably constant, even as European countries were undergoing demographic transitions. Indeed, even when EFRs fell below 2 after 1975, we find that EFRs remained stable rather than continuing to decline. Third, data from the US since 1800 reveal that, despite great differences in mortality rates, Black and White populations have remarkably similar numbers of surviving children over time.


""",
    url_main="https://www.nber.org/papers/w33175",
)
# Extrapolate data for UK nations from UK & Ireland
UK_NATION_EXTRAPOLATION = {
    "England and Wales": "United Kingdom",
    "Scotland": "United Kingdom",
    "Northern Ireland": "Ireland",
}
# Countries that are in HMD but not in UN
COUNTRIES_NOT_IN_UN = ["West Germany", "East Germany", "Taiwan"]
YEARS_EFR_DISTR = [1950, 2023]


def _clean_un_table(tb):
    """Basic cleaning of UN table."""
    # Rename columns
    tb = tb.rename(columns={"location": "country"})

    # Filter 'total' and 'female', select relevant columns
    tb = tb.loc[tb["sex"].isin(["total", "female"]), COLUMNS_UN]

    # Dtypes
    tb["age"] = tb["age"].str.replace("100+", "100").astype("UInt16")

    # Scale
    tb["probability_of_survival"] /= 100

    return tb


def _clean_hmd_table(tb):
    """Basic cleaning of HMD table"""
    # Filter 'total' and 'female', 'period' life tables, relevant columns
    tb = tb.loc[tb["sex"].isin(["total", "female"]) & (tb["type"] == "period"), COLUMNS_HMD]

    # Dtypes
    tb = tb.loc[~tb["age"].str.contains("-")]
    tb["age"] = tb["age"].str.replace("110+", "110").astype("UInt16")

    # Scale
    tb["probability_of_survival"] = 1 - tb["probability_of_death"] / 100
    tb = tb.drop(columns=["probability_of_death"])

    return tb


def combine_un_hmd(tb_un, tb_hmd):
    """Combine UN and HMD tables.

    We use this function to combine survival probabilities and TFR time-series.
    """
    # Keep old years (we use UN for post-1950)
    tb_hmd = tb_hmd.loc[tb_hmd["year"] < YEAR_UN_START]

    # Drop countries not covered by UN
    tb_hmd = tb_hmd.loc[~tb_hmd["country"].isin(COUNTRIES_NOT_IN_UN)]
    ## sanity check
    countries_hmd = set(tb_hmd["country"].unique())
    countries_un = set(tb_un["country"].unique())
    countries_unexpected = {
        c for c in countries_hmd if (c not in countries_un) and (c not in UK_NATION_EXTRAPOLATION.keys())
    }
    assert (
        countries_unexpected == set()
    ), f"There should be no country ({countries_unexpected}) in HMD that is not in UN"

    # UK nation adaptations (extrapolate data from UK & Ireland)
    tb_extra = []
    for nation, country in UK_NATION_EXTRAPOLATION.items():
        tb_extra.append(tb_un.loc[tb_un["country"] == country].assign(country=nation))
    # Combine
    tb = pr.concat([tb_un, tb_hmd, *tb_extra], ignore_index=True)
    # sanity check
    cols = list({"country", "year", "age", "sex"}.intersection(tb.columns))
    _ = tb.format(cols)

    return tb


def get_tfr_estimation(tb_b, tb_p):
    ## Get total births
    tb_b = tb_b.loc[tb_b["sex"] == "total", ["country", "year", "births"]]

    ## Get female population aged 15-49
    ages = {f"{a}-{a+4}" for a in range(15, 50, 5)}
    tb_p = tb_p.loc[(tb_p["sex"] == "female") & tb_p.age.isin(ages)]
    ## sanity check
    x = tb_p.groupby(["country", "year"]).agg({"age": ("unique", "nunique")})
    x.columns = ["set", "nun"]
    assert x.nun.unique() == 7, "There should be 7 unique age groups for each country-year"
    ## Aggregate and get population for women 15-49 years old
    tb_p = tb_p.groupby(["country", "year"], as_index=False)["population"].sum()

    ## Merge
    tb_appr = tb_b.merge(tb_p, on=["country", "year"], validate="1:1")

    ## Approximate TFR = 35 * births / population(females in reproductive age)
    tb_appr["fertility_rate"] = 35 * tb_appr["births"] / tb_appr["population"]

    ## Drop unnecessary columns
    tb_appr = tb_appr.drop(columns=["births", "population"])

    return tb_appr


def run(dest_dir: str) -> None:
    # Load meadow dataset.
    ds_un_lt = paths.load_dataset("un_wpp_lt")
    ds_un_wpp = paths.load_dataset("un_wpp")
    ds_hmd = paths.load_dataset("hmd")

    #
    # 1/ Estimate cumulative survival probabilities
    #

    # Load tables
    tb_un = ds_un_lt.read("un_wpp_lt", reset_metadata="keep_origins")
    tb_un_proj = ds_un_lt.read("un_wpp_lt_proj", reset_metadata="keep_origins")
    tb_hmd = ds_hmd.read("life_tables", reset_metadata="keep_origins")

    # Prepare UN table
    tb_un = _clean_un_table(tb_un)
    tb_un_proj = _clean_un_table(tb_un_proj)
    tb_un = pr.concat([tb_un, tb_un_proj], ignore_index=True)
    # Prepare HMD data
    tb_hmd = _clean_hmd_table(tb_hmd)
    # Combine HMD and UN data (survival probabilities)
    tb = combine_un_hmd(tb_un, tb_hmd)

    # Estimate cumulative survival probabilities
    tb = estimate_cum_survival(tb=tb)

    #
    # 2/ Estimate EFR
    #

    # 2.1 TFR approximation from HFD
    ## Get total births
    tb_b = ds_hmd.read("births", reset_metadata="keep_origins")
    ## Get population
    tb_p = ds_hmd.read("population", reset_metadata="keep_origins")
    ## Combine
    tb_tfr_apr = get_tfr_estimation(tb_b, tb_p)

    # 2.2 Load TFR from UN
    tb_tfr = ds_un_wpp.read("fertility_rate", reset_metadata="keep_origins")
    tb_tfr = tb_tfr.loc[
        (tb_tfr["sex"] == "all") & (tb_tfr["age"] == "all") & (tb_tfr["variant"].isin(["estimates", "medium"])),
        ["country", "year", "fertility_rate"],
    ]

    # 2.3 TFR: Combine HMD and UN data
    tb_tfr = combine_un_hmd(tb_tfr, tb_tfr_apr)

    # 2.4 Get EFR distribution (for each age)
    tb = estimate_efr(tb, tb_tfr)

    #
    # 3/ Create output tables

    # 3.1 Distribution indicators (EFR(age) in YEARS_EFR_DISTR)
    tb_efr = tb.loc[tb["year"].isin(YEARS_EFR_DISTR) & (tb["sex"] == "total"), ["country", "year", "age", "efr"]]
    tb_efr = tb_efr.rename(columns={"year": "birth_year"})

    # 3.2 Obtain labor and reproductive EFRs
    ## Keep only years of interest (15-65), further reduction of 50% rows (aggregate -50%)
    tb_agg = tb.loc[(tb["age"] >= AGE_LAB_START) & (tb["age"] <= AGE_LAB_END)]
    tb_agg = aggregate_efr(tb=tb_agg)

    # 3.3 Format
    tb_agg = tb_agg.format(["country", "year"], short_name="aggregated")
    tb_efr = tb_efr.format(["country", "age", "birth_year"], short_name="distribution")

    # 3.4 Add extra origin
    for col in tb_agg.columns:
        tb_agg[col].metadata.origins = [origin] + tb_agg[col].metadata.origins
    for col in tb_efr.columns:
        tb_efr[col].metadata.origins = [origin] + tb_efr[col].metadata.origins

    # 3.5 Build list of tables
    tables = [
        tb,
        tb_agg,
    ]

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def estimate_cum_survival(tb):
    # Cumulative product
    # We estimate the cumulative survival probability. This is the probability to survive from birth to a given age.
    # The source provides the probability to survive from one age to the next (pn = probability to survive age n to n+1).
    # To estimate this for people born in 1950, we need the data of p0 in 1950, p1 in 1951, etc. That's why we create year_born.
    # After that, we just do the cumulative product for each year_born.
    # Note that for the cumulative product to make sense, we need to first sort table by age!
    # Step 0: Save min year. i.e. year to start recording
    tb["year_min"] = tb.groupby("country")["year"].transform("min")
    # Step 1: Replace year with "cohort year"
    tb["year"] = tb["year"] - tb["age"]
    # Step 2: We only estimate the cumulative survival probability for people born between year_min* and 2023 (reduction of 50% rows)
    # year_min is the first year for which the source reported data (e.g. 1950 for most UN-only countries, varies for HMD countries)
    tb = tb.loc[(tb["year"] >= tb["year_min"]) & (tb["year"] <= YEAR_UN_END)]
    assert (
        tb[tb["year"] == tb["year_min"]].groupby("country").age.min().max() == 0
    ), "There should be age zero for starting year of each country!"
    # Step 3: Sort by age, so we can do the cumulative product later
    tb = tb.sort_values(["country", "sex", "year", "age"], ignore_index=True)
    # Step 4: Estimate cumulative survival probability
    tb["cumulative_survival"] = tb.groupby(["country", "sex", "year"])["probability_of_survival"].cumprod()
    # Step 6: Drop unnecessary columns
    tb = tb.drop(columns=["year_min"])
    return tb


def estimate_efr(tb, tb_tfr):
    # Add TFR
    tb = tb.merge(tb_tfr, on=["country", "year"], validate="m:1")

    # Estimate EFR
    tb["efr"] = tb["fertility_rate"] * tb["cumulative_survival"]

    return tb


def aggregate_efr(tb):
    """Estimate labor and reproductive EFRs."""
    # Estimate metrics
    ## EFR-labor: Average number of daughters that make it to the reproductive age (15-49)
    ## EFR-reproductive: Average number of kids that make it to the labour age (15-65)
    ## Cum survival prob, labor: Probability of a girl to survive to the reproductive age (15-49)
    ## Cum survival prob, reproductive: Probability of a kid to survive to the labor age (15-65)
    tb = tb.loc[(tb["age"] <= AGE_REPR_END) | (tb["sex"] == "total")]
    tb = tb.groupby(["country", "year", "sex"], as_index=False)[["efr", "cumulative_survival"]].mean()

    # Pivot
    tb = tb.pivot(index=["country", "year"], columns=["sex"], values=["efr", "cumulative_survival"]).reset_index()

    # Rename columns
    def rename_col(colname):
        mapping = {
            "female": "repr",
            "total": "labor",
        }

        if colname[1] == "":
            return colname[0]
        else:
            return f"{colname[0]}_{mapping.get(colname[1])}"

    tb.columns = [rename_col(col) for col in tb.columns]

    # Check inf values
    x = tb[tb["efr_repr"].isin([np.inf, -np.inf])]
    assert len(x) == 4
    x = tb[tb["efr_labor"].isin([np.inf, -np.inf])]
    assert len(x) == 4

    # Replace inf with NA
    tb[["efr_repr", "efr_labor"]] = tb[["efr_repr", "efr_labor"]].replace([np.inf, -np.inf], np.nan)
    return tb
