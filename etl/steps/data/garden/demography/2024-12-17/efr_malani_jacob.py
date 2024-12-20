"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Origin
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_UN = ["country", "year", "age", "sex", "probability_of_survival"]
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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_un_lt = paths.load_dataset("un_wpp_lt")
    ds_un_wpp = paths.load_dataset("un_wpp")

    # ds_hmd = paths.load_dataset("hmd")
    # ds_hfd = paths.load_dataset("hfd")

    # Load tables
    tb_un = ds_un_lt.read("un_wpp_lt", reset_metadata="keep_origins")
    tb_un_proj = ds_un_lt.read("un_wpp_lt_proj", reset_metadata="keep_origins")
    tb_tfr = ds_un_wpp.read("fertility_rate", reset_metadata="keep_origins")

    # Estimate cumulative survival in UN LT tables
    tb_un = estimate_un_cum_survival(
        tb=tb_un,
        tb_proj=tb_un_proj,
    )

    # Add EFR
    tb_un = estimate_un_efr(tb_un, tb_tfr)

    # Format
    tb_un = tb_un.format(["country", "year"], short_name="un")

    # Add extra origin
    for col in tb_un.columns:
        tb_un[col].metadata.origins = [origin] + tb_un[col].metadata.origins

    # Build list of tables
    tables = [
        tb_un,
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


def estimate_un_cum_survival(tb, tb_proj):
    # Concatenate
    tb = pr.concat([tb, tb_proj], ignore_index=True)

    # Rename columns
    tb = tb.rename(columns={"location": "country"})

    # Filter 'total' and 'female'
    tb = tb.loc[tb["sex"].isin(["total", "female"]), COLUMNS_UN]

    # Dtypes
    tb["age"] = tb["age"].str.replace("100+", "100").astype("UInt16")

    # Scale
    tb["probability_of_survival"] /= 100

    # Cumulative product
    # We estimate the cumulative survival probability. This is the probability to survive from birth to a given age.
    # The source provides the probability to survive from one age to the next (pn = probability to survive age n to n+1).
    # To estimate this for people born in 1950, we need the data of p0 in 1950, p1 in 1951, etc. That's why we create year_born.
    # After that, we just do the cumulative product for each year_born.
    # Note that for the cumulative product to make sense, we need to first sort table by age!
    # Step 1: Replace year with "cohort year"
    tb["year"] = tb["year"] - tb["age"]
    # Step 2: We only estimate the cumulative survival probability for people born between 1950 and 2023 (reduction of 50% rows)
    tb = tb.loc[(tb["year"] >= YEAR_UN_START) & (tb["year"] <= YEAR_UN_END)]
    # Step 3: Sort by age, so we can do the cumulative product later
    tb = tb.sort_values(["country", "sex", "year", "age"], ignore_index=True)
    # Step 4: Estimate cumulative survival probability
    tb["cumulative_survival"] = tb.groupby(["country", "sex", "year"])["probability_of_survival"].cumprod()
    # Step 5: Keep only years of interest (15-65), further reduction of 65% rows (aggregate -83%)
    tb = tb.loc[(tb["age"] >= AGE_LAB_START) & (tb["age"] <= AGE_LAB_END)]
    # # Step 6: Drop columns
    # tb = tb.drop(columns=["year_born"])

    return tb


def estimate_un_efr(tb_un, tb_tfr):
    # Filter TFR table
    tb_tfr = tb_tfr.loc[
        (tb_tfr["sex"] == "all") & (tb_tfr["age"] == "all") & (tb_tfr["variant"].isin(["estimates", "medium"])),
        ["country", "year", "fertility_rate"],
    ]

    # Add TFR
    tb_un = tb_un.merge(tb_tfr, on=["country", "year"], validate="m:1")

    # Estimate EFR
    tb_un["efr"] = tb_un["fertility_rate"] * tb_un["cumulative_survival"]

    # Estimate metrics
    ## EFR-labor: Average number of daughters that make it to the reproductive age (15-49)
    ## EFR-reproductive: Average number of kids that make it to the labour age (15-65)
    ## Cum survival prob, labor: Probability of a girl to survive to the reproductive age (15-49)
    ## Cum survival prob, reproductive: Probability of a kid to survive to the labor age (15-65)
    tb_un = tb_un.loc[(tb_un["age"] <= AGE_REPR_END) | (tb_un["sex"] == "total")]
    tb_un = tb_un.groupby(["country", "year", "sex"], as_index=False)[["efr", "cumulative_survival"]].mean()

    # Pivot
    tb_un = tb_un.pivot(index=["country", "year"], columns=["sex"], values=["efr", "cumulative_survival"]).reset_index()

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

    tb_un.columns = [rename_col(col) for col in tb_un.columns]

    return tb_un
