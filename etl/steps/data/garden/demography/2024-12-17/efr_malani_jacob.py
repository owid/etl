"""Load a meadow dataset and create a garden dataset."""

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

    # Format
    tb_un = tb_un.format(["country", "year"], short_name="un")

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
