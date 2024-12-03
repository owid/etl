"""Load a meadow dataset and create a garden dataset.


Combines HMD and UN life tables.

Some notes:

    - Time coverage:
        - UN contains data on many more countries, but only since 1950.
        - HMD contains data on fewer countries, but since 1676!
        - We therefore use UN since 1950 for all countries, and HMD prior to that. We use the same source for all countries in each time period to ensure comparability across countries.
    - Age groups:
        - HMD contains single-age groups from 0 to 109 and 110+ (equivalent to >=110). It also contains data on wider age groups, but we discard these.
        - UN contains single-age groups from 0 to 99 and 100+ (equivalent to >=100)
"""

import numpy as np
import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# List of indicator columns
COLUMNS_INDICATORS = [
    "central_death_rate",
    "probability_of_death",
    "probability_of_survival",
    "number_survivors",
    "number_deaths",
    "number_person_years_lived",
    "survivorship_ratio",
    "number_person_years_remaining",
    "life_expectancy",
    "average_survival_length",
]
COLUMN_INDICATORS_REL = [
    "life_expectancy_fm_diff",
    "life_expectancy_fm_ratio",
    "central_death_rate_mf_ratio",
]
COLUMNS_INDEX = [
    "country",
    "year",
    "sex",
    "age",
    "type",
]
COLUMNS_INDEX_REL = [
    "country",
    "year",
    "age",
    "type",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets.
    paths.log.info("load dataset, tables")
    ds_hmd = paths.load_dataset("hmd")
    ds_un = paths.load_dataset("un_wpp_lt")

    # Read table from meadow dataset.
    tb_hmd = ds_hmd.read("life_tables")
    tb_hmd_diff = ds_hmd.read("diff_ratios")
    tb_un = ds_un.read("un_wpp_lt")

    #
    # Process data.
    #
    tb_un = tb_un.rename(
        columns={
            "location": "country",
        }
    )
    # Set type='period' for UN data
    tb_un["type"] = "period"

    # Keep only single-years
    ## Get only single-year, set dtype as int
    flag = ~tb_hmd["age"].str.contains("-")
    tb_hmd = tb_hmd.loc[flag]
    flag = ~tb_hmd_diff["age"].str.contains("-")
    tb_hmd_diff = tb_hmd_diff.loc[flag]

    # Add life expectancy differences and ratios
    paths.log.info("calculating extra variables (ratio and difference in life expectancy for f and m).")
    tb_un_rel = make_table_diffs_ratios(tb_un)

    # Combine HMD + UN
    paths.log.info("concatenate tables")
    tb = combine_tables(tb_hmd, tb_un, COLUMNS_INDEX, COLUMNS_INDICATORS)
    tb_rel = combine_tables(tb_hmd_diff, tb_un_rel, COLUMNS_INDEX_REL, COLUMN_INDICATORS_REL)

    # Set DTypes
    dtypes = {
        "type": "string",
    }
    tb = tb.astype(dtypes)
    tb_rel = tb_rel.astype(dtypes)

    # Set index
    tb = tb.format(COLUMNS_INDEX, short_name=paths.short_name)
    tb_rel = tb_rel.format(COLUMNS_INDEX_REL, short_name="diff_ratios")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_rel], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()


def combine_tables(tb_hmd: Table, tb_un: Table, cols_index, cols_indicators) -> Table:
    """Combine HMD and UN life tables.

    - UN only provides period data.
    - We use UN data after 1950. Prior to that, we use HMD.
    - We considered using HMD over UN after 1950 if data was available for a given country for all years, ages and sexes.
        - However, this is only the case for very few countries: Australia, Germany, Hungary, Lithuania, Northern Ireland, Scotland, United Kingdom.
        - We decided against this to ensure comparability across countries (i.e. all countries use same source after 1950).
    """
    # HMD
    ## Sanity check years
    assert tb_hmd["year"].max() == 2023, "HMD data should end in 2023"
    assert tb_hmd["year"].min() == 1751, "HMD data should start in 1751"
    ## Keep only period HMD data prior to 1950 (UN data starts in 1950)
    tb_hmd = tb_hmd.loc[((tb_hmd["year"] < 1950) & (tb_hmd["type"] == "period")) | (tb_hmd["type"] == "cohort")]
    ## Filter relevant columns (UN has two columns that HMD doesn't: 'probability_of_survival', 'survivorship_ratio')
    columns_indicators_hmd = [col for col in tb_hmd.columns if col in cols_indicators]
    tb_hmd = tb_hmd.loc[:, cols_index + columns_indicators_hmd]

    # UN
    ## Sanity check years
    assert tb_un["year"].max() == 2023, "UN data should end in 2023"
    assert tb_un["year"].min() == 1950, "UN data should start in 1950"
    assert (tb_un["year"].drop_duplicates().diff().dropna() == 1).all(), "UN data should be yearly"
    ## Filter relevant columns
    tb_un = tb_un.loc[:, cols_index + cols_indicators]

    # Combine tables
    tb = pr.concat([tb_hmd, tb_un], short_name=paths.short_name)

    # Remove all-NaN rows
    tb = tb.dropna(subset=cols_indicators, how="all")

    return tb


def make_table_diffs_ratios(tb: Table) -> Table:
    """Create table with metric differences and ratios.

    Currently, we estimate:

    - female - male: Life expectancy
    - male/female: Life Expectancy, Central Death Rate
    """
    # Pivot & obtain differences and ratios
    cols_index = ["country", "year", "age", "type"]
    tb_new = (
        tb.pivot_table(
            index=cols_index,
            columns="sex",
            values=["life_expectancy", "central_death_rate"],
        )
        .assign(
            life_expectancy_fm_diff=lambda df: df[("life_expectancy", "female")] - df[("life_expectancy", "male")],
            life_expectancy_fm_ratio=lambda df: df[("life_expectancy", "female")] / df[("life_expectancy", "male")],
            central_death_rate_mf_ratio=lambda df: df[("central_death_rate", "male")]
            / df[("central_death_rate", "female")],
        )
        .reset_index()
    )

    # Keep relevant columns
    cols = [col for col in tb_new.columns if col[1] == ""]
    tb_new = tb_new.loc[:, cols]

    # Rename columns
    tb_new.columns = [col[0] for col in tb_new.columns]

    # Add metadata back
    for col in tb_new.columns:
        if col not in cols_index:
            tb_new[col].metadata.origins = tb["life_expectancy"].m.origins.copy()
            tb_new[col] = tb_new[col].replace([np.inf, -np.inf], np.nan)

    return tb_new
