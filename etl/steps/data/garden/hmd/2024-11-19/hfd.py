"""There are various input tables, this step brings them together.


The comments below summarize the various columns in all tables. This comments are meant for maintenance, and not for the end-user. They try to:

    - Group the various tables into categories, depending on the primary index columns that they have.
    - Each group is separated from the next one with '----'.
    - I've tagged each category with a number + letter. E.g. "2y" meaning it has two primary indices, and one is 'year'. 'c' stands for 'cohort.
    - '*y' categories likely contain period indicators. '*c' categories likely contain cohort indicators.
    - Within each group, I've listed the names of the original tables, along with their header (description of its indicators). I've also added the list of columns (indicators that the table presents).
    - At the end I've added a section '=> OUTPUT' which tries to summarize the format of the output & consolidated table.

------------
P 2y: code, year [PERIOD]

    x adjtfrRR
        Tempo-adjusted total fertility rates, Bongaarts-Feeney method
        adjTFR
    x adjtfrRRbo
        Tempo-adjusted total fertility rates by birth order, Bongaarts-Feeney method
        adjTFR   adjTFR1   adjTFR2   adjTFR3   adjTFR4  adjTFR5p

    cbrRR
        Crude birth rate
        CBR
    cbrRRbo
        Crude birth rate by birth order
        CBR     CBR1     CBR2     CBR3     CBR4    CBR5p

    x mabRR
        Period mean ages at birth and period mean ages at birth by age 40
        MAB      MAB40
    x mabRRbo
        Period mean ages at birth by birth order and period mean ages at birth by birth order by age 40
        MAB1     MAB2     MAB3     MAB4    MAB5p    MAB40_1    MAB40_2    MAB40_3    MAB40_4    MAB40_5p

    patfr
        Parity- and age-adjusted total fertility rate
        PATFR   PATFR1   PATFR2   PATFR3   PATFR4  PATFR5p
    patfrc
        Parity- and age-adjusted total fertility rate (based on parity distribution from population census)
        PATFR   PATFR1   PATFR2   PATFR3   PATFR4  PATFR5p

    x pmab
        Period table mean ages at birth by birth order
        TMAB    TMAB1    TMAB2    TMAB3    TMAB4   TMAB5p
    x pmabc
        Period table mean ages at birth by birth order (based on parity distribution from population census)
        TMAB    TMAB1    TMAB2    TMAB3    TMAB4   TMAB5p

    x sdmabRR
        Standard deviation in period mean ages at birth and standard deviation in period mean ages at birth by age 40
        sdMAB    sdMAB40
    x sdmabRRbo
        Standard deviation in period mean ages at birth by birth order and standard deviation in period mean ages at birth by birth order by age 40
        sdMAB      sdMAB1      sdMAB2      sdMAB3      sdMAB4     sdMAB5p     sdMAB40   sdMAB40_1   sdMAB40_2   sdMAB40_3   sdMAB40_4  sdMAB40_5p

    x tfrRR
        Period total fertility rates and period total fertility rates by age 40
        TFR     TFR40
    x tfrRRbo
        Period total fertility rates by birth order and period total fertility rates by birth order by age 40
        TFR      TFR1      TFR2      TFR3      TFR4     TFR5p     TFR40   TFR40_1   TFR40_2   TFR40_3   TFR40_4  TFR40_5p

    x totbirthsRR
        Total live births
        Total
    x totbirthsRRbo
        Total live births by birth order
        Total            B1            B2            B3            B4           B5p

    => OUTPUT:
    columns
        adjTFR
        CBR
        MAB
        MAB40
        sdMAB
        sdMAB40
        TFR
        TFR40
        PATFR
        PATFR_c
        TMAB
        TMAB_c
        B
    dimensions
        code, year, birth_order

------------
P 3y: code, year, age [PERIOD]

    asfrRR
        Period fertility rates by calendar year and age (Lexis squares, age in completed years (ACY))
        ASFR
    asfrRRbo
        Period fertility rates by calendar year, age and birth order (Lexis squares, age in completed years (ACY))
        ASFR       ASFR1       ASFR2       ASFR3       ASFR4      ASFR5p

    ? birthsRR
        Live births by calendar year and age (Lexis squares, age in completed years (ACY))
        Total
    ? birthsRRbo
        Live births by calendar year, age and birth order (Lexis squares, age in completed years (ACY))
        Total          B1          B2          B3          B4         B5p

    cpfrRR
        Cumulative period fertility rates (Lexis squares)
        CPFR
    cpfrRRbo
        Cumulative period fertility rates by birth order (Lexis squares)
        CPFR     CPFR1     CPFR2     CPFR3     CPFR4    CPFR5p

    exposRR
        Female population exposure by calendar year and age (Lexis squares, age in completed years (ACY))
        Exposure
    exposRRpa
        Female exposure to risk by calendar year, age and parity
        E0x            E1x            E2x            E3x           E4px
    exposRRpac
        Female exposure to risk by calendar year, age and parity (based on parity distribution from population census)
        E0x            E1x            E2x            E3x           E4px

    mi
        Conditional fertility rates by calendar year, age and birth order
        m1x       m2x       m3x       m4x      m5px
    mic
        Conditional fertility rates by calendar year, age and birth order (based on parity distribution from population census)
        m1x       m2x       m3x       m4x      m5px

        => OUTPUT:
        columns
            ASFR
            B
            CPFR
            expos
            expos_c
            mi
        dimensions
            code, year, age, parity

------------
4y: code, year, age, cohort  [PERIOD]

    asfrTR
        Period fertility rates by calendar year, age and birth cohort (Lexis triangles)
        ASFR
    asfrTRbo
        Period fertility rates by calendar year, age, birth cohort and birth order (Lexis triangles)
        ASFR       ASFR1       ASFR2       ASFR3       ASFR4      ASFR5p

    ? birthsTR
        Live births by calendar year, age and birth cohort (Lexis triangles)
        Total
    ? birthsTRbo
        Live births by calendar year, age, birth cohort and birth order (Lexis triangles)
        Total             B1             B2             B3             B4            B5p

    exposTR
        Female population exposure by calendar year, age and birth cohort (Lexis triangles)
        Exposure

    => OUTPUT
    columns
        ASFR
        B
        E
    dimensions
        code, year, age, cohort, parity

------------
C 2c: code, cohort  [COHORT]

    x mabVH
        Cohort mean ages at birth and cohort mean ages at birth by age 40
        CMAB    CMAB40
    x mabVHbo
        Cohort mean ages at birth by birth order and cohort mean ages at birth by birth order by age 40
        CMAB     CMAB1     CMAB2     CMAB3     CMAB4    CMAB5p    CMAB40  CMAB40_1  CMAB40_2  CMAB40_3  CMAB40_4 CMAB40_5p

    pprVHbo
        Cohort parity progression ratios
        PPR0_1    PPR1_2    PPR2_3    PPR3_4

    sdmabVH
        Standard deviation in cohort mean ages at birth and standard deviation in cohort mean ages at birth by age 40
        sdCMAB   sdCMAB40
    sdmabVHbo
        Standard deviation in cohort mean ages at birth by birth order and standard deviation in cohort mean ages at birth by birth order by age 40
        sdCMAB     sdCMAB1     sdCMAB2     sdCMAB3     sdCMAB4    sdCMAB5p    sdCMAB40  sdCMAB40_1  sdCMAB40_2  sdCMAB40_3  sdCMAB40_4 sdCMAB40_5p

    x tfrVH
        Completed cohort fertility and completed cohort fertility by age 40
        CCF     CCF40
    x tfrVHbo
        Completed cohort fertility by birth order and completed cohort fertility by birth order by age 40
        CCF      CCF1      CCF2      CCF3      CCF4     CCF5p     CCF40   CCF40_1   CCF40_2   CCF40_3   CCF40_4  CCF40_5p

    => OUTPUT
    columns
        CMAB
        PPR
        sdCMAB
        sdCMAB40
        CCF
        CCF40
    dimensions
        code, cohort, parity

------------
C 3c: code, cohort, age [COHORT]
    asfrVH
        Cohort fertility rates by birth cohort and age (horizontal parallelograms, age in completed years (ACY))
        ASFR
    asfrVHbo
        Cohort fertility rates by birth cohort, age and birth order (horizontal parallelograms, age in completed years (ACY))
        ASFR       ASFR1       ASFR2       ASFR3       ASFR4      ASFR5p

    birthsVH
        Live births by birth cohort and age (horizontal parallelograms, age in completed years (ACY))
        Total
    birthsVHbo
        Live births by birth cohort, age and birth order (horizontal parallelograms, age in completed years (ACY))
        Total          B1          B2          B3          B4         B5p

    ccfrVH
        Cumulative cohort fertility rates (horizontal parallelograms)
        CCFR
    ccfrVHbo
        Cumulative cohort fertility rates by birth order (horizontal parallelograms
        CCFR     CCFR1     CCFR2     CCFR3     CCFR4    CCFR5p

    exposVH
        Female population exposure by birth cohort and age (horizontal parallelograms, age in completed years (ACY))
        Exposure

    => OUTPUT
    columns
        ASFR
        B
        CCFR
        E
    dimensions
        code, cohort, age, parity

------------
C 3x COHORT FERTILITY TABLES: code, cohort, x [COHORT]
    cft
        Cohort fertility tables, birth orders 1 to 5+
        b1x     l0x       m1x       q1x   Sb1x   b2x    l1x       m2x       q2x   Sb2x    b3x    l2x       m3x       q3x   Sb3x    b4x    l3x       m4x       q4x   Sb4x   b5px    l4x      m5px      q5px   Sb5px     chix

    => OUTPUT
    columns
        b1x
        l0x
        m1x
        q1x
        Sb1x
        b2x
        l1x
        m2x
        q2x
        Sb2x
        b3x
        l2x
        m3x
        q3x
        Sb3x
        b4x
        l3x
        m4x
        q4x
        Sb4x
        b5px
        l4x
        m5px
        q5px
        Sb5px
        chix
    dimensions
        code, cohort, x

------------
P 3X PERIOD FERTILITY TABLES: code, year, x [PERIOD]
    pft
        Period fertility tables, birth orders 1 to 5+
        w0x       m1x       q1x     l0x    b1x     L0x   Sb1x       w1x       m2x       q2x    l1x    b2x     L1x   Sb2x       w2x       m3x       q3x    l2x    b3x     L2x   Sb3x       w3x       m4x       q4x    l3x    b4x     L3x   Sb4x       w4x      m5px      q5px    l4x   b5px     L4x  Sb5px

    pftc
        Census-based period fertility tables, birth orders 1 to 5+
        w0x       m1x       q1x     l0x    b1x     L0x   Sb1x       w1x       m2x       q2x    l1x    b2x     L1x   Sb2x       w2x       m3x       q3x    l2x    b3x     L2x   Sb3x       w3x       m4x       q4x    l3x    b4x     L3x   Sb4x       w4x      m5px      q5px    l4x   b5px     L4x  Sb5px

    => OUTPUT
    columns
        w0x
        m1x
        q1x
        l0x
        b1x
        L0x
        Sb1x
        w1x
        m2x
        2x
        l1x
        b2x
        L1x
        Sb2x
        w2x
        m3x
        q3x
        l2x
        b3x
        L2x
        Sb3x
        w3x
        m4x
        q4x
        l3x
        b4x
        L3x
        Sb4x
        w4x
        m5px
        q5px
        l4x
        b5px
        L4x
        Sb5px
    dimensions
        code, year, x

------------
C 4A: code, year, cohort, ardy [COHORT]
    asfrVV
        Period fertility rates by calendar year, age reached during the year (ARDY) and birth cohort (vertical parallelograms)
        ASFR
    asfrVVbo
        Period fertility rates by calendar year, age reached during the year (ARDY), birth cohort and birth order (vertical parallelograms)
        ASFR       ASFR1       ASFR2       ASFR3       ASFR4      ASFR5p

    birthsVV
        Live births by calendar year, age reached during the year (ARDY) and birth cohort (vertical parallelograms)
        Total
    birthsVVbo
        Live births by calendar year, age reached during the year (ARDY), birth cohort and birth order (vertical parallelograms)
        Total          B1          B2          B3          B4         B5p

    cpfrVV
        Cumulative period fertility rates (vertical parallelograms)
        CPFR
    cpfrVVbo
        Cumulative period fertility rates by birth order (vertical parallelograms)
        CPFR     CPFR1     CPFR2     CPFR3     CPFR4    CPFR5p

    exposVV
        Female population exposure by calendar year, age reached during the year (ARDY) and birth cohort (vertical parallelograms)
        Exposure

    => OUTPUT
    columns
        ASFR
        B
        CPFR
        E
    dimensions
        code, year, cohort, ardy
"""

import re

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# CONFIG
COLUMN_RAW = "column_raw"
COLUMN_IND = "indicator_name"
COLUMNS_RENAME = {
    "totbirthsrr": {
        "total": "b",
    }
}

# Tables to process for PERIOD country-year
TABLES_PERIOD = [
    "adjtfrrr",
    "cbrrr",
    "mabrr",
    "patfr",
    # "patfrc",
    "pmab",
    # "pmabc",
    "sdmabrr",
    "tfrrr",
    "totbirthsrr",
]
TABLES_PERIOD_W_PARITY = {
    "patfr": {
        "indicators": ["patfr"],
    },
    # "patfrc",
    "pmab": {
        "indicators": ["tmab"],
    },
    # "pmabc",
}
REGEX_PERIOD_BO = {}

# Tables to process for COHORT country-cohort
TABLES_COHORT = [
    "mabvh",
    "pprvhbo",
    "sdmabvh",
    "tfrvh",
]
TABLES_COHORT_W_PARITY = {
    "pprvhbo": {
        "indicators": ["ppr"],
    },
}
REGEX_COHORT_BO = {
    "pprvhbo": {
        "ppr": r"^ppr\d+_\d+$",
    },
}
# Tables to process for PERIOD country-year-age
TABLES_PERIOD_AGE = [
    "asfrrr",
]
TABLES_PERIOD_AGE_W_PARITY = {}
REGEX_PERIOD_AGE_BO = {}
# Tables to process for COHORT country-year-age
TABLES_COHORT_AGE = [
    "asfrvh",
    "ccfrvh",
]
TABLES_COHORT_AGE_W_PARITY = {}
REGEX_COHORT_AGE_BO = {}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hfd")

    # 1/ Read period tables + consolidate in one table tb_period
    ## Initial definitions
    cols_index = ["country", "year"]
    col_bo = "birth_order"
    ## Read tables
    tbs = make_table_list(
        ds_meadow=ds_meadow,
        table_names=TABLES_PERIOD,
        tables_w_parity=TABLES_PERIOD_W_PARITY,
        cols_index=cols_index,
        col_bo=col_bo,
        regex_bo=REGEX_PERIOD_BO,
    )
    ## Merge
    tb_period = consolidate_table_from_list(tbs, cols_index + [col_bo], "period")

    # 2/ Read cohort tables + consolidate in one table tb_cohort
    ## Initial definitions
    cols_index = ["country", "cohort"]
    col_bo = "birth_order"
    ## Read tables
    tbs = make_table_list(
        ds_meadow=ds_meadow,
        table_names=TABLES_COHORT,
        tables_w_parity=TABLES_COHORT_W_PARITY,
        cols_index=cols_index,
        col_bo=col_bo,
        regex_bo=REGEX_COHORT_BO,
    )
    # Quick fix: change birth_order label for PPR
    tbs = _fix_ppr(tbs)

    ## Merge
    def add_shifted_to_cohort(tb):
        cols_index_all = cols_index + [col_bo]
        # Create shifted cohorts
        tb_plus15 = tb.copy()
        tb_plus15["cohort"] = tb_plus15["cohort"] + 15
        tb_plus30 = tb.copy()
        tb_plus30["cohort"] = tb_plus30["cohort"] + 30
        # Merge
        tb = tb.merge(tb_plus15[cols_index_all + ["ccf"]], on=cols_index_all, suffixes=["", "_plus15y"], how="outer")
        tb = tb.merge(tb_plus30[cols_index_all + ["ccf"]], on=cols_index_all, suffixes=["", "_plus30y"], how="outer")
        return tb

    tb_cohort = consolidate_table_from_list(
        tbs=tbs,
        cols_index_out=cols_index + [col_bo],
        short_name="cohort",
        fcn=add_shifted_to_cohort,
    )

    # 3/ Period tables (by age)
    cols_index = ["country", "year", "age"]
    col_bo = "birth_order"
    ## Read tables
    tbs = make_table_list(
        ds_meadow=ds_meadow,
        table_names=TABLES_PERIOD_AGE,
        tables_w_parity=TABLES_PERIOD_AGE_W_PARITY,
        cols_index=cols_index,
        col_bo=col_bo,
        regex_bo=REGEX_PERIOD_AGE_BO,
    )
    ## Consolidate
    tb_period_ages = consolidate_table_from_list(
        tbs=tbs,
        cols_index_out=cols_index + [col_bo],
        short_name="period_ages",
        # fcn=keep_relevant_ages,
        formatting=False,
    )
    tb_period_ages = tb_period_ages.rename(
        columns={
            "asfr": "asfr_period",
        }
    )

    # TODO: move elsewhere
    # Build special table
    years = list(range(1925, tb_period_ages["year"].max() + 1, 5))
    tb_period_years = tb_period_ages.loc[
        tb_period_ages["year"].isin(years) & (tb_period_ages["birth_order"] == "total")
    ].drop(columns=["birth_order"])
    tb_period_years["age"] = tb_period_years["age"].str.replace("-", "").str.replace("+", "").astype("UInt8")
    tb_period_years = tb_period_years.rename(
        columns={
            "year": "year_as_dimension",
        }
    )
    tb_period_years = tb_period_years.format(["country", "age", "year_as_dimension"], short_name="period_ages_years")

    # Resume normal processing
    tb_period_ages = keep_relevant_ages(tb_period_ages)
    tb_period_ages = tb_period_ages.format(cols_index + [col_bo], short_name="period_ages")

    # 4/ Cohort tables (by age)
    cols_index = ["country", "cohort", "age"]
    col_bo = "birth_order"
    ## Read tables
    tbs = make_table_list(
        ds_meadow=ds_meadow,
        table_names=TABLES_COHORT_AGE,
        tables_w_parity=TABLES_COHORT_AGE_W_PARITY,
        cols_index=cols_index,
        col_bo=col_bo,
        regex_bo=REGEX_COHORT_AGE_BO,
        check_integration=False,
        check_integration_limit={
            "asfr": 143,
            "ccfr": 84,
        },
    )
    ## Consolidate
    tb_cohort_ages = consolidate_table_from_list(
        tbs=tbs,
        cols_index_out=cols_index + [col_bo],
        short_name="cohort_ages",
        # fcn=keep_relevant_ages,
        formatting=False,
    )
    tb_cohort_ages = tb_cohort_ages.rename(
        columns={
            "asfr": "asfr_cohort",
            "ccfr": "ccfr_cohort",
        }
    )

    # TODO: move elsewhere
    # Build special table
    years = list(range(1925, tb_cohort_ages["cohort"].max() + 1, 5))
    tb_cohort_years = tb_cohort_ages.loc[
        tb_cohort_ages["cohort"].isin(years) & (tb_cohort_ages["birth_order"] == "total")
    ].drop(columns=["birth_order"])
    tb_cohort_years["age"] = tb_cohort_years["age"].str.replace("-", "").str.replace("+", "").astype("UInt8")
    # Fix 12- vs 12, 55+ vs 55 etc.
    assert tb_cohort_years.groupby(["country", "cohort", "age"])["asfr_cohort"].nunique().max() == 1
    assert tb_cohort_years.groupby(["country", "cohort", "age"])["ccfr_cohort"].nunique().max() == 1
    tb_cohort_years = tb_cohort_years.groupby(["country", "cohort", "age"], as_index=False).mean()
    # Format
    tb_cohort_years = tb_cohort_years.format(["country", "age", "cohort"], short_name="cohort_ages_years")

    # Resume normal processing
    tb_cohort_ages = keep_relevant_ages(tb_cohort_ages)
    tb_cohort_ages = tb_cohort_ages.format(cols_index + [col_bo], short_name="cohort_ages")

    #
    # Process data.
    #
    tables = [
        tb_period,
        tb_cohort,
        tb_period_ages,
        tb_cohort_ages,
        tb_period_years,
        tb_cohort_years,
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


def make_table_list(
    ds_meadow,
    table_names,
    tables_w_parity,
    cols_index,
    col_bo,
    regex_bo=None,
    check_integration=True,
    check_integration_limit=None,
):
    """Reads relevant tables, and formats them accordingly.

    Tables come in wide format, sometimes as two-tables (main and birth order). This function consolidates them into single tables per topic.

    For instance, we have one table with total fertility rates (columns `tfr`). And then another one with fertilities broken down by birth order (columns `tfr`, `tfr1`, etc.)
    Instead, we want a table in long format, which has one column `tfr` and adds the birth order as a dimension of the table.
    """
    if regex_bo is None:
        regex_bo = {}

    tbs = []
    for tname in table_names:
        # Get custom regex for this table
        regex = regex_bo.get(tname)

        # Read main table
        tb = read_table(ds_meadow, tname)

        # Check if there is a birth order table for this indicator(s). If so, process it and integrate it to the main table
        tname_bo = tname + "bo"
        if tname_bo in ds_meadow.table_names:
            # Read BO table
            tb_bo = read_table(ds_meadow, tname_bo, tname)
            # Get list of core indicators: These are the names of the columns that are actual indicators (and not dimensional indicators, e.g. `tfr1`, `tfr2`, etc.)
            core_indicators = [col for col in tb.columns.intersection(tb_bo.columns) if col not in cols_index]
            # Add BO to main table
            tb = integrate_bo(
                tb=tb,
                tb_bo=tb_bo,
                cols_index=cols_index,
                core_indicators=core_indicators,
                check=check_integration,
                check_limit_wrong=check_integration_limit,
            )
            # Consolidate table: Use long format, and add birth_order as a dimension of the main table.
            tb = make_table_with_birth_order(tb, cols_index, core_indicators, col_bo, regex)
        # Sometimes, the main table contains already indicators broken down by birth order. In such cases, we also need to reshape the table.
        elif tname in tables_w_parity:
            core_indicators = tables_w_parity[tname]["indicators"]
            tb = make_table_with_birth_order(tb, cols_index, core_indicators, col_bo, regex)

        dtypes = {}
        if "age" in tb.columns:
            dtypes["age"] = "string"
        if "birth_order" in tb.columns:
            dtypes["birth_order"] = "string"
        tb = tb.astype(dtypes)

        # Add formatted table to the list of tables.
        tbs.append(tb)

    return tbs


def read_table(ds_meadow, tname, tname_base=None):
    """Read table from dataset and minor cleaning:

    - Rename columns if applicable
    - Harmonize country names
    """
    # Read table
    tb = ds_meadow.read(tname)

    # Rename columns
    if tname_base is None:
        tname_base = tname
    if tname_base in COLUMNS_RENAME:
        tb = tb.rename(columns=COLUMNS_RENAME[tname_base])

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        country_col="code",
        warn_on_unused_countries=False,
    )

    # Rename country column
    tb = tb.rename(columns={"code": "country"})

    return tb


def integrate_bo(tb, tb_bo, cols_index, core_indicators, check=True, check_limit_wrong=None):
    """Merge main table with its BO counterpart.

    Some tables have a secondary table which provides the same core indicators but by birth order.
    """
    # Outer join
    tb = tb.merge(
        tb_bo,
        on=cols_index,
        suffixes=["", "__bo"],
        how="outer",
    )

    # Integrate core indicators
    # It can happen that one table has more values for the core indicator. We solve this with fillna calls.
    for col in core_indicators:
        # Check that we can integrate them!
        TOLERANCE = 5e-3

        # Check per column
        do_check = check
        if isinstance(check, dict):
            do_check = check.get(col, True)

        if do_check:
            assert (
                (((tb[col] - tb[f"{col}__bo"]) / tb[col]).dropna().abs() < TOLERANCE).all()
            ).all(), f"Integration failed for {col}. Core indicator is not equivalent between main and `bo` tables."
        elif check_limit_wrong is not None:
            max_allowed = check_limit_wrong
            if isinstance(check_limit_wrong, dict):
                if col not in check_limit_wrong:
                    raise ValueError(f"Missing limit for {col} in check_limit_wrong!")
                max_allowed = check_limit_wrong[col]

            num = (~(((tb[col] - tb[f"{col}__bo"]) / tb[col]).dropna().abs() < TOLERANCE)).sum()
            assert (
                num == max_allowed
            ), f"Integration failed for {col}. There are too many ({num}) allowed miss-matches ({check_limit_wrong})!"
        # Actual integration
        tb[col] = tb[col].fillna(tb[f"{col}__bo"])
        tb = tb.drop(columns=[f"{col}__bo"])

    return tb


def make_table_with_birth_order(tb, cols_index, core_indicators, col_bo="birth_order", regex_bo=None):
    """Change the format of a table from wide to long, to incorporate the birth order as a dimension."""

    if regex_bo is None:
        regex_bo = {}

    def _generate_regex(name):
        if re.search(r"\d$", string=name):  # Check if the name ends with a number
            return rf"^{name}_?(\d+|(\d+p)?)$"
        else:
            return rf"^{name}(\d+|(\d+p)?)$"

    regex_patterns = {name: regex_bo.get(name, _generate_regex(name)) for name in core_indicators}

    tb = tb.melt(
        cols_index,
        var_name=COLUMN_RAW,
        value_name="value",
    )

    tb["indicator_name"] = None
    tb[col_bo] = None
    for name, pattern in regex_patterns.items():
        # print(f"> {name}")

        # Set indicator name
        flag_0 = (~tb[COLUMN_RAW].isin(core_indicators)) | (tb[COLUMN_RAW] == name)
        flag = tb[COLUMN_RAW].str.match(pattern) & flag_0
        assert tb.loc[flag, COLUMN_IND].isna().all(), "Multiple columns assign to the same indicator!"
        tb.loc[flag, COLUMN_IND] = name

        # Get birth order
        tb.loc[flag, col_bo] = tb.loc[flag, COLUMN_RAW].replace({f"{name}_?": ""}, regex=True)
        tb.loc[tb[COLUMN_RAW] == name, col_bo] = "total"

    # Sanity check
    assert tb[COLUMN_IND].notna().all(), "Some NaNs found in column `indicator_name`"
    assert tb[col_bo].notna().all(), f"Some NaNs found in column `{col_bo}`"

    # Final reshape
    tb = tb.drop(columns=[COLUMN_RAW])
    tb = tb.pivot(index=cols_index + [col_bo], columns=COLUMN_IND, values="value").reset_index()
    tb = tb.rename_axis(None, axis=1)

    # Drop NaNs
    tb = tb.dropna(subset=core_indicators)

    return tb


def consolidate_table_from_list(tbs, cols_index_out, short_name, fcn=None, formatting=True) -> geo.Table:
    ## Sanity check: no column is named the same
    _sanity_check_colnames(tbs, cols_index_out)

    # Merge
    tb = pr.multi_merge(tbs, on=cols_index_out, how="outer")

    # Optional function
    if fcn is not None:
        tb = fcn(tb)

    # Format
    if formatting:
        tb = tb.format(cols_index_out, short_name=short_name)
    return tb


def _fix_ppr(tbs):
    for tb in tbs:
        if tb.m.short_name == "pprvhbo":
            tb["birth_order"] = tb["birth_order"].str.split("_").str[-1]
    return tbs


def _sanity_check_colnames(tbs, cols_index_out):
    colnames = [col for t in tbs for col in t.columns if col not in cols_index_out]
    assert len(colnames) == len(set(colnames)), "Some columns are named the same!"


def keep_relevant_ages(tb):
    AGES_RELEVANT = [
        "12-",
        "20",
        "30",
        "40",
        "50",
        "55+",
    ]
    tb = tb.loc[tb["age"].isin(AGES_RELEVANT)]
    return tb
