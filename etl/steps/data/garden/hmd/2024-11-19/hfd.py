"""There are various input tables, this step brings them together.

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
        Cumulative cohort fertility rates (horizontal parallelograms
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
# Tables to process for COHORT country-cohort
TABLES_COHORT = [
    "mabvh",
    # "pprvhbo",
    "sdmabvh",
    "tfrvh",
]
TABLES_COHORT_W_PARITY = {
    # "pprvhbo": {
    #     "indicators": ["patfr"],
    # },
}


def integrate_bo(tb, tb_bo, col_index, core_indicators):
    """Merge main table with its BO counterpart.

    Some tables have a secondary table which provides the same core indicators but by birth order.
    """
    # Outer join
    tb = tb.merge(
        tb_bo,
        on=col_index,
        suffixes=["", "__bo"],
        how="outer",
    )

    # Integrate core indicators
    # It can happen that one table has more values for the core indicator. We solve this with fillna calls.
    for col in core_indicators:
        # Check that we can integrate them!
        TOLERANCE = 5e-3
        assert (
            (((tb[col] - tb[f"{col}__bo"]) / tb[col]).dropna().abs() < TOLERANCE).all()
        ).all(), f"Integration failed for {col}. Core indicator is not equivalent between main and `bo` tables."

        # Actual integration
        tb[col] = tb[col].fillna(tb[f"{col}__bo"])
        tb = tb.drop(columns=[f"{col}__bo"])

    return tb


def make_table_with_birth_order(tb, col_index, core_indicators, col_bo="birth_order"):
    """Make a table from wide to long, to incorporate the birth order as a dimension."""

    def _generate_regex(name):
        if re.search(r"\d$", name):  # Check if the name ends with a number
            return rf"^{name}_?(\d+|(\d+p)?)$"
        else:
            return rf"^{name}(\d+|(\d+p)?)$"

    regex_patterns = {name: _generate_regex(name) for name in core_indicators}

    tb = tb.melt(
        col_index,
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
    tb = tb.pivot(index=col_index + [col_bo], columns=COLUMN_IND, values="value").reset_index()
    tb = tb.rename_axis(None, axis=1)

    # Drop NaNs
    tb = tb.dropna(subset=core_indicators)

    return tb


def read_table(ds_meadow, tname, tname_base=None):
    """Read table from dataset and minor cleaning:

    - Rename columns if applicable
    - Harmonize country names
    """
    tb = ds_meadow.read(tname)

    # Rename columns
    if tname_base is None:
        tname_base = tname

    if tname_base in COLUMNS_RENAME:
        tb = tb.rename(columns=COLUMNS_RENAME[tname_base])

    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        country_col="code",
    )

    tb = tb.rename(columns={"code": "country"})

    return tb


def make_table_list(ds_meadow, table_names, cols_index, col_bo):
    tbs = []
    for tname in table_names:
        # print(tname)
        # Main table
        tb = read_table(ds_meadow, tname)

        # Birth order table
        tname_bo = tname + "bo"
        if tname_bo in ds_meadow.table_names:
            # Read BO table
            tb_bo = read_table(ds_meadow, tname_bo, tname)

            # Get list of core indicators
            core_indicators = [col for col in tb.columns.intersection(tb_bo.columns) if col not in cols_index]
            # Add BO to main table
            tb = integrate_bo(tb, tb_bo, cols_index, core_indicators)
            # Consolidate table
            tb = make_table_with_birth_order(tb, cols_index, core_indicators, col_bo)
        elif tname in TABLES_PERIOD_W_PARITY:
            core_indicators = cols_index + TABLES_PERIOD_W_PARITY[tname]["indicators"]
            tb = make_table_with_birth_order(tb, cols_index, core_indicators, col_bo)
        tbs.append(tb)

    return tbs


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hfd")

    # Read period tables
    cols_index = ["country", "year"]
    col_bo = "birth_order"
    cols_index_out = cols_index + [col_bo]
    tbs = make_table_list(ds_meadow, TABLES_PERIOD, cols_index, col_bo)
    ## Sanity check: no column is named the same
    colnames = [col for t in tbs for col in t.columns if col not in cols_index_out]
    assert len(colnames) == len(set(colnames)), "Some columns are named the same!"
    ## Merge
    tb_period = pr.multi_merge(tbs, on=cols_index_out, how="outer")
    tb_period = tb_period.format(cols_index_out, short_name="period")

    # Read cohort tables
    cols_index = ["country", "cohort"]
    col_bo = "birth_order"
    cols_index_out = cols_index + [col_bo]
    tbs = make_table_list(ds_meadow, TABLES_COHORT, cols_index, col_bo)
    ## Sanity check: no column is named the same
    colnames = [col for t in tbs for col in t.columns if col not in cols_index_out]
    assert len(colnames) == len(set(colnames)), "Some columns are named the same!"
    ## Merge
    tb_cohort = pr.multi_merge(tbs, on=cols_index_out, how="outer")
    tb_cohort = tb_cohort.format(cols_index_out, short_name="cohort")

    #
    # Process data.
    #
    tables = [
        tb_period,
        tb_cohort,
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
