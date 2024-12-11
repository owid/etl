"""Load a snapshot and create a meadow dataset.

Each table has different dimensions. I had to explore them and decide which columns to use as index. Find below the list of columns, tagged according to the columns used to index them:

2y adjtfrRR
2y adjtfrRRbo

3y asfrRR
3y asfrRRbo
4y asfrTR
4y asfrTRbo
3c asfrVH
3c asfrVHbo
4A asfrVV
4A asfrVVbo

3y birthsRR
3y birthsRRbo
4y birthsTR
4y birthsTRbo
3c birthsVH
3c birthsVHbo
4A birthsVV
4A birthsVVbo

2y cbrRR
2y cbrRRbo

3c ccfrVH
3c ccfrVHbo

3x cft

3y cpfrRR
3y cpfrRRbo
4A cpfrVV
4A cpfrVVbo

3y exposRR
3y exposRRpa
3y exposRRpac
4y exposTR
3c exposVH
4A exposVV

2y mabRR
2y mabRRbo
2c mabVH
2c mabVHbo

3y mi
3y mic

2y patfr
2y patfrc

3X pft
3X pftc

2y pmab
2y pmabc

2c pprVHbo

2y sdmabRR
2y sdmabRRbo
2c sdmabVH
2c sdmabVHbo

2y tfrRR
2y tfrRRbo
2c tfrVH
2c tfrVHbo

2y totbirthsRR
2y totbirthsRRbo


where:

2y: code, year

    adjtfrRR
        Tempo-adjusted total fertility rates, Bongaarts-Feeney method
        adjTFR
    adjtfrRRbo
        Tempo-adjusted total fertility rates by birth order, Bongaarts-Feeney method
        adjTFR   adjTFR1   adjTFR2   adjTFR3   adjTFR4  adjTFR5p


    cbrRR
        Crude birth rate
        CBR
    cbrRRbo
        Crude birth rate by birth order
        CBR     CBR1     CBR2     CBR3     CBR4    CBR5p

    mabRR
        Period mean ages at birth and period mean ages at birth by age 40
        MAB      MAB40
    mabRRbo
        Period mean ages at birth by birth order and period mean ages at birth by birth order by age 40
        MAB1     MAB2     MAB3     MAB4    MAB5p    MAB40_1    MAB40_2    MAB40_3    MAB40_4    MAB40_5p

    patfr
        Parity- and age-adjusted total fertility rate
        PATFR   PATFR1   PATFR2   PATFR3   PATFR4  PATFR5p
    patfrc
        Parity- and age-adjusted total fertility rate (based on parity distribution from population census)
        PATFR   PATFR1   PATFR2   PATFR3   PATFR4  PATFR5p

    pmab
        Period table mean ages at birth by birth order
        TMAB    TMAB1    TMAB2    TMAB3    TMAB4   TMAB5p
    pmabc
        Period table mean ages at birth by birth order (based on parity distribution from population census)
        TMAB    TMAB1    TMAB2    TMAB3    TMAB4   TMAB5p

    sdmabRR
        Standard deviation in period mean ages at birth and standard deviation in period mean ages at birth by age 40
        sdMAB    sdMAB40
    sdmabRRbo
        Standard deviation in period mean ages at birth by birth order and standard deviation in period mean ages at birth by birth order by age 40
        sdMAB      sdMAB1      sdMAB2      sdMAB3      sdMAB4     sdMAB5p     sdMAB40   sdMAB40_1   sdMAB40_2   sdMAB40_3   sdMAB40_4  sdMAB40_5p

    tfrRR
        Period total fertility rates and period total fertility rates by age 40
        TFR     TFR40
    tfrRRbo
        Period total fertility rates by birth order and period total fertility rates by birth order by age 40
        TFR      TFR1      TFR2      TFR3      TFR4     TFR5p     TFR40   TFR40_1   TFR40_2   TFR40_3   TFR40_4  TFR40_5p


    => OUTPUT

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
    dimensions
        code, year, birth_order

3y: code, year, age
4y: code, year, age, cohort
2c: code, cohort
3c: code, cohort, age
3x: code, cohort, x
3X: code, year, x
4A: code, year, cohort, ardy
----

3y asfrRR
3y asfrRRbo
4y asfrTR
4y asfrTRbo
3c asfrVH
3c asfrVHbo
4A asfrVV
4A asfrVVbo

3y birthsRR
3y birthsRRbo
4y birthsTR
4y birthsTRbo
3c birthsVH
3c birthsVHbo
4A birthsVV
4A birthsVVbo


3c ccfrVH
3c ccfrVHbo

3x cft

3y cpfrRR
3y cpfrRRbo
4A cpfrVV
4A cpfrVVbo

3y exposRR
3y exposRRpa
3y exposRRpac
4y exposTR
3c exposVH
4A exposVV

2c mabVH
2c mabVHbo

3y mi
3y mic

3X pft
3X pftc

2c pprVHbo

2c sdmabVH
2c sdmabVHbo

2c tfrVH
2c tfrVHbo

2y totbirthsRR
2y totbirthsRRbo

"""
from pathlib import Path

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Behavior 1:
# Mosrtly use ["code", "year", "age"] except:
# - When table end with 'TR' or 'TRbo': ["code", "year", "year", "cohort"] for those
# - When table end with 'VV' or 'VVbo': ["code", "year", "ardy"] for those
cols_1 = (
    "asfr",
    "births",
    "ccfr",
    "cpfr",
    "expos",
    "mi",
)
# Behaviour 2: Use ["code", "cohort"] always
cols_2 = ("pprVHbo",)
# Behavior 3: Use ["code", "year", "age"] always
cols_3 = (
    "cft",
    "pft",
)


def get_cols_format(tb, short_name):
    cols_index = None  # Default value

    # 2y: code, year
    if (short_name.endswith(("RR", "RRbo")) and "Age" not in tb.columns) or (
        short_name in {"patfr", "patfrc", "pmab", "pmabc"}
    ):
        cols_index = ["code", "year"]

    # 3y: code, year, age
    elif (short_name.endswith(("RR", "RRbo", "RRpa", "RRpac")) and "Age" in tb.columns) or (
        short_name in {"mi", "mic"}
    ):
        cols_index = ["code", "year", "age"]

    # 4y: code, year, age, cohort
    elif short_name.endswith(("TR", "TRbo")):
        cols_index = ["code", "year", "age", "cohort"]

    # 2c: code, cohort
    elif short_name.endswith(("VH", "VHbo")) and "Age" not in tb.columns:
        cols_index = ["code", "cohort"]

    # 3c: code, cohort, age
    elif short_name.endswith(("VH", "VHbo")) and "Age" in tb.columns:
        cols_index = ["code", "cohort", "age"]

    # 3X: code, cohort, x
    elif short_name == "cft":
        cols_index = ["code", "cohort", "x"]

    # 3X: code, year, x
    elif short_name in {"pft", "pftc"}:
        cols_index = ["code", "year", "x"]

    # 4A: code, year, cohort, ardy
    elif short_name.endswith(("VV", "VVbo")):
        cols_index = ["code", "year", "cohort", "ardy"]

    else:
        raise Exception(f"No index columns defined for this table! {short_name}")
    return cols_index


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("hfd.zip")

    # Load data from snapshot.
    tbs = []
    with snap.extract_to_tempdir() as tmp_dir:
        p = Path(tmp_dir)
        files = sorted(p.glob("Files/zip_w/*.txt"))
        for f in files:
            # print(f"> {f}")
            # Read the content of the text file
            tb_ = pr.read_csv(
                f,
                sep="\s+",
                skiprows=2,
                na_values=["."],
                metadata=snap.to_table_metadata(),
                origin=snap.m.origin,
            )
            short_name = f.stem

            # Detect the columns to use to index the table (accounting for dimensions)
            cols_format = get_cols_format(tb_, short_name)
            # print(cols_format)
            # print(tb_.columns)

            if short_name in {"pft", "pftc"}:
                tb_ = tb_.rename(
                    columns={
                        "L0x": "cap_l0x",
                        "L1x": "cap_l1x",
                        "L2x": "cap_l2x",
                        "L3x": "cap_l3x",
                        "L4x": "cap_l4x",
                    }
                )
            # Format
            tb_ = tb_.format(cols_format, short_name=short_name)
            tbs.append(tb_)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tbs, check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
