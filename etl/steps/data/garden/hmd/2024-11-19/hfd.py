"""There are various input tables, this step brings them together.

------------
P 2y: code, year [PERIOD]

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

    totbirthsRR
        Total live births
        Total
    totbirthsRRbo
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

    birthsRR
        Live births by calendar year and age (Lexis squares, age in completed years (ACY))
        Total
    birthsRRbo
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

    birthsTR
        Live births by calendar year, age and birth cohort (Lexis triangles)
        Total
    birthsTRbo
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

    mabVH
        Cohort mean ages at birth and cohort mean ages at birth by age 40
        CMAB    CMAB40
    mabVHbo
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

    tfrVH
        Completed cohort fertility and completed cohort fertility by age 40
        CCF     CCF40
    tfrVHbo
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

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("hfd")

    # Read table from meadow dataset.
    tb = ds_meadow.read("hfd")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
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
etl harmonize data/meadow/hmd/2024-11-19/hfd/pft.feather code etl/steps/data/garden/hmd/2024-11-19/hfd.countries.yml
