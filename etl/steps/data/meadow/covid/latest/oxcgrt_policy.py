"""Load a snapshot and create a meadow dataset."""

from owid.catalog.processing import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Relevant columns
COLUMNS_RELEVANT_COMPACT = [
    "CountryName",
    "Date",
    "RegionCode",
    "C1M_School closing",
    "C2M_Workplace closing",
    "C3M_Cancel public events",
    "C4M_Restrictions on gatherings",
    "C5M_Close public transport",
    "C6M_Stay at home requirements",
    "C7M_Restrictions on internal movement",
    "C8EV_International travel controls",
    "E1_Income support",
    "E2_Debt/contract relief",
    "E3_Fiscal measures",
    "E4_International support",
    "H1_Public information campaigns",
    "H2_Testing policy",
    "H3_Contact tracing",
    "H4_Emergency investment in healthcare",
    "H5_Investment in vaccines",
    "H6M_Facial Coverings",
    "H7_Vaccination policy",
    "StringencyIndex_Average",
    "ContainmentHealthIndex_Average",
    "V2A_Vaccine Availability (summary)",
    "V2B_Vaccine age eligibility/availability age floor (general population summary)",
    "V2C_Vaccine age eligibility/availability age floor (at risk summary)",
]
COLUMNS_RELEVANT_VAX = [
    "CountryName",
    "Date",
    "V2_Vaccine Availability (summary)",
    "V2_Pregnant people",
]

COLUMNS_RELEVANT_NATIONAL = [
    "CountryName",
    "Date",
    "StringencyIndex_SimpleAverage",
    "StringencyIndex_NonVaccinated",
    "StringencyIndex_Vaccinated",
    "StringencyIndex_WeightedAverage",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap_compact = paths.load_snapshot("oxcgrt_policy_compact.csv")
    snap_vax = paths.load_snapshot("oxcgrt_policy_vaccines.csv")
    snap_2020 = paths.load_snapshot("oxcgrt_policy_national_2020.csv")
    snap_2021 = paths.load_snapshot("oxcgrt_policy_national_2021.csv")
    snap_2022 = paths.load_snapshot("oxcgrt_policy_national_2022.csv")

    # Load data from snapshot.
    tb_compact = snap_compact.read(usecols=COLUMNS_RELEVANT_COMPACT)
    tb_vax = snap_vax.read(usecols=COLUMNS_RELEVANT_VAX)
    tb_2020 = snap_2020.read(usecols=COLUMNS_RELEVANT_NATIONAL)
    tb_2021 = snap_2021.read(usecols=COLUMNS_RELEVANT_NATIONAL)
    tb_2022 = snap_2022.read(usecols=COLUMNS_RELEVANT_NATIONAL)

    #
    # Process data.
    #
    tb_stringency = concat([tb_2020, tb_2021, tb_2022], short_name="oxcgrt_policy_stringency")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_compact.format(["countryname", "date"]),
        tb_vax.format(["countryname", "date"]),
        tb_stringency.format(keys=["countryname", "date"]),
    ]
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir=dest_dir, tables=tables, check_variables_metadata=True, default_metadata=snap_compact.metadata
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
