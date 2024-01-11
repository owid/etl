"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Columns to select from the data, and how to rename them.
# NOTE: Definitions were copied (or inferred or adapted) from the original codebook:
# https://kyungwonsuh.weebly.com/uploads/1/4/6/1/146191116/codebook_v2.pdf
# ccode1 - This indicates state A’s the Correlates of War (COW) Project country codes.
# ccode2 - This indicates state B’s the COW Project country codes.
# year - This indicates four-digit year of observation.
# int_capdis - This indicates the inter-capital distance of state A and B, in miles.
# int_capdis__km - This indicates the inter-capital distance of state A and B, in kilometers. The data are from EUGene ver 3.204.
# land1 - This indicates the total number of state A’s deployed land-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# sea1 - This indicates the total number of state A’s deployed sea-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# air1 - This indicates the total number of state A’s deployed air-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# total1 - This indicates the total number of state A’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year
# dnukel1 - This indicates the total number of nuclear warheads that can be delivered to state B by state A’s deployed land-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# dnukes1 - This indicates the total number of nuclear warheads that can be delivered to state B by state A’s deployed sea-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# dnukea1 - This indicates the total number of nuclear warheads that can be delivered to state B by state A’s deployed air-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year. For India (v. Pakistan, 1994-2010), the values of this variable are replaced with the values of (totalw1-dnukel1).
# totalw1 - This indicates the total number of nuclear warheads possessed by state A in a given year.
# totaldw1 - This indicates the total number of nuclear warheads that can be delivered to state B by state A’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# emt1 - This indicates the EMT (equivalent megatonnage) score of state A’s nuclear strike using all nuclear warheads that can be delivered to state B by state A’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# cmp1 - This indicates the CMP (counter-military potential) score of state A’s nuclear strike using all nuclear warheads that can be delivered to state B by state A’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# land2 - This indicates the total number of state B’s deployed land-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# sea2 - This indicates the total number of state B’s deployed sea-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# air2 - This indicates the total number of state B’s deployed air-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# total2 - This indicates the total number of state B’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# dnukel2 - This indicates the total number of nuclear warheads that can be delivered to state A by state B’s deployed land-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# dnukes2 - This indicates the total number of nuclear warheads that can be delivered to state A by state B’s deployed sea-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# dnukea2 - This indicates the total number of nuclear warheads that can be delivered to state A by state B’s deployed air-based strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year. For the United Kingdom (v. the United States, 1957-1960), the values of this variable are replaced with the values of the totalw2. For Pakistan (v. India, 1995-2006), the values of this variable are replaced with the values of (totalw2-dnukel2).
# totalw2 - This indicates the total number of nuclear warheads possessed by state B in a given year.
# NOTE: In the codebook, the definitions of totaldw1 and totaldw2 are identical. I fixed the following as expected:
# totaldw2 - This indicates the total number of nuclear warheads that can be delivered to state A by state B’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# emt2 - This indicates the EMT score of state B’s nuclear strike using all nuclear warheads that can be delivered to state A by state A’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.
# cmp2 - This indicates the CMP score of state B’s nuclear strike using all nuclear warheads that can be delivered to state A by state B’s all strategic nuclear delivery platforms whose range of operation is greater than the A-B inter-capital distance in a given non-directed dyad year.

# One equivalent megaton can destroy an area of about 20 square miles (or 51.8 square kilometers), as estimated by
# Richelson, Jeffrey. 1980. Evaluating the Strategic Balance. American Journal of Political Science 24(4): 782.
AREA_IN_SQ_KM_PER_EQUIVALENT_MEGATON = 51.8

# Columns to select from the dyadic data, and how to rename them.
# NOTE: Columns in the original data are followed by either "1" or "2". But the data will be converted into the maximum
# capabilities of each country (regardless of the attacked country), and then the columns will be renamed.
COLUMNS_DYADIC = {
    "ccode": "country",
    "year": "year",
    "totalw": "nuclear_warheads_owned",
    "totaldw": "nuclear_warheads_deliverable",
    "emt": "nuclear_warheads_yield",
}

# Columns to select from the monadic data, and how to rename them.
COLUMNS_MONADIC = {
    "ccode": "country",
    "year": "year",
    "warheads": "nuclear_warheads_owned",
    "monadicdnuke": "nuclear_warheads_deliverable",
    "monadicemt": "nuclear_warheads_yield",
}


def prepare_dyadic_data(tb_dyadic: Table) -> Table:
    # For each country-year, we need:
    # * The total number of nuclear warheads owned (the stockpile).
    # * The maximum number of warheads that could be delivered in a first strike, against any country.
    # * The maximum yield (in number of megatons) that could be released on a first strike, against any country.
    # * The maximum area (in square kilometers) that could be destroyed on a first strike, against any country.
    # To achieve that, we only need ccode, year, totalw, and emt.
    # However, a country can be either state A or state B in a given year, and the maximum can be on either of the two.
    # For example, for country with code 365, in 1964, the maximum EMT is 1206, which happens when the country is state
    # B (against country 200).
    # Therefore, we will:
    # 1. Separate the relevant data about state A, and the relevant data about state B.
    # 2. Assert that, where they overlap, the number of nuclear warheads owned is consistent (since this doesn't depend
    #  on the attacked country).
    # 3. Combine both, and pick maximum values.
    tb_dyadic = tb_dyadic.copy()

    # Separate data for state A and state B.
    tb_a = tb_dyadic[["ccode1", "year", "totalw1", "totaldw1", "emt1"]].rename(
        columns={"ccode1": "ccode", "totalw1": "totalw", "totaldw1": "totaldw", "emt1": "emt"}, errors="raise"
    )
    tb_b = tb_dyadic[["ccode2", "year", "totalw2", "totaldw2", "emt2"]].rename(
        columns={"ccode2": "ccode", "totalw2": "totalw", "totaldw2": "totaldw", "emt2": "emt"}, errors="raise"
    )

    # Sanity check.
    compared = tb_a.merge(tb_b, on=["ccode", "year"], how="inner")
    error = "The stockpile of nuclear weapons should be the same, regardless of whether a country is state A or B."
    assert (compared["totalw_x"] == compared["totalw_y"]).all(), error

    # Combine data for state A and state B.
    combined = pr.concat([tb_a, tb_b], ignore_index=True)

    # Calculate the maximum values for each country and year.
    combined = combined.groupby(["ccode", "year"], as_index=False).max()

    return combined


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its tables of dyadic and monadic data.
    ds_meadow = paths.load_dataset("strategic_nuclear_forces")
    tb_dyadic = ds_meadow["strategic_nuclear_forces_dyadic"].reset_index()
    tb_monadic = ds_meadow["strategic_nuclear_forces_monadic"].reset_index()

    #
    # Process data.
    #
    # First prepare dyadic data to include the maximum nuclear capabilities of each country, regardless of the country
    # they could attack.
    tb_dyadic = prepare_dyadic_data(tb_dyadic=tb_dyadic)

    # Rename columns.
    tb_dyadic = tb_dyadic[list(COLUMNS_DYADIC)].rename(columns=COLUMNS_DYADIC, errors="raise")
    tb_monadic = tb_monadic[list(COLUMNS_MONADIC)].rename(columns=COLUMNS_MONADIC, errors="raise")

    # Add a column for the area that could be destroyed on a first strike.
    tb_dyadic["nuclear_warheads_area"] = tb_dyadic["nuclear_warheads_yield"] * AREA_IN_SQ_KM_PER_EQUIVALENT_MEGATON
    tb_monadic["nuclear_warheads_area"] = tb_monadic["nuclear_warheads_yield"] * AREA_IN_SQ_KM_PER_EQUIVALENT_MEGATON

    # Harmonize country names.
    tb_dyadic = geo.harmonize_countries(
        df=tb_dyadic.astype({"country": str}), countries_file=paths.country_mapping_path
    )
    tb_monadic = geo.harmonize_countries(
        df=tb_monadic.astype({"country": str}), countries_file=paths.country_mapping_path
    )

    # Set an appropriate index and sort conveniently.
    tb_dyadic = tb_dyadic.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)
    tb_monadic = tb_monadic.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_dyadic, tb_monadic], check_variables_metadata=True)
    ds_garden.save()
