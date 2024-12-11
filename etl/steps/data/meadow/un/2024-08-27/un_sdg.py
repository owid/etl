"""Load a snapshot and create a meadow dataset."""
import re

import pandas as pd
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = get_logger()

COLS = [
    "goal",
    "target",
    "indicator",
    "seriescode",
    "seriesdescription",
    "geoareacode",
    "country",
    "year",
    "value",
    "time_detail",
    "timecoverage",
    "upperbound",
    "lowerbound",
    "baseperiod",
    "source",
    "geoinfourl",
    "footnote",
    "age",
    "hazard_type",
    "location",
    "nature",
    "observation_status",
    "quantile",
    "reporting_type",
    "sex",
    "units",
    "freq",
    "severity_of_price_levels",
    "type_of_product",
    "ihr_capacity",
    "name_of_non_communicable_disease",
    "substance_use_disorders",
    "type_of_occupation",
    "education_level",
    "type_of_skill",
    "activity",
    "deviation_level",
    "level_status",
    "type_of_renewable_technology",
    "disability_status",
    "migratory_status",
    "mode_of_transportation",
    "type_of_mobile_technology",
    "counterpart",
    "fiscal_intervention_stage",
    "grounds_of_discrimination",
    "name_of_international_institution",
    "policy_domains",
    "cities",
    "custom_breakdown",
    "level_of_government",
    "type_of_facilities",
    "food_waste_sector",
    "government_name",
    "level_of_requirement",
    "name_of_international_agreement",
    "policy_instruments",
    "type_of_waste_treatment",
    "report_ordinal",
    "type_of_support",
    "frequency_of_chlorophyll_a_concentration",
    "marine_spatial_planning__msp",
    "nutrient_loading",
    "sampling_stations",
    "bioclimatic_belt",
    "land_cover",
    "mountain_elevation",
    "cause_of_death",
    "illicit_financial_flows",
    "parliamentary_committees",
    "population_group",
    "service_attribute",
    "tariff_regime__status",
    "type_of_ofdi_scheme",
    "type_of_speed",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.

    log.info("un_sdg.start")
    snap = paths.load_snapshot("un_sdg.feather")

    tb = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    log.info("un_sdg.load_and_clean")
    tb = load_and_clean(tb)

    log.info("Size of dataframe", rows=tb.shape[0], colums=tb.shape[1])

    tb = tb.reset_index(drop=True).drop(columns="index")

    tb = tb.format(keys=COLS)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def load_and_clean(tb: Table) -> Table:
    # Load and clean the data
    log.info("un_sdg.reading_in_original_data")
    original_tb = tb.copy(deep=False)

    # removing values that aren't numeric e.g. Null and N values
    original_tb.dropna(subset=["Value"], inplace=True)
    original_tb.dropna(subset=["TimePeriod"], how="all", inplace=True)

    original_tb = original_tb.loc[pd.to_numeric(original_tb["Value"], errors="coerce").notnull()]
    original_tb.rename(columns={"GeoAreaName": "Country", "TimePeriod": "Year"}, inplace=True)
    original_tb = original_tb.rename(columns=lambda k: re.sub(r"[\[\]]", "", k))  # type: ignore
    return original_tb
