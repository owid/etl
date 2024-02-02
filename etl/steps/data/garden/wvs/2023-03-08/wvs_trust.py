"""Load a meadow dataset and create the World Values Survey - Trust garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("wvs_trust.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("wvs_trust")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wvs_trust"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index()

    #
    # Process data.
    #
    log.info("wvs_trust.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Verify index and sort
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(df, like=tb_meadow)

    # Drop selected variables
    vars_to_drop = [
        "trust_first_not_very_much",
        "trust_personally_not_very_much",
        "confidence_confidence_in_cer_with_australia",  # Only New Zealand
        "confidence_american_forces",  # Only Iraq
        "confidence_non_iraqi_television",  # Only Iraq
        "confidence_mainland_government",  # Only Hong Kong
        "confidence_free_commerce_treaty__tratado_de_libre_comercio",  # Only Mexico and Chile
        "confidence_united_american_states_organization",  # Only Peru and Dominican Republic
        "confidence_movimiento_en_pro_de_vieques__puerto_rico",  # Only Puerto Rico
        "confidence_education_system",  # most of the data is in 1993
        "confidence_social_security_system",  # most of the data is in 1993
        "confidence_andean_pact",  # Only Venezuela
        "confidence_eco",  # Only Iran
        "confidence_east_african_cooperation__eac",  # Only Tanzania
        "confidence_presidency",  # Only Algeria
        "confidence_local_regional_government",  # Only Argentina and Puerto Rico
        "confidence_civil_society_groups",  # Only Algeria
        "confidence_non_governmental_organizations__ngos",  # Only Argentina
        "confidence_religious_leaders",  # Only Jordan
        "confidence_tv_news",  # Only Argentina
        "confidence_evangelic_church",  # Only Peru
        "confidence_organization_of_american_states__oae",  # Only Peru
        "confidence_unasur",  # Only Colombia
        "confidence_undp_united_nations_development_programme",  # Only Egypt, Iraq and Lebanon
    ]
    tb_garden = tb_garden.drop(columns=vars_to_drop)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wvs_trust.end")
