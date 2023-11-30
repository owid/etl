"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("integrated_values_survey")

    # Read table from meadow dataset.
    tb = ds_meadow["integrated_values_survey"].reset_index()

    #
    # Process data.

    # Drop columns
    tb = drop_indicators(tb)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_indicators(tb: Table) -> Table:
    """
    Drop indicators/questions not useful enough for OWID's purposes (too few data points, for too few countries, etc.)
    """

    # Drop selected variables
    vars_to_drop = [
        "trust_first_not_very_much",
        "trust_personally_not_very_much",
        "confidence_confidence_in_cer_with_australia",  # Only New Zealand
        "confidence_free_commerce_treaty__tratado_de_libre_comercio",  # Only Mexico and Chile
        "confidence_united_american_states_organization",  # Only Peru and Dominican Republic
        "confidence_education_system",  # most of the data is in 1993
        "confidence_social_security_system",  # most of the data is in 1993
        "confidence_andean_pact",  # Only Venezuela
        "confidence_local_regional_government",  # Only Argentina and Puerto Rico
        "confidence_organization_of_american_states__oae",  # Only Peru
    ]
    tb = tb.drop(columns=vars_to_drop)

    return tb
