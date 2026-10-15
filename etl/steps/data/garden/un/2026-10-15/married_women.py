"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Countries, areas and regional groupings we keep from the 2024 edition (April 2025 revision).
# NOTE: Re-check this count if the producer adds or removes countries or regional groupings.
N_ENTITIES = 240


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("married_women")

    # Read table from meadow dataset.
    tb = ds_meadow.read("married_women")

    #
    # Process data.
    #
    # Step 1: Harmonize country names.
    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Step 2: Keep only the shares; we don't use the number of women.
    tb = tb.drop(columns=["number_married_union"])

    sanity_check_outputs(tb)

    # dataprocess specifies whether the data is an estimate or a projection.
    tb = tb.format(["country", "year", "agegroup", "dataprocess"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_outputs(tb) -> None:
    # Shares are percentages, with no gaps.
    assert tb["percentage_married_union"].notna().all(), "Missing shares."
    assert tb["percentage_married_union"].between(0, 100).all(), "Shares outside [0, 100]."
    # Coverage didn't shrink.
    n_entities = tb["country"].nunique()
    assert n_entities >= N_ENTITIES, f"Only {n_entities} entities, expected at least {N_ENTITIES}."
