"""Garden step for UN World Urbanization Prospects (National Definitions)."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wup_national_definitions")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("wup_urban_rural_population")

    #
    # Process data.
    #

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # After harmonization, some countries have duplicates (different source names map to same target)
    # Verify that duplicate values are identical before dropping
    duplicates = tb[tb.duplicated(subset=["country", "year", "area_type"], keep=False)]

    if len(duplicates) > 0:
        value_cols = ["population", "share", "growth_rate", "share_growth_rate"]
        # Check that all duplicates have identical values
        for col in value_cols:
            max_unique = duplicates.groupby(["country", "year", "area_type"])[col].nunique().max()
            if max_unique > 1:
                raise ValueError(f"Duplicate rows have different values in column '{col}'!")

    # Drop duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "area_type"], keep="first")
    tb = tb.drop(columns="loc_id")

    # Set index
    tb = tb.format(["country", "year", "area_type"])

    #
    # Save outputs.
    #
    # Create a new garden dataset
    ds_garden = paths.create_dataset(tables=[tb])

    # Save changes in the new garden dataset.
    ds_garden.save()
