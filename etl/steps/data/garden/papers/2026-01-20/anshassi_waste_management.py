"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("anshassi_waste_management")
    tb = ds_meadow.read("anshassi_waste_management")

    #
    # Process data.
    #

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Fix UAE data error (collected and uncollected values were swapped by factor of 100) - classfied as level 2 country so can't possibly have such low values of collected waste and likely a decimal place error
    uae_mask = tb["country"] == "United Arab Emirates"
    tb.loc[uae_mask, "collected"] = tb.loc[uae_mask, "collected"] * 100
    tb.loc[uae_mask, "uncollected"] = tb.loc[uae_mask, "uncollected"] / 100

    # Check and fix floating-point rounding errors in percentages
    # Collected + uncollected should equal 100%
    total_collection = tb["collected"] + tb["uncollected"]
    if (total_collection > 100.001).any():
        raise ValueError("collected + uncollected significantly exceeds 100% for some countries")

    # Calculate total shares for each waste management category
    # Each subcategory is expressed as: (collected%/100 * collected_method%/100) + (uncollected%/100 * uncollected_method%/100)
    # This gives us the share of TOTAL waste (collected + uncollected) that goes to each method

    # Convert percentages to fractions for calculation
    collected_fraction = tb["collected"] / 100
    uncollected_fraction = tb["uncollected"] / 100

    # For collected methods: multiply collected fraction by the method's share of collected waste (also as fraction)
    # For uncollected methods: multiply uncollected fraction by the method's share of uncollected waste (also as fraction)

    # Open dump landfill
    tb["open_dump_landfill"] = (
        collected_fraction * tb["collected_open_dump_landfill"] + uncollected_fraction * tb["uncollected_open_dump"]
    )

    # Controlled landfill
    tb["controlled_landfill"] = (
        collected_fraction * tb["collected_controlled_landfill"]
        + uncollected_fraction * tb["uncollected_controlled_landfill"]
    )

    # Sanitary landfill
    tb["sanitary_landfill"] = (
        collected_fraction * tb["collected_sanitary_landfill"]
        + uncollected_fraction * tb["uncollected_sanitary_landfill"]
    )

    # Open air burning
    tb["open_air_burning"] = (
        collected_fraction * tb["collected_open_air_burning"]
        + uncollected_fraction * tb["uncollected_open_air_burning"]
    )

    # These categories only exist in the collected waste stream so to calculate the total share,
    # we only multiply the collected fraction by the method's share of collected waste.

    # MSWI Incineration (only in collected waste)
    tb["mswi_incineration"] = collected_fraction * tb["collected_mswi_incineration"]

    # Composting (only in collected waste)
    tb["composting"] = collected_fraction * tb["collected_composting"]

    # Recycling (only in collected waste)
    tb["recycling"] = collected_fraction * tb["collected_recycling"]

    tb["composition_composite"] = tb["composition_food_waste_organic"] + tb["composition_yard_garden_green_waste"]

    tb["share_compostable_waste_composted"] = (tb["composting"] / tb["composition_composite"]) * 100
    # Select final columns for output
    columns_to_keep = [
        "country",
        "final_classification",
        "collected",
        "uncollected",
        "open_dump_landfill",
        "controlled_landfill",
        "sanitary_landfill",
        "open_air_burning",
        "mswi_incineration",
        "composting",
        "recycling",
        "composition_composite",
        "share_compostable_waste_composted",
        "composition_food_waste_organic",
    ]

    tb = tb[columns_to_keep]

    #  Add year column based on the paper
    tb["year"] = 2020

    # Format table
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
