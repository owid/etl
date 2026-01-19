"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("waste_management")
    tb = ds_meadow.read("waste_management")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb)

    # Calculate total shares for each waste management category
    # These combine both collected and uncollected fractions

    # Convert percentages to fractions for calculation
    collected_fraction = tb["collected"] / 100
    uncollected_fraction = tb["uncollected"] / 100

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

    # MSWI Incineration (only in collected fraction)
    tb["mswi_incineration"] = collected_fraction * tb["collected_mswi_incineration"]

    # Composting (only in collected fraction)
    tb["composting"] = collected_fraction * tb["collected_composting"]

    # Recycling (only in collected fraction)
    tb["recycling"] = collected_fraction * tb["collected_recycling"]

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
    ]

    tb = tb[columns_to_keep]

    # For now use 2020 but confirm with Hannah
    tb["year"] = 2020

    # Format table
    tb = tb.format(["country"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    ds_garden.save()
