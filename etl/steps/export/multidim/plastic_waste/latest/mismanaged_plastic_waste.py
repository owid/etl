"""MDIM step for plastic waste emissions data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Common configuration for all charts
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "chartTypes": ["StackedDiscreteBar"],
}

# Configuration for stacked bar charts
STACKED_VIEW_CONFIG = {
    "hasMapTab": False,
    "tab": "chart",
    "chartTypes": ["StackedDiscreteBar"],
}

# Define dimensions for all variables
DIMENSIONS_DETAILS = {
    # Total emissions - All sources
    "plas_em": {
        "emission_type": "total_burned_debris",
        "emissions_source": "all",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_em_per_cap": {
        "emission_type": "total_burned_debris",
        "emissions_source": "all",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    "plas_em_share_global": {
        "emission_type": "total_burned_debris",
        "emissions_source": "all",
        "measure": "share_of_global_total",
        "original_short_name": "emissions",
    },
    # Open burning - All sources
    "plas_burn_em": {
        "emission_type": "open_burning",
        "emissions_source": "all",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_burn_em_per_cap": {
        "emission_type": "open_burning",
        "emissions_source": "all",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    "plas_burn_em_share_global": {
        "emission_type": "open_burning",
        "emissions_source": "all",
        "measure": "share_of_global_total",
        "original_short_name": "emissions",
    },
    # Debris - All sources
    "plas_debris_em": {
        "emission_type": "debris",
        "emissions_source": "all",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_debris_em_per_cap": {
        "emission_type": "debris",
        "emissions_source": "all",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    "plas_debris_em_share_global": {
        "emission_type": "debris",
        "emissions_source": "all",
        "measure": "share_of_global_total",
        "original_short_name": "emissions",
    },
    # By specific sources (uncollected waste)
    "plas_uncol_em": {
        "emission_type": "total_burned_debris",
        "emissions_source": "uncollected_waste",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_uncol_em_per_cap": {
        "emission_type": "total_burned_debris",
        "emissions_source": "uncollected_waste",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    # By specific sources (litter)
    "plas_litter_em": {
        "emission_type": "total_burned_debris",
        "emissions_source": "litter",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_litter_em_per_cap": {
        "emission_type": "total_burned_debris",
        "emissions_source": "litter",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    # By specific sources (disposal)
    "plas_disp_em": {
        "emission_type": "total_burned_debris",
        "emissions_source": "disposal",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_disp_em_per_cap": {
        "emission_type": "total_burned_debris",
        "emissions_source": "disposal",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    # By specific sources (collection system)
    "plas_collection_em": {
        "emission_type": "total_burned_debris",
        "emissions_source": "collection_system",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_collection_em_per_cap": {
        "emission_type": "total_burned_debris",
        "emissions_source": "collection_system",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
    # By specific sources (rejects/recycling)
    "plas_recy_em": {
        "emission_type": "total_burned_debris",
        "emissions_source": "rejects",
        "measure": "total",
        "original_short_name": "emissions",
    },
    "plas_recy_em_per_cap": {
        "emission_type": "total_burned_debris",
        "emissions_source": "rejects",
        "measure": "per_person",
        "original_short_name": "emissions",
    },
}


def generate_title_by_type(view):
    """Generate title based on emission type breakdown and measure."""
    measure = view.dimensions.get("measure")
    source = view.dimensions.get("emissions_source")

    # Map measure to title text
    measure_text = {
        "total": "Plastic waste pollution by type",
        "per_person": "Plastic waste pollution per person by type",
    }.get(measure, "Plastic waste pollution by type")

    # Add source context if not "all"
    if source and source != "all":
        source_names = {
            "uncollected_waste": "from uncollected waste",
            "litter": "from litter",
            "disposal": "from disposal sites",
            "collection_system": "from collection system",
            "rejects": "from recycling rejects",
        }
        source_text = source_names.get(source, "")
        if source_text:
            measure_text += f" {source_text}"

    return measure_text


def generate_subtitle_by_type(view):
    """Generate subtitle based on measure."""
    if view.matches(measure="total"):
        return "Breakdown of the estimated total amount of plastic waste released to the environment each year through debris and open burning from municipal sources such as households, shops, and offices."
    elif view.matches(measure="per_person"):
        return "Breakdown of the estimated total amount of plastic waste released to the environment per person each year through debris and open burning from municipal sources such as households, shops, and offices."
    else:
        return "Breakdown of the estimated total amount of plastic waste released to the environment through debris and open burning from municipal sources such as households, shops, and offices."


def run() -> None:
    """
    Main function to process plastic waste emissions data and create multidimensional data views.
    """
    #
    # Load inputs.
    #
    # Load configuration from adjacent yaml file
    config = paths.load_collection_config()

    # Load grapher dataset
    ds_grapher = paths.load_dataset("cottom_plastic_waste")
    tb = ds_grapher.read("cottom_plastic_waste", reset_index=False)

    #
    # Process data.
    #
    # Add dimension metadata to columns based on their indicator type
    for col, details in DIMENSIONS_DETAILS.items():
        if col in tb.columns:
            # Set dimensions (all keys except original_short_name)
            tb[col].m.dimensions = {k: v for k, v in details.items() if k != "original_short_name"}
            # Set original_short_name if present
            if "original_short_name" in details:
                tb[col].m.original_short_name = details["original_short_name"]

    # Create collection - this will automatically generate views from dimensions
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["emissions"],
        dimensions=["emission_type", "emissions_source", "measure"],
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add grouped stacked bar views for "Total (by type)" - breakdown by burning vs debris
    view_metadata = {
        "presentation": {
            "title_public": "{title}",
        },
        "description_short": "{subtitle}",
        "description_key": [
            "Plastic pollution is plastic that is no longer contained because it escapes from collection, disposal, or recycling and enters the environment.",
            "Total plastic pollution is the sum of debris (unburned plastic that escapes into the environment as physical items) and plastic burned in open, uncontrolled fires.",
            "The data covers macroplastics — physical plastic pieces larger than 5 millimeters.",
            "Plastic pollution is attributed to five land-based sources: uncollected waste, littering, losses during collection and transport, uncontrolled disposal sites (open dumps), and rejects from sorting and reprocessing.",
            "This data covers plastic that comes from land-based municipal solid waste (everyday waste from households and similar sources). It does not include pollution from making plastic, textiles, sea-based sources (like fishing gear), electronic waste, or plastic that is exported as waste and then lost elsewhere.",
            "Values are model-based estimates and come with uncertainty. They should be interpreted as approximate estimates rather than exact measurements.",
        ],
    }

    view_config = STACKED_VIEW_CONFIG | {
        "title": "{title}",
        "subtitle": "{subtitle}",
    }

    c.group_views(
        groups=[
            {
                "dimension": "emission_type",
                "choice_new_slug": "total_by_type",
                "choices": ["open_burning", "debris"],
                "view_config": view_config,
                "view_metadata": view_metadata,
            },
        ],
        params={
            "title": lambda view: generate_title_by_type(view),
            "subtitle": lambda view: generate_subtitle_by_type(view),
        },
    )

    # Remove "Total by type" views for share_of_global_total measure
    # (stacked bar doesn't make sense for percentage shares)
    c.views = [
        view
        for view in c.views
        if not (
            view.dimensions.get("emission_type") == "total_by_type"
            and view.dimensions.get("measure") == "share_of_global_total"
        )
    ]

    #
    # Save outputs.
    #
    c.save()
