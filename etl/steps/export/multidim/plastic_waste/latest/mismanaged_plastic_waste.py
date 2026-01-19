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


def generate_title_by_type(view):
    """Generate title based on emission type breakdown and measure."""
    measure = view.dimensions.get("measure")
    source = view.dimensions.get("emissions_source")

    # Map measure to title text
    measure_text = {
        "total": "Plastic waste emissions by type",
        "per_person": "Plastic waste emissions per person by type",
    }.get(measure, "Plastic waste emissions by type")

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
    measure = view.dimensions.get("measure")

    if measure == "total":
        return "Breakdown of macroplastic emissions by open burning and debris, measured in tonnes per year."
    elif measure == "per_person":
        return (
            "Breakdown of macroplastic emissions by open burning and debris, measured in kilograms per person per year."
        )
    else:
        return "Breakdown of macroplastic emissions by open burning and debris."


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
    # Mapping structure: (emission_type, emissions_source, measure) -> indicator_name

    # Total emissions (burned + debris) - All sources
    if "plas_em" in tb.columns:
        tb["plas_em"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "all",
            "measure": "total",
        }
        tb["plas_em"].m.original_short_name = "emissions"
    if "plas_em_per_cap" in tb.columns:
        tb["plas_em_per_cap"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "all",
            "measure": "per_person",
        }
        tb["plas_em_per_cap"].m.original_short_name = "emissions"
    if "plas_em_share_global" in tb.columns:
        tb["plas_em_share_global"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "all",
            "measure": "share_of_global_total",
        }
        tb["plas_em_share_global"].m.original_short_name = "emissions"

    # Open burning - All sources
    if "plas_burn_em" in tb.columns:
        tb["plas_burn_em"].m.dimensions = {
            "emission_type": "open_burning",
            "emissions_source": "all",
            "measure": "total",
        }
        tb["plas_burn_em"].m.original_short_name = "emissions"
    if "plas_burn_em_per_cap" in tb.columns:
        tb["plas_burn_em_per_cap"].m.dimensions = {
            "emission_type": "open_burning",
            "emissions_source": "all",
            "measure": "per_person",
        }
        tb["plas_burn_em_per_cap"].m.original_short_name = "emissions"
    if "plas_burn_em_share_global" in tb.columns:
        tb["plas_burn_em_share_global"].m.dimensions = {
            "emission_type": "open_burning",
            "emissions_source": "all",
            "measure": "share_of_global_total",
        }
        tb["plas_burn_em_share_global"].m.original_short_name = "emissions"

    # Debris - All sources
    if "plas_debris_em" in tb.columns:
        tb["plas_debris_em"].m.dimensions = {"emission_type": "debris", "emissions_source": "all", "measure": "total"}
        tb["plas_debris_em"].m.original_short_name = "emissions"
    if "plas_debris_em_per_cap" in tb.columns:
        tb["plas_debris_em_per_cap"].m.dimensions = {
            "emission_type": "debris",
            "emissions_source": "all",
            "measure": "per_person",
        }
        tb["plas_debris_em_per_cap"].m.original_short_name = "emissions"
    if "plas_debris_em_share_global" in tb.columns:
        tb["plas_debris_em_share_global"].m.dimensions = {
            "emission_type": "debris",
            "emissions_source": "all",
            "measure": "share_of_global_total",
        }
        tb["plas_debris_em_share_global"].m.original_short_name = "emissions"

    # By specific sources (uncollected waste)
    if "plas_uncol_em" in tb.columns:
        tb["plas_uncol_em"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "uncollected_waste",
            "measure": "total",
        }
        tb["plas_uncol_em"].m.original_short_name = "emissions"
    if "plas_uncol_em_per_cap" in tb.columns:
        tb["plas_uncol_em_per_cap"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "uncollected_waste",
            "measure": "per_person",
        }
        tb["plas_uncol_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (litter)
    if "plas_litter_em" in tb.columns:
        tb["plas_litter_em"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "litter",
            "measure": "total",
        }
        tb["plas_litter_em"].m.original_short_name = "emissions"
    if "plas_litter_em_per_cap" in tb.columns:
        tb["plas_litter_em_per_cap"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "litter",
            "measure": "per_person",
        }
        tb["plas_litter_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (disposal)
    if "plas_disp_em" in tb.columns:
        tb["plas_disp_em"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "disposal",
            "measure": "total",
        }
        tb["plas_disp_em"].m.original_short_name = "emissions"
    if "plas_disp_em_per_cap" in tb.columns:
        tb["plas_disp_em_per_cap"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "disposal",
            "measure": "per_person",
        }
        tb["plas_disp_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (collection system)
    if "plas_collection_em" in tb.columns:
        tb["plas_collection_em"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "collection_system",
            "measure": "total",
        }
        tb["plas_collection_em"].m.original_short_name = "emissions"
    if "plas_collection_em_per_cap" in tb.columns:
        tb["plas_collection_em_per_cap"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "collection_system",
            "measure": "per_person",
        }
        tb["plas_collection_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (rejects/recycling)
    if "plas_recy_em" in tb.columns:
        tb["plas_recy_em"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "rejects",
            "measure": "total",
        }
        tb["plas_recy_em"].m.original_short_name = "emissions"
    if "plas_recy_em_per_cap" in tb.columns:
        tb["plas_recy_em_per_cap"].m.dimensions = {
            "emission_type": "total_burned_debris",
            "emissions_source": "rejects",
            "measure": "per_person",
        }
        tb["plas_recy_em_per_cap"].m.original_short_name = "emissions"

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
            "These emissions refer to macroplastic, meaning physical plastic pieces larger than 5 millimetres.",
            "Total plastic emissions are the sum of debris (unburned plastic that escapes into the environment as physical items) and plastic burned in open, uncontrolled fires.",
            "Emissions are plastic that is no longer contained because it escapes from collection, disposal, or recycling and enters the environment.",
            "Emissions are attributed to five land-based sources: uncollected waste, littering, losses during collection and transport, uncontrolled disposal sites (open dumps), and rejects from sorting and reprocessing.",
            "This data covers plastic that comes from land-based municipal solid waste (everyday waste from households and similar sources). It does not include emissions from making plastic, textiles, sea-based sources (like fishing gear), electronic waste, or plastic that is exported as waste and then lost elsewhere.",
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
