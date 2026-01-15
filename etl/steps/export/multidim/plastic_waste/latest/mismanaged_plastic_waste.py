"""MDIM step for plastic waste emissions data."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
        tb["plas_em"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "all", "measure": "total"}
        tb["plas_em"].m.original_short_name = "emissions"
    if "plas_em_per_cap" in tb.columns:
        tb["plas_em_per_cap"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "all", "measure": "per_person"}
        tb["plas_em_per_cap"].m.original_short_name = "emissions"
    if "plas_em_share_global" in tb.columns:
        tb["plas_em_share_global"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "all", "measure": "share_of_global_total"}
        tb["plas_em_share_global"].m.original_short_name = "emissions"

    # Open burning - All sources
    if "plas_burn_em" in tb.columns:
        tb["plas_burn_em"].m.dimensions = {"emission_type": "open_burning", "emissions_source": "all", "measure": "total"}
        tb["plas_burn_em"].m.original_short_name = "emissions"
    if "plas_burn_em_per_cap" in tb.columns:
        tb["plas_burn_em_per_cap"].m.dimensions = {"emission_type": "open_burning", "emissions_source": "all", "measure": "per_person"}
        tb["plas_burn_em_per_cap"].m.original_short_name = "emissions"
    if "plas_burn_em_share_global" in tb.columns:
        tb["plas_burn_em_share_global"].m.dimensions = {"emission_type": "open_burning", "emissions_source": "all", "measure": "share_of_global_total"}
        tb["plas_burn_em_share_global"].m.original_short_name = "emissions"

    # Debris - All sources
    if "plas_debris_em" in tb.columns:
        tb["plas_debris_em"].m.dimensions = {"emission_type": "debris", "emissions_source": "all", "measure": "total"}
        tb["plas_debris_em"].m.original_short_name = "emissions"
    if "plas_debris_em_per_cap" in tb.columns:
        tb["plas_debris_em_per_cap"].m.dimensions = {"emission_type": "debris", "emissions_source": "all", "measure": "per_person"}
        tb["plas_debris_em_per_cap"].m.original_short_name = "emissions"
    if "plas_debris_em_share_global" in tb.columns:
        tb["plas_debris_em_share_global"].m.dimensions = {"emission_type": "debris", "emissions_source": "all", "measure": "share_of_global_total"}
        tb["plas_debris_em_share_global"].m.original_short_name = "emissions"

    # By specific sources (uncollected waste)
    if "plas_uncol_em" in tb.columns:
        tb["plas_uncol_em"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "uncollected_waste", "measure": "total"}
        tb["plas_uncol_em"].m.original_short_name = "emissions"
    if "plas_uncol_em_per_cap" in tb.columns:
        tb["plas_uncol_em_per_cap"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "uncollected_waste", "measure": "per_person"}
        tb["plas_uncol_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (litter)
    if "plas_litter_em" in tb.columns:
        tb["plas_litter_em"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "litter", "measure": "total"}
        tb["plas_litter_em"].m.original_short_name = "emissions"
    if "plas_litter_em_per_cap" in tb.columns:
        tb["plas_litter_em_per_cap"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "litter", "measure": "per_person"}
        tb["plas_litter_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (disposal)
    if "plas_disp_em" in tb.columns:
        tb["plas_disp_em"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "disposal", "measure": "total"}
        tb["plas_disp_em"].m.original_short_name = "emissions"
    if "plas_disp_em_per_cap" in tb.columns:
        tb["plas_disp_em_per_cap"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "disposal", "measure": "per_person"}
        tb["plas_disp_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (collection system)
    if "plas_collection_em" in tb.columns:
        tb["plas_collection_em"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "collection_system", "measure": "total"}
        tb["plas_collection_em"].m.original_short_name = "emissions"
    if "plas_collection_em_per_cap" in tb.columns:
        tb["plas_collection_em_per_cap"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "collection_system", "measure": "per_person"}
        tb["plas_collection_em_per_cap"].m.original_short_name = "emissions"

    # By specific sources (rejects/recycling)
    if "plas_recy_em" in tb.columns:
        tb["plas_recy_em"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "rejects", "measure": "total"}
        tb["plas_recy_em"].m.original_short_name = "emissions"
    if "plas_recy_em_per_cap" in tb.columns:
        tb["plas_recy_em_per_cap"].m.dimensions = {"emission_type": "total_burned_debris", "emissions_source": "rejects", "measure": "per_person"}
        tb["plas_recy_em_per_cap"].m.original_short_name = "emissions"

    # Common view configuration
    common_view_config = {
        "hasMapTab": True,
        "tab": "map",
    }

    # Create collection - this will automatically generate views from dimensions
    c = paths.create_collection(
        config=config,
        tb=tb,
        indicator_names=["emissions"],
        dimensions=["emission_type", "emissions_source", "measure"],
        common_view_config=common_view_config,
    )

    #
    # Save outputs.
    #
    c.save()
