"""Load a snapshot and create a meadow dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("technology_specific_cost_and_performance_parameters.pdf")

    #
    # Process data.
    #
    # The data was manually extracted from Table A.III.2 | Emissions of selected electricity supply technologies (gCO2eq/kWh).
    # Specifically, we will simply extract the direct combustion emissions factors.
    tb = pr.read_from_dict(
        {
            "Coal": {"min": 670, "median": 760, "max": 870},
            "Gas": {"min": 350, "median": 370, "max": 490},
        },
        origin=snap.metadata.origin,
        metadata=snap.to_table_metadata(),
    )
    # Add data for biomass (which is nan).
    tb["Biomass-cofiring"] = np.nan
    tb["Biomass-dedicated"] = np.nan
    # Add data for renewables (which is zero).
    for column in [
        "Geothermal",
        "Hydropower",
        "Nuclear",
        "Concentrated Solar Power",
        "Solar PV—rooftop",
        "Solar PV—utility",
        "Wind onshore",
        "Wind offshore",
    ]:
        tb[column] = 0

    # Transpose to recover the shape of the original table in the annex.
    tb = tb.transpose()

    # Rename conveniently.
    tb = tb.rename(
        columns={level: f"{level}_direct_combustion_emission_factor" for level in tb.columns}, errors="raise"
    )

    # Add metadata.
    for column in tb.columns:
        tb[column].metadata.origins = [snap.metadata.origin]
    tb.index.name = "Source"

    # Improve table format.
    tb = tb.reset_index().format(keys=["source"], sort_rows=False, short_name="electricity_emission_factors")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
