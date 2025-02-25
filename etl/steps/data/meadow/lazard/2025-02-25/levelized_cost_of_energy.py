"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("levelized_cost_of_energy.csv")

    #
    # Process data.
    #
    # The data was manually extracted from the PDF.
    # TODO: Double-check all data points.
    df = pd.DataFrame(
        {
            "year": [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2023, 2024],
            "lcoe_nuclear": [123, 96, 95, 96, 104, 112, 117, 117, 148, 151, 155, 163, 167, 180, 182],
            "lcoe_gas_peaking": [275, 243, 227, 216, 205, 205, 192, 191, 183, 179, 175, 175, 173, 168, 169],
            "lcoe_coal": [111, 111, 111, 102, 105, 112, 108, 102, 102, 102, 109, 112, 108, 117, 118],
            "lcoe_geothermal": [76, 107, 104, 116, 116, 116, 100, 98, 97, 91, 91, 80, 75, 82, 85],
            "lcoe_gas_combined_cycle": [83, 82, 83, 75, 74, 74, 64, 63, 60, 58, 56, 59, 60, 70, 76],
            "lcoe_solar_pv": [359, 248, 157, 125, 98, 79, 64, 55, 50, 43, 40, 37, 36, 60, 61],
            "lcoe_wind_onshore": [135, 124, 71, 72, 70, 59, 55, 47, 45, 42, 41, 40, 38, 50, 50],
        },
    )
    tb = pr.read_df(df=df, metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["year"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
