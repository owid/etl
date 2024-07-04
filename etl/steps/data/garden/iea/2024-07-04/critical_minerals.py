"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# TODO: Add to metadata the notes extracted from the total demand sheet:
# Notes: Lithium demand is in lithium (Li) content, not carbonate equivalent (LCE). Demand for magnet rare earth elements covers praseodymium (Pr), neodymium (Nd), terbium (Tb) and dysprosium (Dy). Graphite demand  includes all grades of mined and synthetic graphite.
# TODO: Add to metadata the notes extracted from the supply sheet:
# Notes: Supply projections for the key energy transition minerals are built using the data for the pipeline of operating and announced mining and refining projects by country.
# “Base case” is assessed through their probability of coming online based on various factors such as the status of financing, permitting and feasibility studies.


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("critical_minerals")

    # Read table from meadow dataset.
    tb = ds_meadow["critical_minerals"].reset_index()

    #
    # Process data.
    #
    # The only sheet on supply (also, the only sheet with country data) is 2.
    # It seems that the sheets 4.1, ..., 4.6 contain all relevant data on demand.
    # Then, sheet 3.2 seems to be a summary of the 4.X sheets.
    # TODO: Check that all data in 3.2 is contained in 4.X.
    # Sheet 3.1 is also a summary of the 4.X sheets. However, it seems to contain data for at least two additional technology aggregates, namely "Low emissions power generation", and "Other low emissions power generation".
    # Finally, sheet 1 seems to be very similar to 3.1 (therefore also a summary of the 4.X sheets) with some additional derived indicators (like totals and shares).
    # Therefore, it may suffice to combine all 4.X sheets on demand with sheet 2 on supply.
    # But we may possibly need to include also the "*low emissions*" indicators from sheet 3.1.
    # NOTE: Aggregates will not be possible for supply data, since there is data for "Rest of world" for each mineral.

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
