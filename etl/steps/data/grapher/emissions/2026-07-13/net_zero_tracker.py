"""Load a garden dataset and create a grapher dataset.

The garden dataset holds the plain net-zero status and each country's real target year. Two adjustments
are made here purely for the map visualization:

- Embed each country's target year in the status value (e.g. "In law (2050)") so the map tooltip shows
  it. The chart's custom category labels and colours relabel every status-year value back to its plain
  status with a shared colour, and grapher collapses the identical legend entries into a single swatch.
  NOTE: the chart config lists each status-year value, so a target year appearing for the first time in
  a future update must be added there too.
- Flatten the time axis to a single year so every country sits at one map time point and the map shows
  no "data not available for <year>" tolerance notice.
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Status value for countries assessed as having no target (no year is embedded for these).
NO_TARGET_LABEL = "No target"


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("net_zero_tracker")
    tb = ds_garden["net_zero_tracker"].reset_index()

    #
    # Process data.
    #
    # Embed each country's target year in the status value so it shows in the map tooltip.
    status = tb["net_zero_status"]
    is_target = status != NO_TARGET_LABEL
    with_year = status.astype("string") + " (" + tb["year"].astype("Int64").astype("string") + ")"
    new_status = status.astype("string")
    new_status[is_target] = with_year[is_target]
    tb["net_zero_status"] = new_status.copy_metadata(status)

    # The plain-status ordinal ordering no longer applies once the year is embedded; the chart config
    # controls the legend order, so drop the ordinal metadata to satisfy grapher's ordinal validation.
    tb["net_zero_status"].metadata.sort = []
    tb["net_zero_status"].metadata.type = None

    # Flatten the time axis to the data's publication year so every country sits at one map time point.
    tb["year"] = int(status.metadata.origins[0].date_published[:4])

    #
    # Save outputs.
    #
    # Set an appropriate index and sort conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)
    ds_grapher.save()
