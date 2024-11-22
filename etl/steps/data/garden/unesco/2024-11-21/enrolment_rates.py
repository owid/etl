"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("enrolment_rates")

    # Read table from meadow dataset.
    tb = ds_meadow["enrolment_rates"].reset_index()

    # Retrieve snapshot with the metadata provided via World Bank.

    snap_wb = paths.load_snapshot("edstats_metadata.xls")
    tb_wb = snap_wb.read()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add the long description from the World Bank metadata
    long_definition = {}
    for indicator in tb["indicator"].unique():
        definition = tb_wb[tb_wb["Indicator Name"] == indicator]["Long definition"].values
        if len(definition) > 0:
            long_definition[indicator] = definition[0]
        else:
            long_definition[indicator] = ""

    tb["long_description"] = tb["indicator"].map(long_definition)

    # Pivot the table to have the indicators as columns to add descriptions from producer
    tb_pivoted = tb.pivot(index=["country", "year"], columns="indicator", values="value")
    for column in tb_pivoted.columns:
        meta = tb_pivoted[column].metadata
        long_definition = tb["long_description"].loc[tb["indicator"] == column].iloc[0]
        meta.description_from_producer = long_definition
        meta.title = column
        meta.display = {}

        meta.display["numDecimalPlaces"] = 1
        meta.unit = "%"
        meta.short_unit = "%"

    tb_pivoted = tb_pivoted.reset_index()
    tb_pivoted = tb_pivoted.format(["country", "year"])

    # Drop columns that are not needed
    tb_pivoted = tb_pivoted.drop(
        columns=[
            "total_net_enrolment_rate__lower_secondary__adjusted_gender_parity_index__gpia",
            "total_net_enrolment_rate__primary__adjusted_gender_parity_index__gpia",
            "total_net_enrolment_rate__upper_secondary__adjusted_gender_parity_index__gpia",
        ]
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_pivoted], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
