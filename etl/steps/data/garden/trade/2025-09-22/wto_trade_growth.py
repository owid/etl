"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets.
    ds_wto = paths.load_dataset("wto_trade_growth")
    ds_historic = paths.load_dataset("historic_trade")

    # Read tables from meadow datasets.
    tb_wto = ds_wto.read("wto_trade_growth")
    tb_historic = ds_historic.read("historic_trade")

    #
    # Process data.
    #

    # Filter historic_trade data up to 2000
    tb_historic_filtered = tb_historic[tb_historic["year"] <= 2000].copy()

    # Filter wto_trade_growth data from 2001 onwards
    tb_wto_filtered = tb_wto[tb_wto["year"] >= 2001].copy()

    # Find the best scaling factor to align WTO data with historic data
    # Find overlapping years between the two datasets
    historic_years = set(tb_historic["year"])
    wto_years = set(tb_wto["year"])
    overlapping_years = list(historic_years.intersection(wto_years))

    # Use the most recent overlapping year for the best alignment
    if overlapping_years:
        most_recent_year = max(overlapping_years)
        historic_value = tb_historic[tb_historic["year"] == most_recent_year]["volume_index"].iloc[0]
        wto_value = tb_wto[tb_wto["year"] == most_recent_year]["volume_index"].iloc[0]
        scaling_factor = historic_value / wto_value
    else:
        # Fallback: use 2000 from historic and earliest WTO year
        historic_value = tb_historic[tb_historic["year"] == 2000]["volume_index"].iloc[0]
        wto_value = tb_wto[tb_wto["year"] == tb_wto["year"].min()]["volume_index"].iloc[0]
        scaling_factor = historic_value / wto_value

    # Apply scaling factor to all WTO data
    tb_wto_filtered["volume_index"] = tb_wto_filtered["volume_index"] * scaling_factor

    # Combine the datasets
    tb_combined = pr.concat([tb_wto_filtered, tb_historic_filtered], ignore_index=True)
    # Improve table format.
    tb_combined = tb_combined.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined], default_metadata=ds_wto.metadata)

    # Save garden dataset.
    ds_garden.save()
