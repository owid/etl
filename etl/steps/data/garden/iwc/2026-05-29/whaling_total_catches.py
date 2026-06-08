"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SUM_AGG = {
    "tblue": "sum",
    "pblue": "sum",
    "fin": "sum",
    "spm": "sum",
    "hbk": "sum",
    "sei": "sum",
    "bryd": "sum",
    "mi__c": "sum",
    "mi__a": "sum",
    "gray": "sum",
    "bhd": "sum",
    "ri": "sum",
    "unsp": "sum",
    "total": "sum",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("whaling_total_catches")

    # Read table from meadow dataset.
    tb = ds_meadow.read("whaling_total_catches")

    # replace type abbreviations with full names
    tb["ty"] = tb["ty"].replace(
        {
            "A": "Aboriginal subsistence catches",
            "Co": "Commercial under objection",
            "Cr": "Commercial under reservation",
            "C": "Commercial catches",
            "I": "Illegal catches",
            "S": "Special permit catches",
            "-": "No catch",
        }
    )
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Sum catches by country, year and type
    # catches columns: TBlue	PBlue	Fin	Spm	Hbk	Sei	Bryd	Mi_C	Mi_A	Gray	Bhd	Ri	Unsp	Total
    tb_type = tb.groupby(["country", "year", "ty"], as_index=False).agg(SUM_AGG)

    # rename columns

    tb_type = tb_type.rename(
        columns={
            "ty": "type",
            "tblue": "blue_whales_caught",
            "pblue": "pygmy_blue_whales_caught",
            "fin": "fin_whales_caught",
            "spm": "sperm_whales_caught",
            "hbk": "humpback_whales_caught",
            "sei": "sei_whales_caught",
            "bryd": "brydes_whales_caught",
            "mi__c": "common_minke_whales_caught",
            "mi__a": "antarctic_minke_whales_caught",
            "gray": "gray_whales_caught",
            "bhd": "bowhead_whales_caught",
            "ri": "right_whales_caught",
            "unsp": "unspecified_large_whales_caught",
            "total": "total_whales_caught",
        }
    )

    # Sum catches by country and year for totals
    tb_total = tb.groupby(["country", "year"], as_index=False).agg(SUM_AGG)

    # rename columns
    tb_total = tb_total.rename(
        columns={
            "tblue": "blue_whales_caught_total",
            "pblue": "pygmy_blue_whales_caught_total",
            "fin": "fin_whales_caught_total",
            "spm": "sperm_whales_caught_total",
            "hbk": "humpback_whales_caught_total",
            "sei": "sei_whales_caught_total",
            "bryd": "brydes_whales_caught_total",
            "mi__c": "common_minke_whales_caught_total",
            "mi__a": "antarctic_minke_whales_caught_total",
            "gray": "gray_whales_caught_total",
            "bhd": "bowhead_whales_caught_total",
            "ri": "right_whales_caught_total",
            "unsp": "unspecified_large_whales_caught_total",
            "total": "total_whales_caught_total",
        }
    )

    # calculate totals for world
    tb_world = tb_total.groupby(["year"], as_index=False).agg(
        {
            "blue_whales_caught_total": "sum",
            "pygmy_blue_whales_caught_total": "sum",
            "fin_whales_caught_total": "sum",
            "sperm_whales_caught_total": "sum",
            "humpback_whales_caught_total": "sum",
            "sei_whales_caught_total": "sum",
            "brydes_whales_caught_total": "sum",
            "common_minke_whales_caught_total": "sum",
            "antarctic_minke_whales_caught_total": "sum",
            "gray_whales_caught_total": "sum",
            "bowhead_whales_caught_total": "sum",
            "right_whales_caught_total": "sum",
            "unspecified_large_whales_caught_total": "sum",
            "total_whales_caught_total": "sum",
        }
    )
    tb_world["country"] = "World"
    tb_total = pr.concat([tb_total, tb_world], ignore_index=True)

    # Improve table format.
    tb_type = tb_type.format(["country", "year", "type"], short_name="whaling_total_catches_type")
    tb_total = tb_total.format(["country", "year"], short_name="whaling_total_catches")

    tables = [tb_type, tb_total]
    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
