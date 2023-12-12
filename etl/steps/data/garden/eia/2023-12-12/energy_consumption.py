"""Garden step for EIA total energy consumption.

"""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Conversion factor from terajoules to terawatt-hours.
TJ_TO_TWH = 1 / 3600

# Columns to use from meadow table, and how to rename them.
COLUMNS = {"country": "country", "year": "year", "values": "energy_consumption"}

# Known overlaps between historical regions and successor countries.
# NOTE: They are not removed from the data when constructing the aggregate for Europe, but the contribution of Aruba is
# negligible, so the double-counting is not relevant.
KNOWN_OVERLAPS = [
    {
        1986: {"Aruba", "Netherlands Antilles"},
        1987: {"Aruba", "Netherlands Antilles"},
        1988: {"Aruba", "Netherlands Antilles"},
        1989: {"Aruba", "Netherlands Antilles"},
        1990: {"Aruba", "Netherlands Antilles"},
        1991: {"Aruba", "Netherlands Antilles"},
        1992: {"Aruba", "Netherlands Antilles"},
        1993: {"Aruba", "Netherlands Antilles"},
        1994: {"Aruba", "Netherlands Antilles"},
        1995: {"Aruba", "Netherlands Antilles"},
        1996: {"Aruba", "Netherlands Antilles"},
        1997: {"Aruba", "Netherlands Antilles"},
        1998: {"Aruba", "Netherlands Antilles"},
        1999: {"Aruba", "Netherlands Antilles"},
        2000: {"Aruba", "Netherlands Antilles"},
        2001: {"Aruba", "Netherlands Antilles"},
        2002: {"Aruba", "Netherlands Antilles"},
        2003: {"Aruba", "Netherlands Antilles"},
        2004: {"Aruba", "Netherlands Antilles"},
        2005: {"Aruba", "Netherlands Antilles"},
        2006: {"Aruba", "Netherlands Antilles"},
        2007: {"Aruba", "Netherlands Antilles"},
        2008: {"Aruba", "Netherlands Antilles"},
        2009: {"Aruba", "Netherlands Antilles"},
        2010: {"Aruba", "Netherlands Antilles"},
        2011: {"Aruba", "Netherlands Antilles"},
        2012: {"Aruba", "Netherlands Antilles"},
        2013: {"Aruba", "Netherlands Antilles"},
        2014: {"Aruba", "Netherlands Antilles"},
        2015: {"Aruba", "Netherlands Antilles"},
        2016: {"Aruba", "Netherlands Antilles"},
        2017: {"Aruba", "Netherlands Antilles"},
        2018: {"Aruba", "Netherlands Antilles"},
        2019: {"Aruba", "Netherlands Antilles"},
        2020: {"Aruba", "Netherlands Antilles"},
        2021: {"Aruba", "Netherlands Antilles"},
    }
]


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load EIA dataset and read its main table.
    ds_meadow = paths.load_dataset("energy_consumption")
    tb_meadow = ds_meadow["energy_consumption"].reset_index()

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb_meadow[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Convert terajoules to terawatt-hours.
    tb["energy_consumption"] *= TJ_TO_TWH

    # Create aggregate regions.
    tb = geo.add_regions_to_table(
        tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        ignore_overlaps_of_zeros=True,
        accepted_overlaps=KNOWN_OVERLAPS,
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
