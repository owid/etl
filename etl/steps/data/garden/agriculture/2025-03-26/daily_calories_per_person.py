"""Historical daily calorie supply per person, based on a combination of sources.

See description_processing (in the adjacent metadata file) for more details on the choices below.

"""

from typing import Union

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# FAOSTAT element code for "Food available for consumption" measured in "kilocalories per day per capita"
# (corresponding to original FAOSTAT element "Food supply (kcal/capita/day)").
ELEMENT_CODE_FOOD_SUPPLY_PER_CAPITA = "0664pc"
# FAOSTAT item code for "Total" (corresponding to original FAOSTAT item "Grand Total").
ITEM_CODE_TOTAL = "00002901"


def correct_year(year: Union[str, int], verbose: bool = False) -> int:
    # Correct the value of a year, to ensure it is an integer, and, when given a range, take the average value.
    year_str = str(year)
    if len(year_str) == 4:
        # Normal format, e.g. "1990".
        year_corrected = int(year_str)
    elif len(year_str) == 9:
        # Range format, e.g. "1990-1999" or "1845/1854".
        year_start, year_end = year_str[0:4], year_str[5:9]
        year_corrected = int((int(year_start) + int(year_end)) / 2)
    elif len(year_str) == 7:
        # Range format, but second year is incomplete, e.g. "1845-54".
        year_start, year_end = year_str[0:4], year_str[0:2] + year_str[5:7]
        year_corrected = int((int(year_start) + int(year_end)) / 2)
    else:
        raise ValueError(f"Unexpected year format: {year}")

    # As a sanity check, optionally print the correction.
    if verbose and (str(year_corrected) != year_str):
        print(f'Corrected "{year}" -> "{year_corrected}"')

    return year_corrected


def correct_data_values(tb: Table) -> Table:
    # Some data values are given as ranges, e.g. "1234-1345".
    # Ensure all values are real numbers, and take the average value when a range is given.
    select_ranges = pd.to_numeric(tb["daily_calories"], errors="coerce").isnull()
    tb.loc[select_ranges, "daily_calories"] = [
        (float(value.split("-")[0]) + float(value.split("-")[1])) / 2
        for value in tb[select_ranges]["daily_calories"].values
    ]
    tb = tb.astype({"daily_calories": float})

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT FBSC dataset and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # Load Harris et al. (2015) dataset and read its main table.
    ds_harris = paths.load_dataset("harris_et_al_2015")
    tb_harris = ds_harris.read("harris_et_al_2015")

    # Load Floud et al. (2011) dataset and read its main table.
    ds_floud = paths.load_dataset("floud_et_al_2011")
    tb_floud = ds_floud.read("floud_et_al_2011")

    # Load Jonsson (1998) dataset and read its main table.
    ds_jonsson = paths.load_dataset("jonsson_1998")
    tb_jonsson = ds_jonsson.read("jonsson_1998")

    # Load Grigg (1995) dataset and read its main table.
    ds_grigg = paths.load_dataset("grigg_1995")
    tb_grigg = ds_grigg.read("grigg_1995")

    # Load Fogel (2004) dataset and read its main table.
    ds_fogel = paths.load_dataset("fogel_2004")
    tb_fogel = ds_fogel.read("fogel_2004")

    # Load FAO (2000) dataset and read its main table.
    ds_fao2000 = paths.load_dataset("fao_2000")
    tb_fao2000 = ds_fao2000.read("fao_2000")

    # Load FAO (1949) dataset and read its main table.
    ds_fao1949 = paths.load_dataset("fao_1949")
    tb_fao1949 = ds_fao1949.read("fao_1949")

    # Load USDA/ERS data on food availability.
    ds_usda = paths.load_dataset("food_availability")
    tb_usda = ds_usda.read("food_availability")

    #
    # Process data.
    #
    # Prepare FAOSTAT data.
    tb_fbsc = (
        tb_fbsc[
            (tb_fbsc["element_code"] == ELEMENT_CODE_FOOD_SUPPLY_PER_CAPITA) & (tb_fbsc["item_code"] == ITEM_CODE_TOTAL)
        ][["country", "year", "value"]]
        .rename(columns={"value": "daily_calories"})
        .reset_index(drop=True)
    )

    # Ensure the "source" column in Harris et al. (2015) is different from the sources in other tables.
    tb_harris["source"] = tb_harris["source"].astype(str) + " via Harris et al. (2015)"

    # Concatenate all tables and add a source column.
    tb = pr.concat(
        [
            tb_fbsc.assign(**{"source": "FAOSTAT"}),
            tb_harris,
            tb_floud.assign(**{"source": "Floud et al. (2011)"}),
            tb_jonsson.assign(**{"source": "Jonsson (1998)"}),
            tb_grigg.assign(**{"source": "Grigg (1995)"}),
            tb_fogel.assign(**{"source": "Fogel (2004)"}),
            tb_fao2000.assign(**{"source": "FAO (2000)"}),
            tb_fao1949.assign(**{"source": "FAO (1949)"}),
            tb_usda.assign(**{"source": "USDA/ERS"}),
        ],
        ignore_index=True,
    )

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, warn_on_missing_countries=True, warn_on_unused_countries=True
    )

    # Ensure years are integers. When given a range of years, take the middle year.
    tb["year"] = tb["year"].apply(correct_year)

    # Sanity checks.
    assert tb[tb["country"].isnull()].empty, "Some countries are missing."
    assert tb[tb["year"].isnull()].empty, "Some years are missing."

    # Drop rows with no data on daily calories.
    tb = tb.dropna(subset=["daily_calories"]).reset_index(drop=True)

    # Some numbers are given as ranges, e.g. "2914-2949". Take the average value.
    tb = correct_data_values(tb=tb)

    # Start a new table with the selection of all countries from different sources.
    # * Most countries have data only from FAOSTAT, or from FAOSTAT + FAO (1949), or from FAOSTAT + FAO (2000).
    #   There is no overlap among them (i.e. there is only one value for each year).
    #   Although there are some abrupt jumps between FAO (1949) and FAOSTAT (which we will accept).
    #   We take all these countries.
    countries_selected = [
        country for country in tb["country"].unique() if len(set(tb[tb["country"] == country]["source"])) < 3
    ]
    tb_selected = tb[tb["country"].isin(countries_selected)].reset_index(drop=True)

    # * Belgium:
    tb_belgium = tb[
        (tb["country"] == "Belgium") & (tb["source"].isin(["FAOSTAT", "FAO (1949)", "Floud et al. (2011)"]))
    ]

    # * Brazil:
    tb_brazil = tb[(tb["country"] == "Brazil") & (tb["source"].isin(["FAOSTAT", "FAO (2000)"]))]

    # * Finland:
    tb_finland = tb[(tb["country"] == "Finland") & (tb["source"].isin(["FAOSTAT", "Grigg (1995)", "FAO (1949)"]))]

    # * France:
    tb_france = pr.concat(
        [
            # Prior to 1800, the only data (two points) comes from Fogel (2004).
            tb[(tb["country"] == "France") & (tb["year"] < 1800) & (tb["source"] == "Fogel (2004)")],
            # After 1800, take data from Grigg (1995), FAOSTAT, and FAO (1949).
            tb[
                (tb["country"] == "France")
                & (tb["year"] >= 1800)
                & (tb["source"].isin(["FAOSTAT", "FAO (1949)", "Grigg (1995)"]))
            ],
        ],
        ignore_index=True,
    )

    # * Germany:
    tb_germany = tb[(tb["country"] == "Germany") & (tb["source"].isin(["FAOSTAT", "Grigg (1995)"]))]

    # * Iceland:
    tb_iceland = tb[(tb["country"] == "Iceland") & (tb["source"].isin(["FAOSTAT", "Jonsson (1998)", "FAO (1949)"]))]

    # * India:
    tb_india = tb[(tb["country"] == "India") & (tb["source"].isin(["FAOSTAT", "FAO (2000)", "FAO (1949)"]))]

    # * Italy:
    tb_italy = tb[(tb["country"] == "Italy") & (tb["source"].isin(["FAOSTAT", "Grigg (1995)", "FAO (1949)"]))]

    # * Netherlands:
    tb_netherlands = tb[
        (tb["country"] == "Netherlands") & (tb["source"].isin(["FAOSTAT", "Floud et al. (2011)", "FAO (1949)"]))
    ]

    # * Norway:
    tb_norway = tb[(tb["country"] == "Norway") & (tb["source"].isin(["FAOSTAT", "Grigg (1995)", "FAO (1949)"]))]

    # * Mexico:
    #   There is an overlap on year 1947 between FAO (1949) and FAO (2000) (with similar values).
    #   Remove the overlapping point from FAO (1949).
    tb_mexico = tb[
        (tb["country"] == "Mexico")
        & (tb["source"].isin(["FAOSTAT", "FAO (2000)", "FAO (1949)"]))
        & ~((tb["year"] == 1947) & (tb["source"] == "FAO (1949)"))
    ]

    # * Peru:
    #   There is an overlap on year 1947 between FAO (1949) and FAO (2000) (with similar values).
    #   Remove the overlapping point from FAO (1949).
    tb_peru = tb[
        (tb["country"] == "Peru")
        & (tb["source"].isin(["FAOSTAT", "FAO (2000)", "FAO (1949)"]))
        & ~((tb["year"] == 1947) & (tb["source"] == "FAO (1949)"))
    ]

    # * United States:
    tb_us = pr.concat(
        [
            # Prior to 1909, the only data is from Floud et al. (2011).
            tb[(tb["country"] == "United States") & (tb["year"] < 1909) & (tb["source"] == "Floud et al. (2011)")],
            # From 1909 to 1960, take data from USDA/ERS.
            tb[
                (tb["country"] == "United States")
                & (tb["year"] >= 1909)
                & (tb["year"] < 1961)
                & (tb["source"] == "USDA/ERS")
            ],
            # After 1960, use FAOSTAT.
            tb[(tb["country"] == "United States") & (tb["year"] >= 1961) & (tb["source"] == "FAOSTAT")],
        ],
        ignore_index=True,
    )

    # * United Kingdom:
    tb_uk = pr.concat(
        [
            # Prior to 1700, take data from Broadberry et al. (2015).
            tb[
                (tb["country"] == "United Kingdom")
                & (tb["year"] < 1700)
                & (tb["source"] == "Broadberry et al. (2015) via Harris et al. (2015)")
            ],
            # On 1700, take the Estimate (A) (which coincides with (B)) from Floud et al. (2011) via Harris et al. (2015).
            tb[
                (tb["country"] == "United Kingdom")
                & (tb["year"] == 1700)
                & (tb["source"] == "Floud et al. (2011) (Estimates A and B) via Harris et al. (2015)")
            ],
            # Between 1700 and 1850, take the corrected data from Floud et al. (2011), averaging estimates (A) and (B) (taken from Harris et al. (2015)).
            tb[
                (tb["country"] == "United Kingdom")
                & (tb["year"] >= 1750)
                & (tb["year"] <= 1850)
                & (tb["source"].str.startswith("Floud et al. "))
                & (tb["source"].str.endswith("via Harris et al. (2015)"))
            ]
            .groupby(["country", "year"], observed=True, as_index=False)
            .agg({"daily_calories": "mean"})
            .assign(**{"source": "Floud et al. (2011) via Harris et al. (2015) average between estimates (A) and (B)"}),
            # Between 1850 and 1960, take data from Floud et al. (2011).
            tb[
                (tb["country"] == "United Kingdom")
                & (tb["year"] > 1850)
                & (tb["year"] <= 1960)
                & (tb["source"] == "Floud et al. (2011)")
            ],
            # After 1960, use FAOSTAT.
            tb[(tb["country"] == "United Kingdom") & (tb["year"] >= 1961) & (tb["source"] == "FAOSTAT")],
        ],
        ignore_index=True,
    )

    # Combine all selected tables.
    tb_combined = (
        pr.concat(
            [
                tb_selected,
                tb_belgium,
                tb_brazil,
                tb_finland,
                tb_france,
                tb_germany,
                tb_iceland,
                tb_india,
                tb_italy,
                tb_netherlands,
                tb_norway,
                tb_mexico,
                tb_peru,
                tb_us,
                tb_uk,
            ],
            ignore_index=True,
        )
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    # Uncomment to visualize all original and combined series.
    # import plotly.express as px
    # tb_plot = pr.concat([tb, tb_combined.copy().assign(**{"source": "combined"})], ignore_index=True)
    # for country in sorted(set(tb_plot["country"])):
    #     if len(set(tb_plot[tb_plot["country"] == country]["source"])) > 2:
    #         px.line(tb_plot[tb_plot["country"]==country], x="year", y="daily_calories", color="source", title=country, markers=True, color_discrete_map={"combined": "rgba(0,256,0,0.5)", "FAOSTAT": "rgba(0,0,256,0.5)", "USDA/ERS": "rgba(100,100,100,0.5)", "FAO (1949)": "rgba(256,0,0,0.5)", "FAO (2000)": "rgba(100,100,0,0.5)", "Fogel (2004)": "rgba(0,100,100,0.5)"}).show()

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_combined])

    # Save garden dataset.
    ds_garden.save()
