"""This dataset creates region aggregates for important indicators on food and agriculture, adjusted for two issues:
(1) changes in historical regions, and
(2) changes in data coverage.

These adjusted regions let us visualize long-term trends without having abrupt jumps due to, e.g. the USSR dissolution, or countries being added to the data on arbitrary years.

"""

import json

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns from food balances dataset.
COLUMNS_FBSC = {
    "country": "country",
    "year": "year",
    "population__00002501__total_population__both_sexes__000511__thousand_number": "population",
    # NOTE: There are two relevant columns, namely:
    # "total__00002901__food_available_for_consumption__000661__kilocalories" - This comes from FBS.
    # "total__00002901__food_available_for_consumption__000664__kilocalories_per_day" - This was constructed by OWID, by multiplying that same column by the FAO population.
    # Ideally, we could use data directly from FAOSTAT. But the former is only given in FBS, and therefore starts in 2010.
    "total__00002901__food_available_for_consumption__000664__kilocalories_per_day": "food_supply",
}
# Columns from land use dataset.
COLUMNS_RL = {
    "country": "country",
    "year": "year",
    "agricultural_land__00006610__area__005110__hectares": "agricultural_land",
}

# Regions (continents).
REGIONS = ["Europe", "Asia", "Oceania", "Africa", "North America", "South America"]

# Countries that do not have coincident data for agricultural land use and food supply for any coincident year.
# They will be removed from the data (to ensure that the same set of countries is considered for both indicators).
COUNTRIES_EXPECTED_TO_MISS_DATA = {
    "American Samoa",
    "Andorra",
    "Aruba",
    "British Virgin Islands",
    "Cayman Islands",
    "Channel Islands",
    "Cook Islands",
    "Equatorial Guinea",
    "Eritrea",
    "Falkland Islands",
    "Faroe Islands",
    "French Guiana",
    "Greenland",
    "Guadeloupe",
    "Guam",
    "Isle of Man",
    "Liechtenstein",
    "Macao",
    "Martinique",
    "Mayotte",
    "Montserrat",
    "Niue",
    "Norfolk Island",
    "Northern Mariana Islands",
    "Palau",
    "Palestine",
    "Puerto Rico",
    "Reunion",
    "Saint Helena",
    "Saint Pierre and Miquelon",
    "San Marino",
    "Singapore",
    "Tokelau",
    "Turks and Caicos Islands",
    "United States Virgin Islands",
    "Wallis and Futuna",
    "Western Sahara",
    "Latin America and the Caribbean (FAO)",
    "OECD (FAO)",
    "Sub-Saharan Africa (FAO)",
}

# Expected list of additional countries that will be excluded from region aggregates due to limited data coverage.
OTHER_COUNTRIES_EXCLUDED_FROM_AGGREGATES = [
    "Bahrain",
    "Bermuda",
    "Bhutan",
    "Brunei",
    "Burundi",
    "Comoros",
    "Democratic Republic of Congo",
    "Libya",
    "Marshall Islands",
    "Melanesia",
    "Micronesia (country)",
    "Nauru",
    "Netherlands Antilles",
    "North Korea",
    "Oman",
    "Papua New Guinea",
    "Polynesia",
    "Qatar",
    "Seychelles",
    "Somalia",
    # South Sudan data is missing from 2012 to 2018 (which creates a dip in the data). We could remove "Sudan (former)", "Sudan", and "South Sudan" to correct for this, but I think it's better to accept that abrupt change rather than losing a significant amount of relevant data; the data for the combined Sudan area is otherwise complete from 1961 to the last year informed.
    # NOTE: Other countries (Libya, DRC, Somalia) also represent a significant area, and it would be preferrable to keep them. But unfortunately, their data starts in 2010, so in those cases it may be better to remove them, to avoid spurious changes in long-term trends.
    # Similarly, other relevant countries in terms of land area, like Eritrea and Western Sahara, simply have no food supply data, so they are removed from the data (they are removed when we drop nans).
    # "South Sudan",
    # "Sudan",
    # "Sudan (former)",
    "Syria",
    "Tonga",
    "Turkmenistan",
    "Tuvalu",
]


def create_adjusted_lists_of_region_members(tb, tb_regions):
    # List countries in different OWID regions.
    regions = {
        region: sorted(
            set(
                tb_regions[
                    (tb_regions["code"].isin(json.loads(tb_regions[tb_regions["name"] == region]["members"].item())))
                ]["name"]
            )
        )
        for region in REGIONS
    }

    # Add USSR.
    regions["USSR"] = sorted(
        set(
            tb_regions[
                (tb_regions["code"].isin(json.loads(tb_regions[tb_regions["name"] == "USSR"]["successors"].item())))
            ]["name"]
        )
    )
    # Add USSR successor countries in Asia.
    regions["USSR Asia"] = sorted(set(regions["USSR"]) & set(regions["Asia"]))

    # Create a list of countries that will be assigned to "Europe (adjusted)".
    # For now, add all European countries, plus USSR Asia successors.
    # NOTE: To be able to replicate FAO's Europe data, we need to add 'Belgium-Luxembourg (FAO)'.
    regions["Europe"] = sorted(set(regions["Europe"]) | set(["Belgium-Luxembourg (FAO)"]))
    regions["Europe (adjusted)"] = sorted(set(regions["Europe"]) | set(regions["USSR Asia"]))
    ####################################################################################################################
    # We noticed that agricultural land area doesn't seem to include Turkmenistan prior to 1992.
    # This can be noticed by plotting the USSR land area before and after 1992 (the later being the sum of USSR successors). When Turkmenistan is included, there is a jump of an additional ~33M ha.
    # In terms of food supply, however, it's unclear; it seems likely that Turkmenistan is indeed included.
    # So, there is no perfect solution. But it seems that the sudden increase in land use has a larger effect in Europe and the sudden decrease in food supply.
    # So we will remove Turkmenistan from the adjusted Europe.
    # NOTE: Consider revisiting this.
    # As a visual check, see the effect in the USSR of removing Turkmenistan.
    # tb_ussr_adjusted = tb[(tb["country"].isin(sorted(set(regions["USSR"]) - set(["Turkmenistan"]))))].drop(columns="country").groupby("year", as_index=False).sum().assign(**{"country": "USSR (adjusted)"})
    # tb_ussr = tb[(tb["country"] == "USSR")].reset_index(drop=True)
    # for column in ["population", "agricultural_land", "food_supply"]:
    #     px.line(pr.concat([tb_ussr, tb_ussr_adjusted], ignore_index=True), x="year", y=column, markers=True, color="country", range_y=[0, None]).show()
    regions["Europe (adjusted)"] = sorted(set(regions["Europe (adjusted)"]) - set(["Turkmenistan"]))

    # For "Asia (adjusted)", add all Asian countries, and remove USSR Asia successors (which are kept in Europe).
    # NOTE: The issue with Turkmenistan mentioned above is irrelevant here, since we remove all USSR Asian successors anyway.
    # Additional issues in FBS: in 2010, data for Brunei is removed (unclear why), and data for Syria is added. Overall, this causes a small increase in Asia's population. Then, in 2019 (unclear why precisely this year), 3 countries are added to the data, namely Bahrain, Qatar, and Bhutan; but this jump is not significant (as they don't make a significant fraction of the Asian population).
    regions["Asia (adjusted)"] = sorted(
        set(regions["Asia"])
        - set(regions["USSR Asia"])
        - set(["Bahrain", "Bhutan", "Brunei", "North Korea", "Oman", "Qatar", "Syria"])
    )

    # For "Oceania (adjusted)", remove all countries that are added after 2010 (namely Papua New Guinea, and other small islands that are added in 2019 to food supply data).
    assert tb[tb["country"].isin(["Papua New Guinea"])]["year"].min() == 2010
    assert (
        tb[tb["country"].isin(["Marshall Islands", "Micronesia (country)", "Nauru", "Tonga", "Tuvalu"])]["year"].min()
        == 2019
    )
    regions["Oceania (adjusted)"] = sorted(
        set(regions["Oceania"])
        - set(["Marshall Islands", "Micronesia (country)", "Nauru", "Papua New Guinea", "Tonga", "Tuvalu"])
    )

    # For "Africa (adjusted)":
    # - From 2009 to 2010, we gain data for Burundi, Comoros, Democratic Republic of Congo, Libya, Seychelles, and Somalia. These countries didn't have data in FBSH, but do have in FBS.
    # - In 2011, data for Sudan (former) ends, but in 2012 we only have data for Sudan (referring to North Sudan). Unfortunately, data for South Sudan in FBS starts in 2019 (hence we are missing data for South Sudan between 2012 and 2018). This causes an abrupt decrease during those years.
    assert (
        tb[
            tb["country"].isin(["Burundi", "Comoros", "Democratic Republic of Congo", "Libya", "Seychelles", "Somalia"])
        ]["year"].min()
        == 2010
    )
    assert tb[tb["country"] == "Sudan (former)"]["year"].max() == 2011
    assert tb[tb["country"] == "Sudan"]["year"].min() == 2012
    assert tb[tb["country"] == "South Sudan"]["year"].min() == 2019
    regions["Africa (adjusted)"] = sorted(
        set(regions["Africa"])
        - set(
            [
                "Burundi",
                "Comoros",
                "Democratic Republic of Congo",
                "Libya",
                "Seychelles",
                "Somalia",
                # We could exclude Sudan (former), Sudan and South Sudan because the latter is only informed from 2019 on.
                # But this causes a significant data loss, so it may be better to keep them, despite it creating an abrupt dent in agricultural land between 2012 and 2018.
                # "Sudan",
                # "South Sudan",
                # "Sudan (former)",
            ]
        )
    )

    # For "North America (adjusted)":
    regions["North America (adjusted)"] = sorted(set(regions["North America"]) - {"Bermuda", "Netherlands Antilles"})

    # South America doesn't need any adjustments, but we include it here for convenience.
    regions["South America (adjusted)"] = regions["South America"]

    # For each of the defined regions, remove countries that are not included in the data.
    countries_informed = set(tb["country"])
    for region in regions:
        regions[region] = sorted(set(regions[region]) & set(countries_informed))

    return regions


def sanity_check_data_coverage(tb, regions):
    # Check if there are other countries that may have data coverage issues.
    min_year = tb["year"].min()
    max_year = tb["year"].max()
    countries_expected_coverage = {
        # Changes in historical regions in Europe.
        "USSR": (min_year, 1991),
        "Yugoslavia": (min_year, 1991),
        "Bosnia and Herzegovina": (1992, max_year),
        "Croatia": (1992, max_year),
        "North Macedonia": (1992, max_year),
        "Slovenia": (1992, max_year),
        "Czechoslovakia": (min_year, 1992),
        "Czechia": (1993, max_year),
        "Slovakia": (1993, max_year),
        "Serbia and Montenegro": (1992, 2005),
        "Serbia": (2006, max_year),
        "Montenegro": (2006, max_year),
        # Additional changes in Europe's data (not related to historical regions).
        "Belgium-Luxembourg (FAO)": (min_year, 1999),
        "Belgium": (2000, max_year),
        "Luxembourg": (2000, max_year),
        # Changes in historical regions in Africa.
        "Ethiopia (former)": (min_year, 1992),
        "Ethiopia": (1993, max_year),
        "Sudan (former)": (1961, 2011),
        "Sudan": (2012, 2022),
        # NOTE: One would expect South Sudan to start in 2012, but it starts in 2019.
        # Changes in historical regions in North America.
        # NOTE: Successors of Netherlands Antilles are not informed in the data, hence we exclude it from North America (adjusted).
        "Netherlands Antilles": (min_year, 2009),
        # Changes in data coverage in Africa.
        "Seychelles": (2010, max_year),
        "South Sudan": (2019, 2022),
        "Democratic Republic of Congo": (2010, max_year),
        "Mali": (1961, 2022),
        "Comoros": (2010, max_year),
        "Somalia": (2010, 2022),
        "Burundi": (2010, 2022),
        "Libya": (2010, max_year),
        "Togo": (1961, 2022),
        "Central African Republic": (1961, 2022),
        "Chad": (1961, 2022),
        "Benin": (1961, 2022),
        # Changes in data coverage in Asia.
        "Brunei": (min_year, 2009),
        "Syria": (2010, max_year),
        "Bahrain": (2019, max_year),
        "Bhutan": (2019, max_year),
        "Qatar": (2019, max_year),
        "Oman": (1990, max_year),
        "North Korea": (min_year, 2018),
        "Japan": (1961, 2022),
        # Changes in data coverage in North America.
        "Bermuda": (min_year, 2009),
        "Cuba": (1961, 2019),
        "Dominica": (1961, 2022),
        # Changes in data coverage in Oceania.
        "Marshall Islands": (2019, max_year),
        "Tonga": (2019, max_year),
        "Tuvalu": (2019, max_year),
        "Nauru": (2019, max_year),
        "Micronesia (country)": (2019, max_year),
        "Papua New Guinea": (2010, max_year),
    }
    # Add USSR successors.
    countries_expected_coverage.update({country: (1992, max_year) for country in regions["USSR"]})
    # Add all other countries.
    remaining_informed_countries = set(
        tb[
            tb["country"].isin(
                regions["Europe"]
                + regions["Asia"]
                + regions["Africa"]
                + regions["North America"]
                + regions["South America"]
                + regions["Oceania"]
            )
        ]["country"]
    ) - set(countries_expected_coverage)
    countries_expected_coverage.update({country: (min_year, max_year) for country in remaining_informed_countries})
    # Check that the data coverages is as expected.
    for country, (range_min, range_max) in countries_expected_coverage.items():
        error = f"Unexpected data coverage for {country}: ({tb[tb['country'] == country]['year'].min()}, {tb[tb['country'] == country]['year'].max()})"
        assert set(tb[tb["country"] == country]["year"]) == set(range(range_min, range_max + 1)), error
        # If the assertion fails, comment it, and uncomment the following lines; then update the list above
        # if not set(tb[tb["country"] == country]["year"]) == set(range(range_min, range_max + 1)):
        #     print(f"'{country}': ({tb[tb['country'] == country]['year'].min()}, {tb[tb['country'] == country]['year'].max()}),")


def plot_adjusted_data(tb):
    import plotly.express as px

    for region in sorted(REGIONS):
        regions_to_plot = [f"{region} (FAO)", f"{region} (adjusted)"]
        # Uncomment to plot the OWID region (it shouldn't be significantly different to FAO's region).
        # regions_to_plot = [region, f"{region} (FAO)", f"{region} (adjusted)"]
        for column in ["agricultural_land", "food_supply", "population"]:
            unit = {"food_supply": "daily kilocalories", "population": "people", "agricultural_land": "hectares"}[
                column
            ]
            _tb = tb[tb["country"].isin(regions_to_plot)][["country", "year", column]].dropna()
            if not _tb.empty:
                fig = px.line(
                    _tb,
                    x="year",
                    y=column,
                    color="country",
                    markers=True,
                    title=f"{region} - {column.replace('_', ' ').capitalize()}",
                    color_discrete_map={
                        f"{region} (FAO)": "blue",
                        f"{region}": "red",
                        f"{region} (adjusted)": "green",
                    },
                    # NOTE: If the upper limit is set to None, when exporting, the png doesn't respect the limits.
                    range_y=[0, _tb[column].max() * 1.05],
                ).update_layout(yaxis_title=unit)
                fig.show()

                # Uncomment to export as png (after doing "uv add kaleido").
                # from pathlib import Path
                # fig.write_image(Path.home() / "Downloads" / "plots_adjusted" / f"{region}_{column}.png", scale=2)


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT combined food balances dataset, and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc_flat")

    # Load FAOSTAT land use dataset, and read its main table.
    ds_rl = paths.load_dataset("faostat_rl")
    tb_rl = ds_rl.read("faostat_rl_flat")

    # Load regions dataset, and read its main table.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions.read("regions")

    #
    # Process data.
    #
    # Select and rename columns in food balances data.
    tb_fbsc = tb_fbsc[list(COLUMNS_FBSC)].rename(columns=COLUMNS_FBSC, errors="raise")

    # Adjust units of population data.
    tb_fbsc["population"] *= 1000

    # Select and rename columns in land use data.
    tb_rl = tb_rl[list(COLUMNS_RL)].rename(columns=COLUMNS_RL, errors="raise")

    # Combine both tables.
    tb = tb_fbsc.merge(tb_rl, on=["country", "year"], how="outer")

    # Remove empty rows.
    tb = tb.dropna(subset=tb.drop(columns=["country", "year"]).columns, how="all").reset_index(drop=True)

    # Original countries in the data (we keep them to later sanity check which countries have been lost).
    countries_original = set(tb["country"])

    # Now keep only rows for which we have data for both food supply, and agricultural land.
    # This way we ensure that the data coverage of food supply and agricultural land use is the same.
    tb = tb.dropna(subset=["agricultural_land", "food_supply"], how="any").reset_index(drop=True)

    # Sanity check.
    error = "List of countries that are removed (due to not having data on coincident years for both land use and food supply) has changed. Check and update this list."
    assert countries_original - set(tb["country"]) == COUNTRIES_EXPECTED_TO_MISS_DATA, error

    # FAO doesn't have data for North America. Instead, it has Northern, Central, South, and Caribbean.
    # For convenience, create a "North America (FAO)" by adding up Northern, Central, and Caribbean.
    tb_north_america_fao = (
        tb[tb["country"].isin(["Northern America (FAO)", "Central America (FAO)", "Caribbean (FAO)"])]
        .groupby("year", as_index=False)
        .agg({"population": "sum", "agricultural_land": "sum", "food_supply": "sum"})
        .assign(**{"country": "North America (FAO)"})
    )
    tb = pr.concat([tb, tb_north_america_fao], ignore_index=True)

    # Create adjusted lists of region members.
    # Before, we ensured that data coverage is the same for food supply and land use.
    # However, we still have the problem that the series may have abrupt jumps, due to changes in historical regions, and also due to countries being removed or added to the data at different times.
    # Here we fix some of those issues.
    regions = create_adjusted_lists_of_region_members(tb=tb, tb_regions=tb_regions)

    sanity_check_data_coverage(tb=tb, regions=regions)

    # Add new definitions of continents, adjusted for changes in historical regions and changes in data coverage.
    tables_adjusted = [
        tb[tb["country"].isin(regions[f"{region} (adjusted)"])]
        .groupby("year", as_index=False)
        .agg({"food_supply": "sum", "population": "sum", "agricultural_land": "sum"})
        .assign(**{"country": f"{region} (adjusted)"})
        for region in REGIONS
    ]
    tb = pr.concat([tb] + tables_adjusted, ignore_index=True)

    # Sanity check.
    countries_in_regions = set(sum([list(regions[region]) for region in regions if "(adjusted)" in region], []))
    other_countries_excluded_from_aggregates = [
        country
        for country in sorted(countries_original - countries_in_regions - COUNTRIES_EXPECTED_TO_MISS_DATA)
        if "(FAO)" not in country
        if country not in REGIONS
        if "income" not in country
        if country not in ["European Union (27)", "World"]
    ]
    error = "The list of additional countries excluded from region aggregates has changed."
    assert set(other_countries_excluded_from_aggregates) - set(OTHER_COUNTRIES_EXCLUDED_FROM_AGGREGATES) == set(), error

    # Uncomment to visually inspect all changes.
    # plot_adjusted_data(tb=tb)

    ####################################################################################################################
    # In the latest FAOSTAT update (2026-02-25), Africa is very incomplete in the latest year (2023).
    # I'll assert the drop, and remove this point.
    africa_2022 = tb[(tb["country"] == "Africa (adjusted)") & (tb["year"] == 2022)]["agricultural_land"].item()
    africa_2023 = tb[(tb["country"] == "Africa (adjusted)") & (tb["year"] == 2023)]["agricultural_land"].item()
    error = "Expected dip in Africa's agricultural land from 2022 and 2023. This may have been fixed; remove this code."
    assert (100 * (africa_2022 - africa_2023) / africa_2022) > 23, error
    tb.loc[
        (tb["country"] == "Africa (adjusted)") & (tb["year"] == 2023),
        ["population", "food_supply", "agricultural_land"],
    ] = None

    # A similar issue happens with Asia, but the dip is smaller than 3%, so we'll keep this.
    # Additionally, Cuba's data from 2020 onwards has been removed from FBS, this causes a noticeable dip in 2020 for North America.
    # This is unfortunate, but we'll keep this data, instead of removing 4 years of data.
    ####################################################################################################################

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], formats=["feather", "parquet", "csv"])
    ds_garden.save()
