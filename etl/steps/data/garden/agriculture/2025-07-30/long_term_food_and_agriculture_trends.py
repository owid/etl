"""This dataset creates region aggregates for important indicators on food and agriculture, corrected for two issues:
(1) changes in historical regions, and
(2) changes in data coverage.

These corrected regions let us visualize long-term trends without having abrupt jumps due to, e.g. the USSR dissolution, or countries being added to the data on arbitrary years.

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
}

# Expected list of additional countries that will be excluded from region aggregates due to limited data coverage.
OTHER_COUNTRIES_EXCLUDED_FROM_AGGREGATES = [
    "Bahrain",
    "Bhutan",
    "Burundi",
    "Brunei",
    "Comoros",
    "Democratic Republic of Congo",
    "Libya",
    "Marshall Islands",
    "Melanesia",
    "Micronesia (country)",
    "Nauru",
    "Papua New Guinea",
    "Polynesia",
    "Qatar",
    "Seychelles",
    "Somalia",
    "South Sudan",
    "Sudan",
    "Sudan (former)",
    "Syria",
    "Tonga",
    "Turkmenistan",
    "Tuvalu",
]


def create_corrected_lists_of_region_members(tb, tb_regions):
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

    # Create a list of countries that will be assigned to "Europe (corrected)".
    # For now, add all European countries, plus USSR Asia successors.
    # NOTE: To be able to replicate FAO's Europe data, we need to add 'Belgium-Luxembourg (FAO)'.
    regions["Europe"] = sorted(set(regions["Europe"]) | set(["Belgium-Luxembourg (FAO)"]))
    regions["Europe (corrected)"] = sorted(set(regions["Europe"]) | set(regions["USSR Asia"]))
    ####################################################################################################################
    # We noticed that agricultural land area doesn't seem to include Turkmenistan prior to 1992.
    # This can be noticed by plotting the USSR land area before and after 1992 (the later being the sum of USSR successors). When Turkmenistan is included, there is a jump of an additional ~33M ha.
    # In terms of food supply, however, it's unclear; it seems likely that Turkmenistan is indeed included.
    # So, there is no perfect solution. But it seems that the sudden increase in land use has a larger effect in Europe and the sudden decrease in food supply.
    # So we will remove Turkmenistan from the corrected Europe.
    # NOTE: Consider revisiting this.
    # As a visual check, see the effect in the USSR of removing Turkmenistan.
    # tb_ussr_corrected = tb[(tb["country"].isin(sorted(set(regions["USSR"]) - set(["Turkmenistan"]))))].drop(columns="country").groupby("year", as_index=False).sum().assign(**{"country": "USSR (corrected)"})
    # tb_ussr = tb[(tb["country"] == "USSR")].reset_index(drop=True)
    # for column in ["population", "agricultural_land", "food_supply"]:
    #     px.line(pr.concat([tb_ussr, tb_ussr_corrected], ignore_index=True), x="year", y=column, markers=True, color="country", range_y=[0, None]).show()
    regions["Europe (corrected)"] = sorted(set(regions["Europe (corrected)"]) - set(["Turkmenistan"]))
    ####################################################################################################################

    # For "Asia (corrected)", add all Asian countries, and remove USSR Asia successors (which are kept in Europe).
    # NOTE: The issue with Turkmenistan mentioned above is irrelevant here, since we remove all USSR Asian successors anyway.
    # Additional issues in FBS: in 2010, data for Brunei is removed (unclear why), and data for Syria is added. Overall, this causes a small increase in Asia's population. Then, in 2019 (unclear why precisely this year), 3 countries are added to the data, namely Bahrain, Qatar, and Bhutan; but this jump is not significant (as they don't make a significant fraction of the Asian population).
    assert tb[tb["country"].isin(["Brunei"])]["year"].max() == 2009
    assert tb[tb["country"].isin(["Syria"])]["year"].min() == 2010
    assert tb[tb["country"].isin(["Bahrain", "Qatar", "Bhutan"])]["year"].min() == 2019
    regions["Asia (corrected)"] = sorted(
        set(regions["Asia"]) - set(regions["USSR Asia"]) - set(["Bahrain", "Bhutan", "Brunei", "Qatar", "Syria"])
    )

    # For "Oceania (corrected", remove all countries that are added after 2010 (namely Papua New Guinea, and other small islands that are added in 2019 to food supply data).
    assert tb[tb["country"].isin(["Papua New Guinea"])]["year"].min() == 2010
    assert (
        tb[tb["country"].isin(["Marshall Islands", "Micronesia (country)", "Nauru", "Tonga", "Tuvalu"])]["year"].min()
        == 2019
    )
    regions["Oceania (corrected)"] = sorted(
        set(regions["Oceania"])
        - set(["Marshall Islands", "Micronesia (country)", "Nauru", "Papua New Guinea", "Tonga", "Tuvalu"])
    )

    # For "Africa (corrected)":
    # - From 2009 to 2010, we gain data for Burundi, Comoros, Democratic Republic of Congo, Libya, Seychelles, and Somalia. These countries didn't have data in FBSH, but do have in FBS.
    # - In 2011, data for Sudan (former) ends, but in 2012 we only have data for Sudan (referring to North Sudan). Unfortunately, data for South Sudan in FBS starts in 2019 (hence we are missing data for South Sudan between 2012 and 2018).
    assert (
        tb[
            tb["country"].isin(["Burundi", "Comoros", "Democratic Republic of Congo", "Libya", "Seychelles", "Somalia"])
        ]["year"].min()
        == 2010
    )
    assert tb[tb["country"] == "Sudan (former)"]["year"].max() == 2011
    assert tb[tb["country"] == "Sudan"]["year"].min() == 2012
    assert tb[tb["country"] == "South Sudan"]["year"].min() == 2019
    regions["Africa (corrected)"] = sorted(
        set(regions["Africa"])
        - set(
            [
                "Burundi",
                "Comoros",
                "Democratic Republic of Congo",
                "Libya",
                "Seychelles",
                "Somalia",
                "Sudan",
                "South Sudan",
                "Sudan (former)",
            ]
        )
    )

    # North and South America don't need any corrections.
    # We include them here for convenience.
    regions["North America (corrected)"] = regions["North America"]
    regions["South America (corrected)"] = regions["South America"]

    # For each of the defined regions, remove countries that are not included in the data.
    countries_informed = set(tb["country"])
    for region in regions:
        regions[region] = set(regions[region]) & set(countries_informed)

    return regions


def additional_debugging_checks():
    # This function loads the original meadow steps for FBSH and FBS, and combines them doing minimal processing.
    # This function is only used to ensure that the results do not depend on any possibly additional processing that happens in the FAOSTAT garden step.
    # NOTE: For this function to work, you will need to add some dependencies to the current step in the DAG, namely:
    # - data://meadow/faostat/2025-03-17/faostat_fbs
    # - data://meadow/faostat/2025-03-17/faostat_fbsh
    # - data://garden/demography/2024-07-15/population
    # - data://garden/wb/2025-07-01/income_groups
    from owid.datautils.dataframes import combine_two_overlapping_dataframes

    from etl.data_helpers import geo
    from etl.paths import STEP_DIR

    # Load FBSH and FBS datasets directly from meadow.
    ds_fbs = paths.load_dataset("faostat_fbs")
    tb_fbs = ds_fbs.read("faostat_fbs")
    ds_fbsh = paths.load_dataset("faostat_fbsh")
    tb_fbsh = ds_fbsh.read("faostat_fbsh")
    tb_fbs = tb_fbs[(tb_fbs["element_code"].isin([511, 664])) & (tb_fbs["item_code"].isin([2501, 2901]))].rename(
        columns={"area": "country"}
    )
    tb_fbs["col"] = tb_fbs["item"] + "-" + tb_fbs["element"]
    tb_fbs = (
        tb_fbs[["country", "year", "col", "value"]]
        .pivot(index=["country", "year"], columns="col", join_column_levels_with="_")
        .rename(
            columns={
                "value_Grand Total-Food supply (kcal/capita/day)": "food_supply",
                "value_Population-Total Population - Both sexes": "population",
            }
        )
    )
    tb_fbsh = tb_fbsh[(tb_fbsh["element_code"].isin([511, 664])) & (tb_fbsh["item_code"].isin([2501, 2901]))].rename(
        columns={"area": "country"}
    )
    tb_fbsh["col"] = tb_fbsh["item"] + "-" + tb_fbsh["element"]
    tb_fbsh = (
        tb_fbsh[["country", "year", "col", "value"]]
        .pivot(index=["country", "year"], columns="col", join_column_levels_with="_")
        .rename(
            columns={
                "value_Grand Total-Food supply (kcal/capita/day)": "food_supply",
                "value_Population-Total Population - Both sexes": "population",
            }
        )
    )
    countries_file = STEP_DIR / f"data/garden/faostat/{ds_fbs.version}/faostat.countries.json"
    excluded_countries_file = STEP_DIR / f"data/garden/faostat/{ds_fbs.version}/faostat.excluded_countries.json"
    tb_fbs = geo.harmonize_countries(
        tb_fbs,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )
    tb_fbsh = geo.harmonize_countries(
        tb_fbsh,
        countries_file=countries_file,
        excluded_countries_file=excluded_countries_file,
        warn_on_unknown_excluded_countries=False,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )
    tb_fbsc = combine_two_overlapping_dataframes(tb_fbs, tb_fbsh, index_columns=["country", "year"])
    tb_population = (
        paths.load_dataset("population")
        .read("population")[["country", "year", "population"]]
        .rename(columns={"population": "owid_population"})
    )
    tb_population["owid_population"] /= 1000

    # An additional check is to use OWID population instead of FAO population, to see if things change.
    # tb_fbsc = tb_fbsc.merge(tb_population, on=["country", "year"], how="left")
    # tb_fbsc["population"] = tb_fbsc["owid_population"].astype(float).fillna(tb_fbsc["population"])
    # Also, visually inspect the difference between FAO and OWID population for USSR.
    # check = tb_fbsc[tb_fbsc["country"].isin(["USSR"] + regions["USSR"])].groupby("year", as_index=False).agg({"population": "sum", "owid_population": "sum"})[["year", "population", "owid_population"]].melt(id_vars=["year"])
    # px.line(check, x="year", y="value", color="variable", markers=True)
    tb_fbsc["food_supply"] *= tb_fbsc["population"]
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    tb_fbsc = geo.add_regions_to_table(tb=tb_fbsc, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    return tb_fbsc


def plot_corrected_data(tb):
    import plotly.express as px

    for region in sorted(REGIONS):
        regions_to_plot = [f"{region} (FAO)", f"{region} (corrected)"]
        # Uncomment to plot the OWID region (it shouldn't be significantly different to FAO's region).
        # regions_to_plot = [region, f"{region} (FAO)", f"{region} (corrected)"]
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
                        f"{region} (corrected)": "green",
                    },
                    # NOTE: If the upper limit is set to None, when exporting, the png doesn't respect the limits.
                    range_y=[0, _tb[column].max() * 1.05],
                ).update_layout(yaxis_title=unit)
                fig.show()

                # Uncomment to export as png (after doing "uv add kaleido").
                # from pathlib import Path
                # fig.write_image(Path.home() / "Downloads" / "plots_corrected" / f"{region}_{column}.png", scale=2)


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT combined food balances dataset, and read its main table.
    # NOTE: It may be necessary to load the meadow FBSH and FBS datasets. For now, try with FBSC.
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

    # Uncomment the following line to replace the loaded FBSC dataset from garden, and instead load the original FBSH and FBS datasets from meadow. This ensures that the results are not affected by any further processing in the FAOSTAT garden step.
    # tb_fbsc = additional_debugging_checks()

    # Correct units of population data.
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

    # Create corrected lists of region members.
    # Before, we ensured that data coverage is the same for food supply and land use.
    # However, we still have the problem that the series may have abrupt jumps, due to changes in historical regions, and also due to countries being removed or added to the data at different times.
    # Here we fix some of those issues.
    regions = create_corrected_lists_of_region_members(tb=tb, tb_regions=tb_regions)

    # Add new definitions of continents, corrected for changes in historical regions and changes in data coverage.
    tables_corrected = [
        tb[tb["country"].isin(regions[f"{region} (corrected)"])]
        .groupby("year", as_index=False)
        .agg({"food_supply": "sum", "population": "sum", "agricultural_land": "sum"})
        .assign(**{"country": f"{region} (corrected)"})
        for region in REGIONS
    ]
    tb = pr.concat([tb] + tables_corrected, ignore_index=True)

    # Sanity check.
    countries_in_regions = set(sum([list(regions[region]) for region in regions if "(corrected)" in region], []))
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
    # plot_corrected_data(tb=tb)

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
