"""This dataset creates region aggregates that are corrected for two issues, namely (1) changes in historical regions, and (2) changes in data coverage. These corrected regions let us visualize long-term trends without having abrupt jumps due to, e.g. the USSR dissolution, or countries being added to the data on arbitrary years."""

import json

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns from food balances dataset.
COLUMNS_FBSC = {
    "country": "country",
    "year": "year",
    "population__00002501__total_population__both_sexes__000511__thousand_number": "fao_population",
    # NOTE: There are two relevant columns, namely:
    # "total__00002901__food_available_for_consumption__000661__kilocalories" - This comes from FBS.
    # "total__00002901__food_available_for_consumption__000664__kilocalories_per_day" - This was constructed by OWID, by multiplying that same column by the FAO population.
    # Ideally, we could use data directly from FAOSTAT. But the former is only given in FBS, and therefore starts in 2010.
    "total__00002901__food_available_for_consumption__000664__kilocalories_per_day": "kcal_per_day",
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
    tb_fbsc = (
        tb_fbsc[list(COLUMNS_FBSC)]
        .rename(columns=COLUMNS_FBSC, errors="raise")
        .dropna(how="all")
        .reset_index(drop=True)
    )

    # Select and rename columns in land use data.
    tb_rl = tb_rl[list(COLUMNS_RL)].rename(columns=COLUMNS_RL, errors="raise").dropna(how="all").reset_index(drop=True)

    # Combine both tables.
    tb = tb_fbsc.merge(tb_rl, on=["country", "year"], how="outer")

    # Remove empty rows.
    tb = tb.dropna(subset=tb.drop(columns=["country", "year"]).columns, how="all").reset_index(drop=True)

    # Original countries in the data.
    countries_original = set(tb["country"])

    # Now keep only rows for which we have data for both food supply, and agricultural land.
    tb = tb.dropna(subset=["agricultural_land", "kcal_per_day"], how="any").reset_index(drop=True)

    # Sanity check.
    error = "List of countries that are removed (due to not having data on coincident years for both land use and food supply) has changed. Check and update this list."
    assert countries_original - set(tb["country"]) == COUNTRIES_EXPECTED_TO_MISS_DATA, error

    # FAO doesn't have data for North America. Instead, it has Northern, Central, South, and Caribbean.
    # For convenience, create a "North America (FAO)" by adding up Northern, Central, and Caribbean.
    tb_north_america_fao = (
        tb[tb["country"].isin(["Northern America (FAO)", "Central America (FAO)", "Caribbean (FAO)"])]
        .groupby("year", as_index=False)
        .agg({"fao_population": "sum", "agricultural_land": "sum", "kcal_per_day": "sum"})
        .assign(**{"country": "North America (FAO)"})
    )
    tb = pr.concat([tb, tb_north_america_fao], ignore_index=True)

    # List countries in different OWID regions.
    regions = {
        region: sorted(
            set(
                tb_regions[
                    (tb_regions["code"].isin(json.loads(tb_regions[tb_regions["name"] == region]["members"].item())))
                ]["name"]
            )
        )
        for region in ["Africa", "Asia", "Europe", "Oceania", "North America", "South America"]
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
    # for column in ["fao_population", "agricultural_land", "kcal_per_day"]:
    #     px.line(pr.concat([tb_ussr, tb_ussr_corrected], ignore_index=True), x="year", y=column, markers=True, color="country", range_y=[0, None]).show()
    regions["Europe (corrected)"] = sorted(set(regions["Europe (corrected)"]) - set(["Turkmenistan"]))
    ####################################################################################################################

    # For "Asia (corrected)", add all Asian countries, and remove USSR Asia successors (which are kept in Europe).
    # NOTE: The issue with Turkmenistan mentioned above is irrelevant here, since we remove all USSR Asian successors anyway.
    regions["Asia (corrected)"] = sorted(set(regions["Asia"]) - set(regions["USSR Asia"]))

    # For "Oceania (corrected", remove all countries that are added after 2010 (namely Papua New Guinea, and other small islands that are added in 2019 to food supply data).
    regions["Oceania (corrected)"] = sorted(
        set(regions["Oceania"])
        - set(["Marshall Islands", "Micronesia (country)", "Nauru", "Papua New Guinea", "Tonga", "Tuvalu"])
    )

    # For "Africa (corrected)", remove countries that are added to the data in FBS, but were not informed in FBSH. Detailed explanation:
    # - From 2009 to 2010, we switch from FBSH to FBS, and lose data for "Sudan (former)" for 2010 and 2011 (since this entity does not exist in FBS).
    # - In that transition, we gain data for Burundi, Comoros, Democratic Republic of Congo, Libya, Seychelles, and Somalia. These countries didn’t have data in FBSH, but do have in FBS.
    # - Then, in 2012 we have data again for Sudan and South Sudan, in FBS.
    #   NOTE: The issue with missing "Sudan (former)" could be fixed by copying that entity's data from FBSH to FBS for 2010 and 2011. But for now, it's better to remove all data for Sudan, to avoid various jumps.
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

    # Add new definitions of continents, corrected for changes in historical regions and changes in data coverage.
    tables_corrected = [
        tb[tb["country"].isin(regions[f"{region} (corrected)"])]
        .groupby("year", as_index=False)
        .agg({"kcal_per_day": "sum", "fao_population": "sum", "agricultural_land": "sum"})
        .assign(**{"country": f"{region} (corrected)"})
        for region in ["Europe", "Africa", "Asia", "Oceania"]
    ]
    tb = pr.concat([tb] + tables_corrected, ignore_index=True)

    # Uncomment to visually inspect all changes.
    # for region in REGIONS:
    #     for column in ["kcal_per_day", "fao_population", "agricultural_land"]:
    #         _tb = tb[tb["country"].isin([region, f"{region} (FAO)", f"{region} (corrected)"])][
    #             ["country", "year", column]
    #         ].dropna()
    #         if not _tb.empty:
    #             px.line(
    #                 _tb,
    #                 x="year",
    #                 y=column,
    #                 color="country",
    #                 markers=True,
    #                 title=f"{region} - {column}",
    #                 color_discrete_map={
    #                     f"{region} (FAO)": "blue",
    #                     f"{region}": "red",
    #                     f"{region} (corrected)": "green",
    #                 },
    #                 range_y=[0, None],
    #             ).show()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
