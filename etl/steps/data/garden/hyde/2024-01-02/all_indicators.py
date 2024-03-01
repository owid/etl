"""Load a meadow dataset and create a garden dataset."""
from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    """Run step."""
    #
    # Load inputs.
    #
    # Load 'meadow dataset.'
    ds_meadow = paths.load_dataset("all_indicators")
    ds_regions = paths.load_dataset("regions")

    # Read 'all_indicators' table, which contains the actual indicators
    tb = ds_meadow["all_indicators"].reset_index()

    # Read 'general_files' table, which contains the country codes (used to map country codes to country names)
    tb_codes = ds_meadow["country_codes"].reset_index()

    # Read country -> region table
    tb_regions = ds_meadow["region_mapping"].reset_index()

    #
    # Process data.
    #
    # Standardise country codes in codes table
    tb_codes = geo.harmonize_countries(
        df=tb_codes,
        countries_file=paths.country_mapping_path,
    )

    # Standardise country codes in codes table
    tb_regions = geo.harmonize_countries(
        df=tb_regions,
        countries_file=paths.directory / (paths.short_name + ".regions.countries.json"),
    )

    # Add country name to main table
    tb = tb.rename(columns={"country": "iso_code"}, errors="raise").astype({"iso_code": "str"})
    tb_codes = tb_codes.astype("str")
    tb = tb.merge(tb_codes, on="iso_code", how="left")
    tb.loc[tb["iso_code"] == "Total", "country"] = "World"
    # Drop columns
    tb = tb.drop(columns=["iso_code"], errors="raise")

    ## Land use indicators are given in km2, but we want ha: 1km2 = 100ha
    tb[
        [
            # "uopp_c",
            "cropland_c",
            "tot_rice_c",
            "tot_rainfed_c",
            "rf_rice_c",
            "rf_norice_c",
            "tot_irri_c",
            "ir_rice_c",
            "ir_norice_c",
            "grazing_c",
            "pasture_c",
            "rangeland_c",
            "conv_rangeland_c",
            "shifting_c",
        ]
    ] *= 100

    # TODO: Work on keeping table with custom regions
    ## Exclude relative indicators (e.g. population density)
    # columns_exclude = ["popd_c"]
    tb_with_regions = tb.merge(tb_regions, on="country", how="left").copy()
    ## Set region column as string
    tb_with_regions["region"] = tb_with_regions["region"].astype("string")
    ## Add missing mapping for Serbia and Montenegro
    tb_with_regions.loc[tb_with_regions["country"] == "Serbia and Montenegro", "region"] = "Europe"
    ## Remove World
    tb_with_regions = tb_with_regions.loc[tb_with_regions["country"] != "World"]
    ## Ensure that all countries have a region
    countries_missing = set(tb_with_regions.loc[tb_with_regions["region"].isna(), "country"])
    assert not countries_missing, f"Missing regions for {countries_missing}"
    ## Map region names
    # region_renames = {

    # }

    # Add region column
    columns = [col for col in tb.columns if col not in ["popd_c", "shifting_c", "year", "country"]]
    aggregations = dict(zip(columns, ["sum"] * len(columns)))
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        aggregations=aggregations,
        regions={
            "Africa": {},
            "Asia": {},
            "Asia (excl. China and India)": {
                "additional_regions": ["Asia"],
                "excluded_members": ["China", "India"],
            },
            "Europe": {},
            "Europe (excl. Russia)": {
                "additional_regions": ["Europe"],
                "excluded_members": ["Russia"],
            },
            "North America": {},
            "South America": {},
            "Oceania": {},
        },
    )

    # Add agriculture land
    tb["agriculture_c"] = tb["grazing_c"] + tb["cropland_c"]

    # Relative indicators (%, per capita)
    ## NOTE: you should add metadata for these indicators in the YAML file
    ## Share indicators (%)
    columns = ["urbc_c", "rurc_c"]
    for col in columns:
        tb[col + "_share"] = (tb[col] / tb["popc_c"]) * 100
    ## Per capita
    columns = ["uopp_c", "cropland_c", "grazing_c", "agriculture_c"]
    for col in columns:
        tb[col + "_per_capita"] = tb[col] / tb["popc_c"]

    # Replace year 0 with year 1
    ## More: https://en.wikipedia.org/wiki/Year_zero#cite_note-7
    tb["year"] = tb["year"].replace(0, 1)

    # Set index
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
