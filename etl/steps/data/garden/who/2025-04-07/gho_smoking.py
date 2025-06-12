"""Loads relevant smoking indicators from the GHO database and harmonizes them.
This step includes both smoking prevalence estimates and the MPOWER indicators on tobacco control policies."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [r for r in geo.REGIONS.keys() if r != "European Union (27)"] + ["World"]

LAST_YEAR = 2022

# short table names
afford_gdp = "affordability_of_cigarettes__percentage_of_gdp_per_capita_required_to_purchase_2000_cigarettes_of_the_most_sold_brand"
taxes = "taxes_as_a_pct_of_price__total_tax"
ad_bans = "enforce_bans_on_tobacco_advertising"
help_quit = "offer_help_to_quit_tobacco_use"
smoke_free = "number_of_places_smoke_free__national_legislation"


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gho")
    ds_regions = paths.load_dataset("regions")
    ds_income = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("un_wpp")

    # get smoking indicators
    all_rel_indicators = [
        c for c in ds_meadow.table_names if (("smok" in c.lower()) or ("tobac" in c.lower()) or "tax" in c.lower())
    ]

    smoking_estimates = [ind for ind in all_rel_indicators if "estimate" in ind.lower()]

    # read tables for policy indicators
    tb_taxes = ds_meadow.read(taxes)
    tb_ads = ds_meadow.read(ad_bans)
    tb_quit = ds_meadow.read(help_quit)
    tb_afford = ds_meadow.read(afford_gdp)
    tb_smoke_free = ds_meadow.read(smoke_free)

    ### clean up policy indicators
    # taxes
    tb_taxes = tb_taxes[tb_taxes["tobacco_and_nicotine_product"] == "Most sold brand of cigarettes (20 sticks)"]
    tb_taxes = tb_taxes.drop(columns=["tobacco_and_nicotine_product", "comments"], errors="raise")

    # smoke free
    tb_smoke_free = tb_smoke_free.drop(columns=["comments"], errors="raise")

    # support to quit
    # 1 means no data available, so should be dropped
    tb_quit = tb_quit[tb_quit[help_quit] != 1]

    tb_empower = pr.multi_merge(
        [tb_taxes, tb_ads, tb_quit, tb_afford, tb_smoke_free],  # type: ignore
        on=["country", "year"],
        how="outer",
    )

    tb_empower = tb_empower.rename(
        columns={
            afford_gdp: "cig_afford_pct_gdp",
            taxes: "cig_tax_pct",
            ad_bans: "tobacco_ad_ban",
            help_quit: "tobacco_help_quit",
            smoke_free: "tobacco_smoke_free",
        },
        errors="raise",
    )

    tbs = []

    # Load all smoking estimates

    for tb_name in smoking_estimates:
        tb = ds_meadow.read(tb_name)

        tb = tb.drop(columns=["comments"], errors="raise")

        # Add to list of tables.
        tbs.append(tb)

    # Concatenate all tables.
    tb = pr.multi_merge(tbs, on=["country", "year", "sex"], how="left")

    # drop and rename columns
    tb = tb.rename(
        columns={
            "estimate_of_current_cigarette_smoking_prevalence__pct": "cig_smoking_pct",
            "estimate_of_current_cigarette_smoking_prevalence__pct__age_standardized_rate": "cig_smoking_pct_age_std",
            "estimate_of_current_tobacco_smoking_prevalence__pct": "tobacco_smoking_pct",
            "estimate_of_current_tobacco_smoking_prevalence__pct__age_standardized_rate": "tobacco_smoking_pct_age_std",
            "estimate_of_current_tobacco_use_prevalence__pct": "tobacco_use_pct",
            "estimate_of_current_tobacco_use_prevalence__pct__age_standardized_rate": "tobacco_use_pct_age_std",
        },
        errors="raise",
    )

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # add population to table (un wpp to have female/ male population and only 15+)
    tb_pop = ds_population.read("population")
    tb_pop = tb_pop[(tb_pop["variant"].isin(["estimates", "medium"]) & (tb_pop["age"] == "15+"))]
    tb_pop["sex"] = tb_pop["sex"].replace({"all": "both sexes"})

    tb = pr.merge(tb, tb_pop, how="left", on=["country", "year", "sex"])

    # create map of relative indicators to absolute indicators
    ind = {
        "cig_smoking_pct": "cig_smokers",
        "cig_smoking_pct_age_std": "cig_smokers_age_std",
        "tobacco_smoking_pct": "tobacco_smokers",
        "tobacco_smoking_pct_age_std": "tobacco_smokers_age_std",
        "tobacco_use_pct": "tobacco_users",
        "tobacco_use_pct_age_std": "tobacco_users_age_std",
    }

    # Calculate the number of smokers and users.
    for rel_ind, abs_ind in ind.items():
        tb[abs_ind] = tb[rel_ind] * tb["population"] / 100

    agg = {
        "cig_smokers": "sum",
        "cig_smokers_age_std": "sum",
        "tobacco_smokers": "sum",
        "tobacco_smokers_age_std": "sum",
        "tobacco_users": "sum",
        "tobacco_users_age_std": "sum",
        "population": "sum",
    }

    tb = geo.add_regions_to_table(
        tb=tb,
        index_columns=["country", "year", "sex"],
        ds_regions=ds_regions,
        ds_income_groups=ds_income,
        regions=REGIONS,
        aggregations=agg,
        frac_allowed_nans_per_year=0.1,
    )

    tb = tb.drop(columns=["age", "variant", "population_change", "population_density"])

    # Calculate the prevalences for regions:
    for rel_ind, abs_ind in ind.items():
        tb.loc[tb["country"].isin(REGIONS), rel_ind] = tb[abs_ind] / tb["population"] * 100

    tb = tb.drop(columns=["cig_smokers_age_std", "tobacco_smokers_age_std", "tobacco_users_age_std", "population"])

    tb["cig_smokers"] = tb["cig_smokers"].copy_metadata(tb["cig_smoking_pct"])
    tb["tobacco_smokers"] = tb["tobacco_smokers"].copy_metadata(tb["tobacco_smoking_pct"])
    tb["tobacco_users"] = tb["tobacco_users"].copy_metadata(tb["tobacco_use_pct"])

    # filter out predictions
    tb = tb[(tb["year"] <= LAST_YEAR)]

    # Improve table format.
    tb = tb.format(["country", "year", "sex"], short_name="gho_smoking")
    tb_empower = tb_empower.format(["country", "year"], short_name="gho_smoking_empower")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_empower], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
