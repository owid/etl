"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [r for r in geo.REGIONS.keys() if r != "European Union (27)"] + ["World"]


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
    all_smoking_indicators = [c for c in ds_meadow.table_names if (("smok" in c.lower()) or ("tobac" in c.lower()))]

    smoking_estimates = [ind for ind in all_smoking_indicators if "estimate" in ind.lower()]

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

    # Improve table format.
    tb = tb.format(["country", "year", "sex"], short_name="gho_smoking")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
