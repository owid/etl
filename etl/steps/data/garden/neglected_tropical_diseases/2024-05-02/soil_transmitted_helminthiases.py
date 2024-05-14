"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("soil_transmitted_helminthiases")

    # Read table from meadow dataset.
    tb = ds_meadow["soil_transmitted_helminthiases"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    age_groups = ["pre_sac", "sac"]
    tbs = {}
    for age_group in age_groups:
        cols = [
            "country",
            "year",
            f"drug_combination__{age_group}",
            f"population_requiring_pc_for_sth__{age_group}",
            f"number_of_{age_group}_targeted",
            f"reported_number_of_{age_group}_treated",
            f"programme_coverage__{age_group}__pct",
            f"national_coverage__{age_group}__pct",
        ]
        tbs[f"tb_{age_group}"] = tb[cols].copy()
        tbs[f"tb_{age_group}"].columns = [
            "country",
            "year",
            "drug_combination",
            "population_requiring_pc_for_sth",
            "number_targeted",
            "reported_number_treated",
            "programme_coverage__pct",
            "national_coverage__pct",
        ]
        tbs[f"tb_{age_group}"] = tbs[f"tb_{age_group}"].dropna(
            subset=[
                "drug_combination",
                "population_requiring_pc_for_sth",
                "number_targeted",
                "reported_number_treated",
                "programme_coverage__pct",
                "national_coverage__pct",
            ],
        )
        # There are some rows which seem to be erroneous duplicates, we will drop these e.g. Burundi 2015 for sac
        tbs[f"tb_{age_group}"] = tbs[f"tb_{age_group}"].drop_duplicates(subset=["country", "year", "drug_combination"])

    tb_pre_sac = tbs["tb_pre_sac"]
    tb_sac = tbs["tb_sac"]

    tb_pre_sac.metadata.short_name = "soil_transmitted_helminthiases_pre_sac"
    tb_sac.metadata.short_name = "soil_transmitted_helminthiases_sac"

    tb_sac = tb_sac.format(["country", "year", "drug_combination"])
    tb_pre_sac = tb_pre_sac.format(["country", "year", "drug_combination"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_sac, tb_pre_sac], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
