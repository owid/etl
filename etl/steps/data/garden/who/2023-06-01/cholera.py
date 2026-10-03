"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cholera.start")

    #
    # Load inputs.
    #
    # Load GHO dataset.
    who_gh_dataset = paths.load_dataset("gho")

    # Load fast track dataset
    snap = paths.load_snapshot("cholera.csv")
    cholera_ft = snap.read_csv()

    # Load countries regions
    regions_dataset = paths.load_dataset("regions")
    regions = regions_dataset["regions"]

    # Process GHO dataset
    cholera_bp = process_gho_cholera(who_gh_dataset).reset_index()

    # The regional and global data in the backport is only provided for 2013 so we remove it here and recalculate it
    cholera_bp = geo.harmonize_countries(
        df=cholera_bp, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Add global aggregate
    cholera_bp = add_global_total(cholera_bp, regions)
    # Combine datasets
    cholera_combined = pr.concat([cholera_bp, cholera_ft])

    cholera_combined = add_regions(cholera_combined, regions)

    tb_garden = cholera_combined.set_index(["country", "year"], verify_integrity=True)
    tb_garden.metadata.short_name = "cholera"

    # Save outputs.
    #
    # Create a new garden dataset, inheriting metadata (title etc.) from the WER snapshot's origin.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("cholera.end")


def process_gho_cholera(who_gh_dataset: Dataset) -> Table:
    tb_names = [
        "cholera_case_fatality_rate",
        "number_of_reported_cases_of_cholera",
        "number_of_reported_deaths_from_cholera",
    ]
    cholera_bp = who_gh_dataset[tb_names[0]]
    for tb_name in tb_names[1:]:
        cholera_bp = cholera_bp.join(who_gh_dataset[tb_name].drop(columns=["comments"]), how="outer")

    tb = (
        cholera_bp.loc[
            :,
            [
                "cholera_case_fatality_rate",
                "number_of_reported_cases_of_cholera",
                "number_of_reported_deaths_from_cholera",
            ],
        ]
        .rename(
            columns={
                "cholera_case_fatality_rate": "cholera_case_fatality_rate",
                "number_of_reported_cases_of_cholera": "cholera_reported_cases",
                "number_of_reported_deaths_from_cholera": "cholera_deaths",
            }
        )
        .dropna(how="all", axis=0)
        .astype(float)
    )

    return tb


def add_global_total(tb: Table, regions: Table) -> Table:
    """
    Calculate global total of cholera cases and add it to the existing dataset
    """

    countries = regions[regions["region_type"] == "country"]["name"].to_list()
    manual_countries_to_allow = ["Serbia and Montenegro (former)"]
    countries = countries + manual_countries_to_allow
    assert all(tb["country"].isin(countries)), (
        f"{tb['country'][~tb['country'].isin(countries)].drop_duplicates()}, is not a country"
    )
    tb_glob = tb.groupby(["year"]).agg({"cholera_reported_cases": "sum", "cholera_deaths": "sum"}).reset_index()
    tb_glob["country"] = "World"
    tb_glob["cholera_case_fatality_rate"] = cholera_case_fatality_rate(tb_glob)
    tb = pr.concat([tb, tb_glob])

    return tb


def add_regions(tb: Table, regions: Table) -> Table:
    continents = regions[regions["region_type"] == "continent"]["name"].to_list()

    countries_in_regions = {
        region: sorted(set(geo.list_countries_in_region(region)) & set(tb["country"])) for region in continents
    }
    tb_out = None
    for continent in continents:
        if continent == "Europe":
            tb_cont = geo.add_region_aggregates(
                df=tb[["year", "country", "cholera_reported_cases", "cholera_deaths"]],
                region=continent,
                countries_in_region=countries_in_regions[continent] + ["Serbia and Montenegro (former)"],
                countries_that_must_have_data=[],
                num_allowed_nans_per_year=None,
                frac_allowed_nans_per_year=0.2,
            )
        else:
            tb_cont = geo.add_region_aggregates(
                df=tb[["year", "country", "cholera_reported_cases", "cholera_deaths"]],
                region=continent,
                countries_in_region=countries_in_regions[continent],
                countries_that_must_have_data=[],
                num_allowed_nans_per_year=200,
                country_col="country",
                year_col="year",
                frac_allowed_nans_per_year=1,
            )
        tb_cont = tb_cont[tb_cont["country"].isin(continents)]
        tb_out = tb_cont if tb_out is None else pr.concat([tb_out, tb_cont])
    tb_out["cholera_case_fatality_rate"] = cholera_case_fatality_rate(tb_out)

    tb = pr.concat([tb, tb_out])

    return tb


def cholera_case_fatality_rate(tb: Table) -> Table:
    return (tb["cholera_deaths"] / tb["cholera_reported_cases"]) * 100
