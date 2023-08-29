"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Dataset

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.paths import DATA_DIR

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
DATASET_GAPMINDER_CHILD_MORTALITY = DATA_DIR / "open_numbers" / "open_numbers" / "latest" / "gapminder__child_mortality"

GAPMINDER_SOURCE_NAME = "gapminder"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_igme = paths.load_dataset("igme")

    # Read table from meadow dataset.
    tb = ds_igme["long_run_child_mortality"].reset_index()

    # Load Gapminder data
    tb_gapminder = load_gapminder_data()
    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb_gapminder,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_igme.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def load_gapminder_data():
    """load gapminder dataset's table only with former countries."""
    ds = Dataset(DATASET_GAPMINDER_CHILD_MORTALITY)
    tb = ds["child_mortality_0_5_year_olds_dying_per_1000_born"]

    # reset index
    tb = tb.reset_index()

    # add source
    tb["source"] = GAPMINDER_SOURCE_NAME

    # filter countries
    # msk = tb["geo"].isin(FORMER_COUNTRIES)
    # tb = tb[msk]

    # rename countries
    # tb["country"] = tb["geo"].map({code: data["name"] for code, data in FORMER_COUNTRIES.items()})

    # columns
    tb = tb.rename(
        columns={
            "time": "year",
            "geo": "country",
            "child_mortality_0_5_year_olds_dying_per_1000_born": "child_mortality",
        }
    )

    # filter countries
    # for _, data in FORMER_COUNTRIES.items():
    #    country_name = data["name"]
    #    end_year = data["end"]
    #    tb = tb[-((tb["country"] == country_name) & (tb["year"] > end_year))]

    # output columns
    tb = tb[["country", "year", "child_mortality", "source"]]

    # reset index
    # tb = tb.reset_index(drop=True)
    return tb
