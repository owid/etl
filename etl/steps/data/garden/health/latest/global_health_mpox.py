"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("global_health_mpox")

    # Read table from meadow dataset.
    tb = ds_meadow["global_health_mpox"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb[tb["case_status"] == "suspected"]
    # Calculate the frequency of suspected cases per reported date
    tb = tb.groupby(["country", "date"], observed=True).count().reset_index().drop(columns=["id"])
    # add suspected cases for 2023
    tb_2023 = Table(
        {
            "country": ["Cameroon", "Congo", "Democratic Republic of Congo"],
            "date": ["2023-12-24", "2023-12-24", "2023-12-24"],
            "case_status": ["113", "74", "12985"],
        }
    )
    tb = pr.concat([tb, tb_2023]).sort_values(["country", "date"])
    # Calculate the cumulative
    tb["case_status"] = tb["case_status"].astype("int")
    tb["suspected_cases_cumulative_cases"] = tb.groupby(["country"])["case_status"].cumsum()
    tb = tb.rename(columns={"case_status": "reported_cases"})
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
