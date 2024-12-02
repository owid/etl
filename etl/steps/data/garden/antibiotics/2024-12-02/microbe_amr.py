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
    ds_meadow = paths.load_dataset("microbe_amr")
    ds_total = paths.load_dataset("total_syndrome")
    # Read table from meadow dataset.
    tb = ds_meadow.read("microbe_amr")
    tb_total = ds_total.read("total_syndrome")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb_total = geo.harmonize_countries(df=tb_total, countries_file=paths.country_mapping_path)

    # We want three variables, total, amr attributable and amr non-attributable

    tb_amr = (
        tb[tb["counterfactual"] == "Attributable"]
        .rename(columns={"value": "amr_attributable_deaths"}, errors="raise")
        .drop(columns=["lower", "upper"])
    )
    tb_total = tb_total.rename(columns={"value": "total_deaths"}, errors="raise").drop(columns=["lower", "upper"])

    tb = tb_amr.merge(tb_total, on=["country", "year", "infectious_syndrome"], how="left")

    tb["non_amr_attributable_deaths"] = tb["total_deaths"] - tb["amr_attributable_deaths"]

    tb = tb.format(["country", "year", "infectious_syndrome"]).drop(columns=["counterfactual"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
