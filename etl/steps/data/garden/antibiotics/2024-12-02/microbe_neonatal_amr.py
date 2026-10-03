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
    ds_meadow = paths.load_dataset("microbe_neonatal_amr")
    ds_total = paths.load_dataset("microbe")
    # Read table from meadow dataset.
    tb = ds_meadow.read("microbe_neonatal_amr")
    tb_total = ds_total.read("microbe")
    #
    # Process data.
    #
    # We want three variables, total, amr attributable and amr non-attributable

    tb_amr = tb.rename(columns={"value": "amr_attributable_deaths"}, errors="raise").drop(columns=["lower", "upper"])
    tb_total = tb_total.rename(columns={"value": "total_deaths"}, errors="raise").drop(
        columns=[
            "lower",
            "upper",
            "age",
            "sex",
            "measure",
            "metric",
            "pathogen_type",
            "infectious_syndrome",
            "counterfactual",
        ]
    )
    tb_total = tb_total[tb_total["year"] == 2021]

    tb = tb_amr.merge(tb_total, on=["country", "year", "pathogen"], how="inner")
    tb["non_amr_attributable_deaths"] = tb["total_deaths"] - tb["amr_attributable_deaths"]

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Reformatting the data so it can be used in stacked bar charts
    tb = tb.drop(columns=["country"]).rename(columns={"pathogen": "country"})

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
