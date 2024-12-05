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
    ds_meadow = paths.load_dataset("microbe_neonatal_total_amr")
    ds_total = paths.load_dataset("microbe")

    # Read table from meadow dataset.
    tb = (
        ds_meadow.read("microbe_neonatal_total_amr")
        .drop(columns=["upper", "lower"])
        .rename(columns={"value": "amr_attributable_deaths"})
    )
    tb_total = ds_total.read("microbe").drop(columns=["upper", "lower"]).rename(columns={"value": "total_deaths"})

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.merge(tb_total, on=["country", "year", "pathogen", "pathogen_type"], how="inner")
    tb["non_amr_attributable_deaths"] = tb["total_deaths"] - tb["amr_attributable_deaths"]
    tb = tb.drop(columns=["country", "pathogen_type"]).rename(columns={"pathogen": "country"})
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
