"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("korevaar")
    ds_meadow_quality = paths.load_dataset("korevaar_quality")

    # Read table from meadow dataset.
    tb = ds_meadow.read("korevaar")
    tb_quality = ds_meadow_quality.read("korevaar_quality")

    # drop belgian cities (as there is only nominal data available for them)
    tb = tb.drop(columns=["antwerp_nom_rent", "bruges_nom_rent", "brussels_nom_rent", "ghent_nom_rent", "belgium_cpi"])

    # Process data.
    tb = tb.melt(id_vars="year")

    # Split the variable column into region and type
    tb[["country", "type"]] = tb["variable"].str.extract(r"(.+?)_(real_rent|real_wage|affordability)")

    tb = tb.drop(columns="variable")
    tb = tb.dropna(subset=["type"])

    tb = tb.pivot(index=["year", "country"], columns="type", values="value").reset_index()

    tb_quality = tb_quality.rename(columns={"city": "country"})

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)
    tb_quality = geo.harmonize_countries(
        df=tb_quality, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Improve table format.
    tb = tb.format(["country", "year"])
    tb_quality = tb_quality.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_quality], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
