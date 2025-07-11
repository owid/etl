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

    # Read table from meadow dataset.
    tb = ds_meadow.read("korevaar")

    # drop belgian cities
    tb = tb.drop(columns=["antwerp_nom_rent", "bruges_nom_rent", "brussels_nom_rent", "ghent_nom_rent", "belgium_cpi"])

    # Process data.
    #

    tb = tb.melt(id_vars="year")

    # Split the variable column into region and type
    tb[["country", "type"]] = tb["variable"].str.extract(r"(.+?)_(real_rent|real_wage|affordability)")

    tb = tb.drop(columns="variable")
    tb = tb.dropna(subset=["type"])

    tb = tb.pivot(index=["year", "country"], columns="type", values="value").reset_index()

    tb.columns = ["year", "country", "affordability", "real_rent", "real_wage"]

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
