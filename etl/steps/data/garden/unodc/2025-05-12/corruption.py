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
    ds_meadow = paths.load_dataset("corruption")

    # Read table from meadow dataset.
    tb = ds_meadow.read("corruption")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Exclude Sweden data for "Other acts of corruption" and "Corruption" before 2015 as suggested by the data provider.
    tb = tb[~((tb["country"] == "Sweden") & (tb["category"] != "Bribery") & (tb["year"] < 2015))]

    # Improve table format.
    tb = tb.drop(["dimension", "sex", "age"], axis=1)
    tb["category"] = tb["category"].replace(
        {
            "Corruption: Other acts of corruption": "Other acts of corruption",
            "Corruption: Bribery": "Bribery",
        }
    )
    # Keep only the specified categories
    tb = tb[tb["category"].isin(["Corruption", "Bribery", "Other acts of corruption"])]

    # Use american spelling for "offences"
    tb["indicator"] = tb["indicator"].replace(
        {
            "Offences": "offenses",
        }
    )

    tb = tb.format(["country", "year", "indicator", "category", "unit_of_measurement"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
