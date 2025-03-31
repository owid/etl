"""Load a meadow dataset and create a garden dataset."""

from typing import Any, List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("gbd_child_mortality")

    # Read table from meadow dataset.
    tb = ds_garden.read("gbd_child_mortality_deaths")
    # At OWID we use a mixture of level 2 and 3 GBD causes of death, to limit the number of causes shown on a chart.
    # These are the causes of death we show in our causes of death charts e.g. https://ourworldindata.org/grapher/causes-of-death-in-children-under-5
    under_five = [
        "Lower respiratory infections",
        "Invasive Non-typhoidal Salmonella (iNTS)",
        "Interpersonal violence",
        "Nutritional deficiencies",
        "Acute hepatitis",
        "Neoplasms",
        "Measles",
        "Digestive diseases",
        "Cirrhosis and other chronic liver diseases",
        "Chronic kidney disease",
        "Cardiovascular diseases",
        "Congenital birth defects",
        "Neonatal preterm birth",
        "Environmental heat and cold exposure",
        "Neonatal sepsis and other neonatal infections",
        "Exposure to forces of nature",
        "Diabetes mellitus",
        "Neonatal encephalopathy due to birth asphyxia and trauma",
        "Meningitis",
        "Other neonatal disorders",
        "Whooping cough",
        "Diarrheal diseases",
        "Fire, heat, and hot substances",
        "Road injuries",
        "Tuberculosis",
        "HIV/AIDS",
        "Drowning",
        "Malaria",
        "Syphilis",
    ]
    # Exclude higher level causes of death but keep the subcategories
    tb = tb[tb["cause"].isin(under_five)]
    # Add more succinct disease names
    disease_dict = {
        "Neonatal encephalopathy due to birth asphyxia and trauma": "Asphyxia and trauma",
        "Neonatal preterm birth": "Preterm birth",
        "Exposure to forces of nature": "Natural disasters",
        "Neoplasms": "Cancer",
    }

    tb["cause"] = tb["cause"].replace(disease_dict)
    # Only keep the number of deaths for this calculation
    tb = tb[tb["metric"] == "Number"]

    # Replace values in "sex" column more descriptive values
    tb["sex"] = tb["sex"].replace({"Both": "children", "Male": "boys", "Female": "girls"})
    # Group by 'country', 'year', 'sex', and 'age_group' and find the cause with the maximum number of deaths
    tb = tb.loc[tb.groupby(["country", "year", "sex", "age"])["value"].idxmax()]
    tb = tb.drop(columns=["value", "metric"])
    # Filter rows where cause is "Malaria" and year is 2021
    malaria_2021 = tb[
        (tb["cause"] == "Malaria") & (tb["year"] == 2021) & (tb["sex"] == "children") & (tb["age"] == "<5 years")
    ]

    # Print the unique countries
    print("Countries where cause is 'Malaria' in 2021:")
    for malaria in malaria_2021["country"].unique():
        print(malaria)
    # Format the table
    tb = tb.format(["country", "year", "age", "sex"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
