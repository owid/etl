"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# At OWID we use a mixture of level 2 and 3 GBD causes of death, to limit the number of causes shown on a chart.
# These are the causes of death we show in our causes of death charts e.g. https://ourworldindata.org/grapher/causes-of-death-in-children-under-5
OWID_HIERARCHY = [
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


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("gbd_cause")

    # Read table from meadow dataset.
    tb = ds_garden.read("gbd_cause_deaths")
    # Only keep the number of deaths for this calculation and children under five
    tb = tb[(tb["age"] == "<5 years") & (tb["metric"] == "Number")]
    tb = tb.drop(columns=["metric", "age"])

    # Exclude higher level causes of death but keep the subcategories
    tb = tb[tb["cause"].isin(OWID_HIERARCHY)]
    # Add more succinct disease names
    disease_dict = {
        "Neonatal encephalopathy due to birth asphyxia and trauma": "Asphyxia and trauma",
        "Neonatal preterm birth": "Preterm birth",
        "Exposure to forces of nature": "Natural disasters",
        "Neoplasms": "Cancer",
    }

    tb["cause"] = tb["cause"].replace(disease_dict)

    # Group by 'country', 'year', 'sex', and 'age_group' and find the cause with the maximum number of deaths
    tb = tb.loc[tb.groupby(["country", "year"])["value"].idxmax()]
    tb = tb.drop(columns=["value"])

    # Format the table
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
