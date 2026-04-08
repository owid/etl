"""Load a garden dataset and create a grapher dataset."""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping from long question names to short display names.
# For comparison, I added the original phrasing used in the Faunalytics visualization:
# https://faunalytics.org/public-acceptability-of-standard-u-s-animal-agriculture-practices/
QUESTION_RENAMING = {
    # Broiler chickens
    # Original: "Chickens kept indoors, <1 sq ft space"
    "Keeping chickens inside a barn with no outdoor access and with less than one square foot of space per bird": "Chickens are kept indoors with little space per bird",
    # Original: "Chickens grown fast, struggle to walk"
    "Growing chickens to reach market weight size by 47 days, leading to difficulties with walking and standing": "Chickens are bred to grow fast and struggle to walk and stand",
    # Original: "Live chickens hung, stunned, throats slit, then boiled"
    "Hanging live chickens upside down by their legs before stunning them in electrified water, slitting their throats, and finally submerging them in boiling water": "Live chickens are hung upside down, stunned, have their throats slit, then boiled",
    # Cows
    # Original: "Calves castrated without pain relief"
    "Castrating new-born calves by surgically removing the testicles, almost always without anesthesia or pain-killers": "Newborn calves are castrated without pain relief",
    # Original: "Calves' horn buds removed, sometimes no pain relief"
    "Removing calves' horn buds (undeveloped horn tissue before it grows into a visible horn) using a knife or hot iron, sometimes without anesthesia or pain-killers": "Calves' horn buds are removed with a knife or hot iron, sometimes without pain relief",
    # Original: "Calves separated from mothers at birth"
    "Permanently separating calves from their mothers immediately after birth": "Calves are permanently separated from their mothers at birth",
    # Laying hens
    # Original: "Chickens' beaks cut off, no pain relief"
    "Cutting the beaks off new-born chickens, almost always without anesthesia or pain-killers": "Newborn chickens' beaks are cut off without pain relief",
    # Original: "Male chicks ground alive"
    "Killing new-born male chicks who can't lay eggs by use of meat-grinders": "Newborn male chicks are killed in meat-grinders",
    # Original: "Chickens in tiny cages (67-86 sq inches)"
    "Keeping chickens in cages with 67-86 square inches of space per bird (smaller than a standard sheet of letter paper)": "Chickens are kept in cages, with less room than a sheet of paper per bird",
    # Pigs
    # Original: "Pigs caged, unable to turn for weeks"
    "Keeping pigs in cages which prevent them from turning around for several weeks": "Pigs are kept in cages, unable to turn around for weeks",
    # Original: "Pigs killed with CO2 gas"
    "Killing pigs in gas chambers by use of carbon dioxide (CO2) gas": "Pigs are killed in CO₂ gas chambers",
    # Original: "Piglets' tails cut off, no pain relief"
    "Cutting the tails off new-born piglets, almost always without anesthesia or pain-killers": "Newborn piglets' tails are cut off without pain relief",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("acceptability_of_us_farming_practices")

    # Read table from garden dataset.
    tb = ds_garden.read("acceptability_of_us_farming_practices")

    #
    # Process data.
    #
    # Map long question names to shorter display names.
    tb["question"] = map_series(
        series=tb["question"],
        mapping=QUESTION_RENAMING,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Adapt to grapher format.
    tb = tb.rename(columns={"question": "country"})

    # Add a year column.
    tb["year"] = int(tb["country"].metadata.origins[0].date_published.split("-")[0])

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])

    # Save grapher dataset.
    ds_grapher.save()
