import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_SPLIT = 2024
COLUMNS_INDEX = ["country", "year", "sex", "age", "variant"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_garden = paths.load_dataset("un_wpp")
    #
    # Process data.
    #
    tb = ds_garden["births"].reset_index()

    # Get only the estimates data
    tb = tb[(tb["variant"] == "estimates") & (tb["sex"] == "all")]
    tb = tb.drop(columns=["variant", "birth_rate", "sex"])
    # Get a separate table for all births, so it can be merged as a column
    msk = tb["age"] == "all"
    tb_all = tb[msk]
    tb = tb[~msk]
    # Move each age-group to a decade
    dict_age = {
        "10-14": "10-19",
        "15-19": "10-19",
        "20-24": "20-29",
        "25-29": "20-29",
        "30-34": "30-39",
        "35-39": "30-39",
        "40-44": "40-49",
        "45-49": "40-49",
        "50-54": "50-54",
    }
    tb["decadal_age"] = tb["age"].map(dict_age)
    tb = tb.groupby(["country", "year", "decadal_age"])["births"].sum().reset_index()

    tb_all = tb_all.rename(columns={"births": "all_births"}).drop(columns=["age"])
    # Combine with original tb
    tb = pr.merge(tb, tb_all, on=["country", "year"])
    tb["share"] = (tb["births"] / tb["all_births"]) * 100
    tb = tb.drop(columns=["all_births"])
    tb = tb.format(["country", "year", "decadal_age"], short_name=paths.short_name)
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    # Save changes in the new garden dataset.
    ds_garden.save()
