"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("invasive_species")

    # Read table from meadow dataset.
    tb_cont = ds_meadow.read("continental")
    tb_cont = tb_cont.rename(columns={"continent": "country"})

    tb_glob = ds_meadow.read("global")
    # Combine the global and continental datasets
    tb = pr.concat([tb_cont, tb_glob])
    # Not clear from the paper what this group includes, and there aren't many of them so I'll drop it for now
    tb = tb.drop(columns=["arthropods_p_p__myriapods__diplopods_etc"])
    # Add cumulative sum for each species in each region
    cols = tb.columns.drop(["country", "year"]).tolist()
    for col in cols:
        tb[f"{col}_cumulative"] = tb.groupby("country", observed=True)[col].transform(lambda x: x.fillna(0).cumsum())

    # Process data.
    #
    tb = tb.format(["country", "year"], short_name="invasive_species")

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
