"""Load a meadow dataset and create a garden dataset."""

import datetime

from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("exoplanets.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("exoplanets")

    # Read table from meadow dataset.
    tb = ds_meadow["exoplanets"].reset_index()

    #
    # Process data.
    #
    log.info("exoplanets.harmonize_countries")

    tb = tb[tb.disc_year < datetime.date.today().year]

    # Clean discovery methods
    # Keep the 3 top discovery methods, and label the rest as "Other methods"
    tb["discoverymethod"] = tb.discoverymethod.astype(str)
    top_methods = [*tb.discoverymethod.value_counts().head(3).index]
    tb.loc[-tb.discoverymethod.isin(top_methods), "discoverymethod"] = "Other methods"
    # Capitalize the discovery methods
    tb["discoverymethod"] = tb.discoverymethod.str.capitalize()

    # Count discoveries by year and method
    tb = tb.groupby(["disc_year", "discoverymethod"], as_index=False).size().copy()

    # Pivot then melt dataset to ensure all combinations of year & method are present
    tb = (
        tb.pivot(columns="discoverymethod", index="disc_year", values="size")
        .fillna(0)
        .reset_index()
        .melt(id_vars="disc_year", var_name="discoverymethod", value_name="N")
    )

    # Calculate cumulative exoplanets by method
    tb = tb.sort_values("disc_year")
    tb["cumulative_exoplanets"] = tb.groupby("discoverymethod").N.cumsum().astype(int)
    tb = tb.drop(columns="N")

    # Rename columns
    tb = tb.rename(columns={"discoverymethod": "country", "disc_year": "year"}).reset_index(drop=True)
    tb.metadata.short_name = "exoplanets"

    tb = tb.set_index(["year", "country"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("exoplanets.end")
