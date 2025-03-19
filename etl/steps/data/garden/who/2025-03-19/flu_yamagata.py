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
    ds_meadow = paths.load_dataset("flunet")

    # Read table from meadow dataset.
    tb = ds_meadow.read("flunet")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    tb = tb[["country", "iso_weekstartdate", "iso_year", "byam"]]
    # tb_agg = tb.groupby(["country", "iso_weekstartdate", "iso_year"]).sum().reset_index()
    tb_world = tb.groupby(["iso_weekstartdate"])["byam"].sum().reset_index()
    tb_world["country"] = "World"
    tb_world.rename(columns={"iso_weekstartdate": "date"}, inplace=True)

    # Improve table format.
    tb_world = tb_world.format(["country", "date"], short_name="flu_yamagata")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_world], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
