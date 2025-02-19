"""Load a garden dataset and create a grapher dataset."""

from datetime import datetime

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("measles_long_run")

    # Read table from garden dataset.
    tb = ds_garden.read("measles_long_run", reset_index=False)
    assert tb["cases"].metadata.origins[1].title == "CDC Yearly measles cases (1985-present)"
    publication_date = tb["cases"].metadata.origins[1].date_published
    desc_short = format_description_short(publication_date)
    tb["cases"].metadata.description_short = desc_short
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def format_description_short(publication_date: str) -> str:
    date_obj = datetime.strptime(publication_date, "%Y-%m-%d")
    formatted_date = date_obj.strftime("%d %B %Y")

    return f"Reported measles cases. The date for {datetime.now().year} is incomplete and was last updated {formatted_date}"
