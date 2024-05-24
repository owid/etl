"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_prevalence")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_prevalence"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Split into two tables: one for prevalence, one for incidence
    tb_prevalence = tb[tb["measure"] == "Prevalence"].copy()
    tb_incidence = tb[tb["measure"] == "Incidence"].copy()

    # Drop the measure column
    tb_prevalence = tb_prevalence.drop(columns="measure")
    tb_incidence = tb_incidence.drop(columns="measure")

    # Format the tables
    tb_prevalence = tb_prevalence.format(["country", "year", "metric", "age", "cause"], short_name="gbd_prevalence")
    tb_incidence = tb_incidence.format(["country", "year", "metric", "age", "cause"], short_name="gbd_incidence")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_prevalence, tb_incidence],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
