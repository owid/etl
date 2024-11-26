"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("antimicrobial_usage")

    # Read table from meadow dataset.
    tb_class = ds_meadow["class"].reset_index()
    tb_aware = ds_meadow["aware"].reset_index()
    #
    # Process data.
    #
    tb_class = geo.harmonize_countries(df=tb_class, countries_file=paths.country_mapping_path)
    tb_aware = geo.harmonize_countries(df=tb_aware, countries_file=paths.country_mapping_path)

    # Drop columns that are not needed in the garden dataset.
    tb_class = tb_class.drop(
        columns=["whoregioncode", "whoregionname", "countryiso3", "incomeworldbankjune", "atc4", "notes"]
    )
    tb_aware = tb_aware.drop(columns=["whoregioncode", "whoregionname", "incomeworldbankjune", "aware", "notes"])

    # Aggregate by antimicrobial class
    tb_class_agg = aggregate_antimicrobial_classes(tb_class)

    tb_class = tb_class.format(["country", "year", "antimicrobialclass", "atc4name", "routeofadministration"])
    tb_aware = tb_aware.format(["country", "year", "awarelabel"])
    tb_class_agg = tb_class_agg.format(["country", "year", "antimicrobialclass"], short_name="class_aggregated")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_class, tb_aware, tb_class_agg],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def aggregate_antimicrobial_classes(tb_class: Table) -> Table:
    """
    Aggregating by antimicrobial class
    """

    tb_class = tb_class.groupby(["country", "year", "antimicrobialclass"])[["ddd", "did"]].sum().reset_index()

    return tb_class
