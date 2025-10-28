"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("patents")

    # Read table from meadow dataset.
    tb = ds_meadow.read("patents")

    tb = tb.drop(columns=["office__code", "origin"])

    # remove leading _ from columns:
    tb.columns = [col.lstrip("_") for col in tb.columns]

    tb = tb.melt(id_vars=["office", "field_of_technology"], var_name="year", value_name="patent_count")

    tb = tb.pivot(index=["office", "year"], columns="field_of_technology", values="patent_count").reset_index()

    # rename patent columns:
    tb = tb.rename(
        columns={
            "13 - Medical technology": "medical_technology_patents",
            "15 - Biotechnology": "biotechnology_patents",
            "16 - Pharmaceuticals": "pharmaceutical_patents",
        }
    )

    # remove entries with no patents:
    tb = tb.dropna(subset=["medical_technology_patents", "biotechnology_patents", "pharmaceutical_patents"], how="all")


    tb = tb.rename(columns={"office": "country"})
    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
