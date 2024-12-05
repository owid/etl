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
    ds_meadow = paths.load_dataset("microbe_amr")
    ds_total = paths.load_dataset("total_syndrome")
    # Read table from meadow dataset.
    tb = ds_meadow.read("microbe_amr")
    tb_total = ds_total.read("total_syndrome")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb_total = geo.harmonize_countries(df=tb_total, countries_file=paths.country_mapping_path)

    # We want three variables, total, amr attributable and amr non-attributable

    tb_amr = (
        tb[tb["counterfactual"] == "Attributable"]
        .rename(columns={"value": "amr_attributable_deaths"}, errors="raise")
        .drop(columns=["lower", "upper"])
    )
    tb_total = tb_total.rename(columns={"value": "total_deaths"}, errors="raise").drop(columns=["lower", "upper"])

    tb = tb_amr.merge(tb_total, on=["country", "year", "infectious_syndrome"], how="inner")
    tb["non_amr_attributable_deaths"] = tb["total_deaths"] - tb["amr_attributable_deaths"]
    # Rename syndromes to be shorter for use in stacked bar charts
    tb = rename_syndromes(tb)

    # Reformatting the data so it can be used in stacked bar charts
    tb = (
        tb.drop(columns=["country"]).rename(columns={"infectious_syndrome": "country"}).drop(columns=["counterfactual"])
    )

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def rename_syndromes(tb: Table) -> Table:
    """
    Rename syndromes to be shorter for use in stacked bar charts.
    Ensure all infectious syndromes are replaced.
    """
    name_dict = {
        "Bloodstream infections": "Bloodstream infections",
        "Lower respiratory infections": "Lower respiratory infections",
        "Diarrhea": "Diarrhea",
        "Meningitis": "Meningitis",
        "Infections of the skin and subcutaneous systems": "Skin infections",
        "Urinary tract infections and pyelonephritis": "Kidney and urinary tract infections",
        "Peritoneal and intra-abdominal infections": "Abdominal infections",
        "Tuberculosis": "Tuberculosis",
        "Endocarditis": "Endocarditis",
        "Typhoid fever, paratyphoid fever, and invasive non-typhoidal Salmonella": "Typhoid, paratyphoid, and iNTS",
        "Infections of bones, joints, and related organs": "Bone and joint infections",
        "Other unspecified site infections": "Other infections",
        "Other parasitic infections": "Other parasitic infections",
        "Oral infections": "Oral infections",
        "Myelitis, meningoencephalitis, and other central nervous system infections": "Central nervous system infections",
        "Upper respiratory infections": "Upper respiratory infections",
        "Hepatitis": "Hepatitis",
        "Eye infections": "Eye infections",
        "Encephalitis": "Encephalitis",
        "Carditis, myocarditis, and pericarditis": "Heart inflammation",
        "Sexually transmitted infections": "Sexually transmitted infections",
    }

    # Find unmatched syndromes
    unmatched_syndromes = set(tb["infectious_syndrome"].unique()) - set(name_dict.keys())
    if unmatched_syndromes:
        raise ValueError(f"The following syndromes were not found in the name dictionary: {unmatched_syndromes}")

    # Replace syndromes
    tb["infectious_syndrome"] = tb["infectious_syndrome"].replace(name_dict)

    return tb
