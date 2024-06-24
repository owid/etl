"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from shared import add_regional_aggregates

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
AGE_GROUPS_RANGES = {
    "All ages": [0, None],
    "<5 years": [0, 4],
    "5-14 years": [5, 14],
    "15-49 years": [15, 49],
    "50-69 years": [50, 69],
    "70+ years": [70, None],
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gbd_cause")

    # Read table from meadow dataset.
    tb = ds_meadow["gbd_cause"].reset_index()
    ds_regions = paths.load_dataset("regions")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Add regional aggregates
    tb = add_regional_aggregates(
        tb=tb,
        ds_regions=ds_regions,
        index_cols=["country", "year", "metric", "measure", "cause", "age"],
        regions=REGIONS,
        age_group_mapping=AGE_GROUPS_RANGES,
    )

    # Split into two tables: one for deaths, one for DALYs
    tb_deaths = tb[tb["measure"] == "Deaths"].copy()
    tb_dalys = tb[tb["measure"] == "DALYs (Disability-Adjusted Life Years)"].copy()
    # Shorten the metric name for DALYs
    tb_dalys["measure"] = "DALYs"

    # Drop the measure column
    tb_deaths = tb_deaths.drop(columns="measure")
    tb_dalys = tb_dalys.drop(columns="measure")

    # Add all forms of violence together - for Deaths only
    tb_deaths = add_all_forms_of_violence(tb_deaths)
    # Create a category for all infectious diseases - for Deaths only
    tb_deaths = add_infectious_diseases(tb_deaths)
    # Aggregate all cancers which cause less than 200,000 deaths a year - for Deaths only
    tb_deaths = add_cancer_other_aggregates(tb_deaths)
    # Format the tables
    tb_deaths = tb_deaths.format(["country", "year", "metric", "age", "cause"], short_name="gbd_cause_deaths")
    tb_dalys = tb_dalys.format(["country", "year", "metric", "age", "cause"], short_name="gbd_cause_dalys")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_deaths, tb_dalys], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_cancer_other_aggregates(tb: Table) -> Table:
    """
    We want a chart showing changes in the deaths from cancer over time, but we don't want to show every single type of cancer. We'll aggregate all cancers which cause less than 200,000 deaths a year
    """
    cancers = [
        "Eye cancer",
        "Soft tissue and other extraosseous sarcomas",
        "Malignant neoplasm of bone and articular cartilage",
        "Neuroblastoma and other peripheral nervous cell tumors",
        "Breast cancer",
        "Cervical cancer",
        "Uterine cancer",
        "Prostate cancer",
        "Colon and rectum cancer",
        "Lip and oral cavity cancer",
        "Nasopharynx cancer",
        "Other pharynx cancer",
        "Gallbladder and biliary tract cancer",
        "Pancreatic cancer",
        "Malignant skin melanoma",
        "Non-melanoma skin cancer",
        "Ovarian cancer",
        "Testicular cancer",
        "Kidney cancer",
        "Bladder cancer",
        "Brain and central nervous system cancer",
        "Thyroid cancer",
        "Mesothelioma",
        "Hodgkin lymphoma",
        "Non-Hodgkin lymphoma",
        "Multiple myeloma",
        "Leukemia",
        "Other malignant neoplasms",
        "Other neoplasms",
        "Esophageal cancer",
        "Stomach cancer",
        "Liver cancer",
        "Larynx cancer",
        "Tracheal, bronchus, and lung cancer",
    ]
    cancers_tb = tb[
        (tb["cause"].isin(cancers))
        & (tb["metric"] == "Number")
        & (tb["age"] == "All ages")
        & (tb["country"] == "World")
        & (tb["year"] == tb["year"].max())
    ]
    cancers_to_aggregate = cancers_tb[cancers_tb["value"] < 200000]["cause"].drop_duplicates().tolist()

    tb_cancer = tb[(tb["cause"].isin(cancers_to_aggregate)) & (tb["metric"] == "Number")]
    tb_cancer = tb_cancer.groupby(["country", "age", "metric", "year"], observed=True)["value"].sum().reset_index()
    tb_cancer["cause"] = "Other cancers (OWID)"
    tb = pr.concat([tb, tb_cancer], ignore_index=True)

    return tb


def add_all_forms_of_violence(tb: Table) -> Table:
    """
    Add all forms of violence together
    """
    violence = ["Interpersonal violence", "Conflict and terrorism", "Police conflict and executions"]

    tb_violence = tb[(tb["cause"].isin(violence)) & (tb["age"] == "Age-standardized")]
    assert all(tb_violence["metric"] == "Rate")
    assert all(
        v in tb_violence["cause"].values for v in violence
    ), "Not all elements of 'violence' are present in tb_violence['cause']"

    tb_violence = tb_violence.groupby(["country", "age", "metric", "year"])["value"].sum().reset_index()
    tb_violence["cause"] = "All forms of violence"

    tb = pr.concat([tb, tb_violence], ignore_index=True)

    return tb


def add_infectious_diseases(tb: Table) -> Table:
    """
    Separate out communicable diseases from maternal and neonatal diseases, and nutritional deficiencies
    """

    broad_level_group = ["Communicable, maternal, neonatal, and nutritional diseases"]
    maternal_neonatal_nutritional = ["Maternal and neonatal disorders", "Nutritional deficiencies"]

    tb_broad = tb[tb["cause"].isin(broad_level_group)]
    assert len(tb_broad) > 0, "No rows found for 'Communicable, maternal, neonatal, and nutritional diseases'"

    tb_maternal_neonatal_nutritional = tb[tb["cause"].isin(maternal_neonatal_nutritional)]
    assert len(tb_maternal_neonatal_nutritional["cause"].unique()) == len(
        maternal_neonatal_nutritional
    ), "Not all elements of 'maternal_neonatal_nutritional' are present in tb['cause']"
    tb_maternal_neonatal_nutritional = (
        tb_maternal_neonatal_nutritional.groupby(["country", "age", "metric", "year"], observed=True)["value"]
        .sum()
        .reset_index()
    )

    tb_combine = pr.merge(
        tb_broad,
        tb_maternal_neonatal_nutritional,
        on=["country", "year", "age", "metric"],
        suffixes=("", "_maternal_neonatal_nutritional"),
    )
    tb_infectious = tb_combine.copy()
    tb_infectious["cause"] = "Infectious diseases"
    tb_infectious["value"] = tb_infectious["value"] - tb_infectious["value_maternal_neonatal_nutritional"]
    tb_infectious = tb_infectious.drop(columns="value_maternal_neonatal_nutritional")
    assert all(tb_infectious["value"] >= 0), "Negative values found in 'value' column"

    tb = pr.concat([tb, tb_infectious], ignore_index=True)
    return tb
