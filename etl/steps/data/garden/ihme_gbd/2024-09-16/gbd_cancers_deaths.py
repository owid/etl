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
    ds_garden = paths.load_dataset("gbd_cause")

    # Read table from meadow dataset.
    tb = ds_garden["gbd_cause_deaths"].reset_index()
    #
    # Process data.
    #
    tb = tb[tb["metric"] == "Number"]  # Filter for number of deaths
    tb = tb.drop(columns=["metric"])  # Drop the metric column

    # List of cancers
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

    # Cancers that are already called 'Other cancers' in the dataset, so we'll combine these in to avoid confusing labelling on the chart
    other_cancers = ["Other malignant neoplasms", "Other neoplasms"]

    # Filter the DataFrame for relevant rows
    cancers_tb = tb[
        (tb["cause"].isin(cancers))
        & (tb["age"] == "All ages")
        & (tb["country"] == "World")
        & (tb["year"] == tb["year"].max())
    ]

    # Identify cancers to aggregate
    cancers_to_aggregate = cancers_tb[cancers_tb["value"] < 200000]["cause"].drop_duplicates().tolist()
    cancers_to_aggregate = cancers_to_aggregate + other_cancers

    # Log the cancers that were grouped]
    paths.log.info(f"Cancers grouped into 'Other cancers (OWID)': {', '.join(cancers_to_aggregate)}")

    # Group the identified cancers into "Other cancers (OWID)"
    tb_cancer = tb[(tb["cause"].isin(cancers_to_aggregate))]
    tb_cancer = tb_cancer.groupby(["country", "age", "year"], observed=True)["value"].sum().reset_index()
    tb_cancer["cause"] = "Other cancers (OWID)"

    # Remove the grouped cancers from the original DataFrame but keep other relevant cancers
    tb = tb[tb["cause"].isin(cancers + other_cancers) & ~tb["cause"].isin(cancers_to_aggregate)]

    # Concatenate the new "Other cancers (OWID)" row to the original DataFrame
    tb = pr.concat([tb, tb_cancer], ignore_index=True)

    # Calculate the total number of cancer deaths for each year
    total_cancer_deaths = tb[tb["cause"].isin(cancers)].groupby(["country", "year", "age"])["value"].sum().reset_index()
    total_cancer_deaths = total_cancer_deaths.rename(columns={"value": "total_cancer_deaths"})

    # Merge total cancer deaths with the original data
    tb = tb.merge(total_cancer_deaths, on=["country", "year", "age"])
    tb = tb.rename(columns={"value": "total_deaths"})

    # Calculate the share of each specific cancer from the total cancer deaths
    tb["share_of_cancer_deaths"] = (tb["total_deaths"] / tb["total_cancer_deaths"]) * 100

    # Drop the temporary 'total_cancer_deaths' column
    tb = tb.drop(columns=["total_cancer_deaths", "total_deaths"])
    tb = tb.format(["country", "year", "age", "cause"], short_name="gbd_cancers_deaths")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
        # Table has optimal types already and repacking can be time consuming.
        repack=False,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
