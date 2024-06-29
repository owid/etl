"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_investment.csv")

    # Load data from snapshot.
    tb = snap.read()

    # Read US consumer prices table from garden dataset.
    ds_us_cpi = paths.load_dataset("us_consumer_prices")

    tb_us_cpi = ds_us_cpi["us_consumer_prices"]
    tb_us_cpi = tb_us_cpi.reset_index()

    #
    # Process data.
    #

    tb = tb.pivot(index=["Year", "Geographic area"], columns="Investment type", values="value").reset_index()
    tb = tb.rename(columns={"Geographic area": "country", "Year": "year"})

    cols_to_adjust_for_infaltion = [
        "AI infrastructure/research/governance",
        "AR/VR",
        "AV",
        "Agritech",
        "Creative, music, video content",
        "Cybersecurity, data protection",
        "Data management, processing",
        "Drones",
        "Ed tech",
        "Energy, oil, and gas",
        "Entertainment",
        "Facial recognition",
        "Fintech",
        "Fitness and wellness",
        "Generative AI",
        "Hardware",
        "Insurtech",
        "Legal tech",
        "Manufacturing",
        "Marketing, digital ads",
        "Medical and healthcare",
        "Merger/Acquisition",
        "Minority Stake",
        "NLP, customer support",
        "Private Investment",
        "Public Offering",
        "Quantum computing",
        "Retail",
        "Semiconductor",
        "VC",
    ]

    tb.loc[:, tb.columns.isin(cols_to_adjust_for_infaltion)] *= 1e9

    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]

    # Adjust 'fp_cpi_totl' column by the 2021 CPI
    tb_us_cpi["cpi_adj_2021"] = 100 * tb_us_cpi["all_items"] / cpi_2021
    tb_us_cpi_2021 = tb_us_cpi[["cpi_adj_2021", "year"]].copy()
    tb_cpi_inv = pr.merge(tb, tb_us_cpi_2021, on="year", how="inner")

    for col in tb_cpi_inv[cols_to_adjust_for_infaltion]:
        tb_cpi_inv[col] = round(100 * tb_cpi_inv[col] / tb_cpi_inv["cpi_adj_2021"])

    tb_cpi_inv = tb_cpi_inv.drop("cpi_adj_2021", axis=1)

    tb_cpi_inv = tb_cpi_inv.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_cpi_inv], check_variables_metadata=True, default_metadata=snap.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
