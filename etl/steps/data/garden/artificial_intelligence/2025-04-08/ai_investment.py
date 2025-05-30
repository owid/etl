"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_investment.csv")

    # Load data from snapshot.
    tb = snap.read(safe_types=False)

    # Read US consumer prices table from garden dataset.
    ds_us_cpi = paths.load_dataset("us_consumer_prices")

    tb_us_cpi = ds_us_cpi.read("us_consumer_prices")
    tb_us_cpi = tb_us_cpi.reset_index()

    #
    # Process data.
    #

    tb = tb.pivot(index=["Year", "Geographic area"], columns="Investment activity", values="value").reset_index()
    tb = tb.rename(columns={"Geographic area": "country", "Year": "year"})
    tb["Total corporate investment"] = (
        tb["Merger/Acquisition"] + tb["Minority Stake"] + tb["Private Investment"] + tb["Public Offering"]
    )
    cols_to_adjust_for_infaltion = [
        "AI infrastructure/research/governance",
        "AR/VR",
        "AV",
        "Agritech",
        "Business operations",
        "Content creation/translation",
        "Creative, music, video content",
        "Cybersecurity, data protection",
        "Data management, processing",
        "Drones",
        "Ed tech",
        "Energy, oil, and gas",
        "Fintech",
        "Generative AI",
        "Insurtech",
        "IoT",
        "Manufacturing",
        "Marketing, digital ads",
        "Medical and health care",
        "Merger/Acquisition",
        "Minority Stake",
        "NLP, customer support",
        "Private Investment",
        "Public Offering",
        "Quantum computing",
        "Retail",
        "Robotics",
        "Semantic search",
        "Semiconductors",
        "Supply chain",
        "Total corporate investment",
    ]

    tb.loc[:, tb.columns.isin(cols_to_adjust_for_infaltion)] *= 1e9

    # Adjust CPI values so that 2021 is the reference year (2021 = 100)
    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    # Adjust 'all_items' column by the 2021 CPI
    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["all_items"] / cpi_2021
    tb_us_cpi_2021 = tb_us_cpi[["cpi_adj_2021", "year"]].copy()
    tb_cpi_inv = pr.merge(tb, tb_us_cpi_2021, on="year", how="inner")

    for col in tb_cpi_inv[cols_to_adjust_for_infaltion]:
        tb_cpi_inv[col] = round(tb_cpi_inv[col] / tb_cpi_inv["cpi_adj_2021"])

    tb_cpi_inv = tb_cpi_inv.drop("cpi_adj_2021", axis=1)
    tb_cpi_inv = tb_cpi_inv.format(["year", "country"])

    # Split into seperate tables to be able to create datapages by restructuring the data
    tb_generative = tb_cpi_inv.loc[:, ["generative_ai"]].copy()
    tb_generative.metadata.short_name = "ai_investment_generative"

    tb_companies = tb_cpi_inv.loc[:, ["companies"]].copy()
    tb_companies.metadata.short_name = "ai_new_companies"

    tb_private_investment = create_private_investment_table(tb_cpi_inv)
    tb_corporate_investment = create_corporate_investment(tb_cpi_inv)

    tb_total_privata_data_page = tb_cpi_inv.loc[:, ["private_investment"]].copy()
    tb_total_privata_data_page.metadata.short_name = "ai_total_investment_private"

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[
            tb_generative,
            tb_private_investment,
            tb_corporate_investment,
            tb_companies,
            tb_total_privata_data_page,
        ],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_private_investment_table(tb):
    tb = tb.reset_index()
    industries = {
        "ai_infrastructure_research_governance": "AI infrastructure and governance",
        "ar_vr": "Augmented or virtual reality",
        "av": "Autonomous vehicles",
        "agritech": "Agricultural technology",
        "business_operations": "Business operations",
        "content_creation_translation": "Content creation and translation",
        "creative__music__video_content": "Creative, music, and video content",
        "cybersecurity__data_protection": "Cybersecurity and data protection",
        "data_management__processing": "Data management and processing",
        "drones": "Drones",
        "ed_tech": "Educational technology",
        "energy__oil__and_gas": "Energy, oil, and gas",
        "fintech": "Financial technology",
        "insurtech": "Insurance technology",
        "iot": "Internet of Things",
        "manufacturing": "Manufacturing",
        "marketing__digital_ads": "Marketing and digital ads",
        "medical_and_health_care": "Medical and healthcare",
        "nlp__customer_support": "Natural Language Processing and customer support",
        "quantum_computing": "Quantum computing",
        "retail": "Retail",
        "robotics": "Robotics",
        "semantic_search": "Semantic search",
        "semiconductors": "Semiconductors",
        "supply_chain": "Supply chain",
        "private_investment": "Total",
    }
    tb = tb[list(industries.keys()) + ["year", "country"]]

    tb = tb.melt(id_vars=["year", "country"], var_name="investment_type", value_name="value")
    tb["investment_type"] = tb["investment_type"].replace(industries)
    tb = tb.pivot(index=["year", "investment_type"], columns="country", values="value").reset_index()
    tb = tb.rename(columns={"investment_type": "country"})

    tb = tb.format(["year", "country"])
    tb.metadata.short_name = "ai_private_investment"

    return tb


def create_corporate_investment(tb: Table) -> Table:
    tb = tb.reset_index()
    industries = {
        "merger_acquisition": " Merger/acquisition",
        "minority_stake": "Minority stake",
        "private_investment": "Private investment",
        "public_offering": "Public offering",
        "total_corporate_investment": "Total",
    }
    tb = tb[list(industries.keys()) + ["year", "country"]]

    tb = tb.melt(id_vars=["year", "country"], var_name="investment_type", value_name="value")
    tb["investment_type"] = tb["investment_type"].replace(industries)
    tb = tb.pivot(index=["year", "investment_type"], columns="country", values="value").reset_index()
    tb = tb.rename(columns={"investment_type": "country"})

    # Only keep the world data (others are NaN for corporate investment)
    tb = tb[["year", "country", "World"]]

    tb = tb.format(["year", "country"])
    tb.metadata.short_name = "ai_corporate_investment"
    return tb
