"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ai_investment")

    # Read table from garden dataset.
    tb = ds_garden["ai_investment"].reset_index()

    industries = {
        "ai_infrastructure_research_governance": "AI infrastructure and governance",
        "ar_vr": "Augmented or virtual reality",
        "av": "Autonomous vehicles",
        "agritech": "Agricultural technology",
        "creative__music__video_content": "Creative, music, and video content",
        "cybersecurity__data_protection": "Cybersecurity and data protection",
        "data_management__processing": "Data management and processing",
        "drones": "Drones",
        "ed_tech": "Educational technology",
        "energy__oil__and_gas": "Energy, oil, and gas",
        "entertainment": "Entertainment",
        "facial_recognition": "Facial recognition",
        "fintech": "Financial technology",
        "fitness_and_wellness": "Fitness and wellness",
        "quantum_computing": "Quantum computing",
        "hardware": "Hardware",
        "insurtech": "Insurance technology",
        "retail": "Retail",
        "semiconductor": "Semiconductor",
        "vc": "Venture capital",
        "legal_tech": "Legal technology",
        "manufacturing": "Manufacturing",
        "marketing__digital_ads": "Marketing and digital ads",
        "medical_and_healthcare": "Medical and healthcare",
        "nlp__customer_support": "Natural Language Processing and customer support",
        "private_investment": "Total",
    }
    tb = tb[list(industries.keys()) + ["year", "country"]]

    tb = tb.melt(id_vars=["year", "country"], var_name="investment_type", value_name="value")
    tb["investment_type"] = tb["investment_type"].replace(industries)
    tb = tb.pivot(index=["year", "investment_type"], columns="country", values="value").reset_index()
    tb = tb.rename(columns={"investment_type": "country"})

    tb = tb.format(["year", "country"], paths.short_name)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
