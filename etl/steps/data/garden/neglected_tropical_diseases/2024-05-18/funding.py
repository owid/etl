"""
Load a meadow dataset and create a garden dataset.
"""

from typing import List

import numpy as np
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
NTDS = [
    "Buruli ulcer",
    "Dengue",
    "Helminth infections (worms & flukes) - Hookworm (ancylostomiasis & necatoriasis)",
    "Helminth infections (worms & flukes) - Lymphatic filariasis (elephantiasis)",
    "Helminth infections (worms & flukes) - Multiple helminth infections",
    "Helminth infections (worms & flukes) - Onchocerciasis (river blindness)",
    "Helminth infections (worms & flukes) - Roundworm (ascariasis)",
    "Helminth infections (worms & flukes) - Schistosomiasis (bilharziasis)",
    "Helminth infections (worms & flukes) - Strongyloidiasis & other intestinal roundworms",
    "Helminth infections (worms & flukes) - Tapeworm (taeniasis / cysticercosis)",
    "Helminth infections (worms & flukes) - Whipworm (trichuriasis)",
    "Kinetoplastid diseases - Chagas' disease",
    "Kinetoplastid diseases - Leishmaniasis",
    "Kinetoplastid diseases - Multiple kinetoplastid diseases",
    "Kinetoplastid diseases - Sleeping sickness (HAT)",
    "Leprosy",
    "Mycetoma",
    "Scabies",
    "Snakebite envenoming",
    "Trachoma",
    "Yaws",
]


def run(dest_dir: str) -> None:
    log.info("funding.start")
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("funding")

    # Read table from meadow dataset.
    tb = ds_meadow["funding"].reset_index()
    # Group some of the research technologies (products) into broader groups
    tb = aggregate_products(tb)
    # The funding for each disease
    tb_disease = format_table(tb=tb, group=["disease", "year"], index_col=["disease"], short_name="funding_disease")
    # The funding for each product - across all diseases
    tb_product = format_table(tb=tb, group=["product", "year"], index_col=["product"], short_name="funding_product")
    # Funding for each product - across only the NTDs in the dataset
    missing_items = [item for item in NTDS if item not in tb["disease"].values]
    log.info(f"Missing items in the NTD list: {missing_items}, check if they are in the dataset.")
    tb_product_ntd = tb[tb["disease"].isin(NTDS)].copy()
    tb_product_ntd = format_table(
        tb=tb_product_ntd, group=["product", "year"], index_col=["product"], short_name="funding_product_ntd"
    )
    # The funding for each disease*product
    tb_disease_product = format_table(
        tb=tb,
        group=["disease", "product", "year"],
        index_col=["disease", "product"],
        short_name="funding_disease_product",
    )
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_disease, tb_product, tb_disease_product, tb_product_ntd],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def aggregate_products(tb: Table) -> Table:
    """
    Aggregate some of the research technologies (products) into broader groups
    """
    replacement_dict = {
        "Diagnostics": "Diagnostics and diagnostic platforms",
        "General diagnostic platforms & multi-disease diagnostics": "Diagnostics and diagnostic platforms",
        "Biological vector control products": "Vector control products and research",
        "Chemical vector control products": "Vector control products and research",
        "Fundamental vector control research": "Vector control products and research",
    }
    missing_keys = set(replacement_dict.keys()) - set(tb["product"].unique())

    assert len(missing_keys) == 0, f"Missing keys in replacement_dict: {missing_keys}"
    # Going round the houses to replace the values in the product column to aggregate them
    tb["product"] = tb["product"].astype(str).replace(replacement_dict)
    tb["product"] = tb["product"].replace("nan", np.nan)
    tb["product"] = tb["product"].astype("category")

    return tb


def format_table(tb: Table, group: List[str], index_col: List[str], short_name: str) -> Table:
    """
    Formatting original table so that we can have total funding by disease, product and disease*product
    """
    tb = tb.groupby(group, observed=True)["amount__usd"].sum().reset_index()
    tb = tb.dropna(subset=index_col, how="any")
    tb["country"] = "World"
    tb = tb.set_index(["country", "year"] + index_col, verify_integrity=False)
    # tb = tb.pivot(index="year", columns=pivot_col, values="amount__usd")
    tb.metadata.short_name = short_name

    return tb
