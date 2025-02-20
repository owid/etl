"""Load garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions.
paths = PathFinder(__file__)

DIMENSIONS = {
    "frequency": ["annual", "monthly"],
    "source": ["electricity", "gas"],
    "consumer": ["household", "non_household", "all"],
    "price_component": [
        "total_price_including_taxes",
        "wholesale",
        "consumer_price_components",
        "energy_and_supply",
        "network_costs",
        "taxes_fees_levies_and_charges",
        "capacity_taxes",
        "environmental_taxes",
        "nuclear_taxes",
        "renewable_taxes",
        "value_added_tax_vat",
        "other",
    ],
    "unit": ["euro", "pps"],
}


def run(dest_dir: str) -> None:
    # Load tables from garden dataset.
    ds_garden = paths.load_dataset("energy_prices")
    tb_annual = ds_garden.read("energy_prices_annual", reset_index=False)
    tb_monthly = ds_garden.read("energy_prices_monthly", reset_index=False)

    # Add dimensions info to metadata.

    # Create a new grapher dataset.
    dataset = create_dataset(
        dest_dir=dest_dir,
        tables=[tb_annual, tb_monthly],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )
    dataset.save()


def add_dimensions_to_metadata(tb: Table, dim_options: Dict[str, List[str]]) -> List[str]:
    # Generate all combinations of the choices.
    all_combinations = list(product(*choices.values()))

    # Create the views.
    results = []
    for combination in all_combinations:
        # Map dimension slugs to the chosen values.
        dimension_mapping = {dim_slug: choice for dim_slug, choice in zip(choices.keys(), combination)}
        slug_combination = "_".join(combination)

    for col in tb:
        filters = []
        for dim, values in dim_options.items():
            filters.append({"name": dim, "value": [v for v in values if v in col][0]})

        tb[col].metadata.additional_info = {
            "dimensions": {
                "originalShortName": col.split("_")[0],
                "short_name": col,
                "filters": filters,
            }
        }

        use_cols.append(col)

    return use_cols
