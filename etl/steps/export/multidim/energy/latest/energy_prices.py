from itertools import product

from structlog import get_logger

from etl import multidim
from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Initialize logger.
log = get_logger()


def generate_combinations_with_config(config, tables, dimensions_order_in_slug=None, warn_on_missing_combinations=True):
    dimensions = config.get("dimensions", [])

    # Extract all choices for each dimension as (slug, choice_slug) pairs
    choices = {dim["slug"]: [choice["slug"] for choice in dim["choices"]] for dim in dimensions}
    dimension_slugs_in_config = set(choices.keys())

    # Sanity check for dimensions_order_in_slug
    if dimensions_order_in_slug:
        dimension_slugs_in_order = set(dimensions_order_in_slug)

        # Check if any slug in the order is missing from the config
        missing_slugs = dimension_slugs_in_order - dimension_slugs_in_config
        if missing_slugs:
            raise ValueError(
                f"The following dimensions are in 'dimensions_order_in_slug' but not in the config: {missing_slugs}"
            )

        # Check if any slug in the config is missing from the order
        extra_slugs = dimension_slugs_in_config - dimension_slugs_in_order
        if extra_slugs:
            log.warning(
                f"The following dimensions are in the config but not in 'dimensions_order_in_slug': {extra_slugs}"
            )

        # Reorder choices to match the specified order
        choices = {dim: choices[dim] for dim in dimensions_order_in_slug if dim in choices}

    # Generate all combinations of the choices
    all_combinations = list(product(*choices.values()))

    # Create the resulting structure
    results = []
    for combination in all_combinations:
        # Map dimension slugs to the chosen values
        dimension_mapping = {dim_slug: choice for dim_slug, choice in zip(choices.keys(), combination)}
        slug_combination = "_".join(combination)

        # Find relevant tables for the current combination
        relevant_table = []
        for table in tables:
            if slug_combination in table:
                relevant_table.append(table)

        # Handle missing or multiple table matches
        if len(relevant_table) == 0:
            if warn_on_missing_combinations:
                log.warning(f"Combination {slug_combination} not found in tables")
            continue
        elif len(relevant_table) > 1:
            log.warning(f"Combination {slug_combination} found in multiple tables: {relevant_table}")

        # Construct the indicator path
        indicator_path = (
            f"{relevant_table[0].metadata.dataset.uri}/{relevant_table[0].metadata.short_name}#{slug_combination}"
        )
        indicators = {
            "y": indicator_path,
        }
        # Append the combination to results
        results.append({"dimensions": dimension_mapping, "indicators": indicators})

    return results


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load Eurostat data on gas and electricity prices.
    ds_grapher = paths.load_dataset("energy_prices")

    # Read table of prices in euros.
    tb_annual = ds_grapher.read("energy_prices_annual")
    tb_monthly = ds_grapher.read("energy_prices_monthly")

    #
    # Process data.
    #
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # Create views.
    config["views"] = generate_combinations_with_config(
        config,
        tables=[tb_annual, tb_monthly],
        dimensions_order_in_slug=("frequency", "source", "consumer", "price_component", "unit"),
        warn_on_missing_combinations=False,
    )

    #
    # Save outputs.
    #
    multidim.upsert_multidim_data_page(slug="mdd-energy-prices", config=config, engine=get_engine())
