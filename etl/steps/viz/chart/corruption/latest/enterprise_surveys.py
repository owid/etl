"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
MULTIDIM_CONFIG = {
    "hasMapTab": True,
    "tab": "map",
    "chartTypes": ["DiscreteBar"],
    "originUrl": "ourworldindata.org/corruption",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("enterprise_surveys")
    tb = ds.read("enterprise_surveys", load_data=False)

    cols_to_drop = [
        "bribery_depth__pct_of_public_transactions_where_a_gift_or_informal_payment_was_requested",
        "percent_of_firms_identifying_corruption_as_a_major_or_very_severe_constraint",
    ]
    tb = tb.drop(columns=cols_to_drop)

    tb = adjust_dimensions_corruption(tb)
    # Create collection object
    #
    c = paths.create_collection(
        config=config,
        tb=tb,
        common_view_config=MULTIDIM_CONFIG,
    )

    # Add grouped view
    c.group_views(
        groups=[
            {
                "dimension": "service",
                "choice_new_slug": "side_by_side",
                "view_config": {
                    "hasMapTab": False,
                    "chartTypes": ["DiscreteBar"],
                    "tab": "chart",
                    "facettingLabelByYVariables": "service",
                    "selectedFacetStrategy": "entity",
                    "title": "Share of firms that were asked to pay a bribe, by interaction",
                    "subtitle": "Share of firms that were asked to pay a bribe, by type of interaction with public officials.",
                },
                "view_metadata": {
                    "description_short": "The percentage of businesses that encountered a bribe request when dealing with six public services - such as import or operating licenses, construction permits, utility connections, and dealings with tax officials.",
                    "presentation": {"title_public": "Share of firms that were asked to pay a bribe, by interaction"},
                },
            },
        ]
    )
    # Sort choices alphabetically
    c.sort_choices({"service": lambda x: sorted(x)})

    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions_corruption(tb):
    """
    Add dimensions to corruption-related table columns.

    It adds fields:
    - `sector` (what the bribe was for)


    And updates `original_short_name` to 'bribery_prevalence' for all.
    """

    service_mapping = {
        "bribery_incidence__percent_of_firms_experiencing_at_least_one_bribe_payment_request": "At least one bribe request",
        "percent_of_firms_expected_to_give_gifts_to_get_a_construction_permit": "Construction permit",
        "percent_of_firms_expected_to_give_gifts_to_get_an_electrical_connection": "Electrical connection",
        "percent_of_firms_expected_to_give_gifts_to_secure_government_contract": "Government contract",
        "percent_of_firms_expected_to_give_gifts_to_get_an_import_license": "Import license",
        "percent_of_firms_expected_to_give_gifts_to_get_an_operating_license": "Operating license",
        "percent_of_firms_expected_to_give_gifts_in_meetings_with_tax_officials": "Tax officials",
        "percent_of_firms_expected_to_give_gifts_to_public_officials_to_get_things_done": 'To "get things done"',
        "percent_of_firms_expected_to_give_gifts_to_get_a_water_connection": "Water connection",
    }

    indicator_name = "bribery_prevalence"
    # Ensure your DataFrame is called df (or update accordingly)
    desired_order = list(service_mapping.keys())

    # Reorder columns: keep only those in the mapping, and preserve others if needed
    tb = tb.loc[
        :, [col for col in desired_order if col in tb.columns] + [col for col in tb.columns if col not in desired_order]
    ]

    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        if col not in service_mapping:
            raise Exception(f"Column '{col}' not recognized in service mapping")

        # Set short name
        tb[col].metadata.original_short_name = indicator_name
        tb[col].metadata.dimensions = {}

        # Set dimensions
        tb[col].metadata.dimensions["service"] = service_mapping[col]

    # Add dimension definitions at table level
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.extend(
            [
                {"name": "Service", "slug": "service"},
            ]
        )

    return tb
