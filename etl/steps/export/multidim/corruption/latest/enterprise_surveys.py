"""Load a meadow dataset and create a garden dataset."""

from owid.catalog.meta import TableDimension

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Default collection config
    config = paths.load_collection_config()

    # Load grapher dataset.
    ds = paths.load_dataset("enterprise_surveys")
    tb = ds.read("enterprise_surveys")

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
    )

    #
    # (optional) Edit views
    #
    for view in c.views:
        # if view.dimension["sex"] == "male":
        #     view.config["title"] = "Something else"
        pass

    #
    # Save garden dataset.
    #
    c.save()


def adjust_dimensions_corruption(tb):
    """
    Add dimensions to corruption-related table columns.

    It adds fields:
    - `stage` (what the bribe was for)


    And updates `original_short_name` to 'bribery_prevalence' for all.
    """

    sector_mapping = {
        "bribery_incidence__percent_of_firms_experiencing_at_least_one_bribe_payment_request": "any",
        "percent_of_firms_expected_to_give_gifts_in_meetings_with_tax_officials": "tax",
        "percent_of_firms_expected_to_give_gifts_to_secure_government_contract": "government_contract",
        "percent_of_firms_expected_to_give_gifts_to_get_an_operating_license": "operating_license",
        "percent_of_firms_expected_to_give_gifts_to_get_an_import_license": "import_license",
        "percent_of_firms_expected_to_give_gifts_to_get_a_construction_permit": "construction_permit",
        "percent_of_firms_expected_to_give_gifts_to_get_an_electrical_connection": "electrical_connection",
        "percent_of_firms_expected_to_give_gifts_to_get_a_water_connection": "water_connection",
        "percent_of_firms_expected_to_give_gifts_to_public_officials_to_get_things_done": "general",
    }

    indicator_name = "bribery_prevalence"

    for col in tb.columns:
        if col in ["country", "year"]:
            continue

        if col not in sector_mapping:
            raise Exception(f"Column '{col}' not recognized in sector mapping")

        # Set short name
        tb[col].metadata.original_short_name = indicator_name
        tb[col].metadata.dimensions = {}

        # Set dimensions
        tb[col].metadata.dimensions["sector"] = sector_mapping[col]

    # Add dimension definitions at table level
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.extend(
            [
                {"name": "sector", "slug": "sector"},
            ]
        )

    return tb
