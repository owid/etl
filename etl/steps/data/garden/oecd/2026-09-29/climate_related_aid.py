"""Load the climate-related aid meadow dataset and create a garden dataset.

There are two tables, with the same indicators:
* climate_related_aid_given: climate-related aid committed by each donor.
* climate_related_aid_received: climate-related aid committed to each recipient.

"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Table names (shared by meadow and garden).
TABLE_NAMES = ["climate_related_aid_given", "climate_related_aid_received"]

# Indicator columns expected in both tables.
DOLLAR_COLUMNS = [
    "climate_mitigation_only_dollars",
    "climate_adaptation_only_dollars",
    "climate_overlap_dollars",
    "climate_principal_dollars",
    "climate_significant_dollars",
    "other_aid_dollars",
]
SHARE_COLUMNS = [column.replace("_dollars", "_pct") for column in DOLLAR_COLUMNS]

# The source file rounds dollars to whole units per category, so the two ways of splitting the same deduplicated
# total ("mitigation only + adaptation only + overlap" vs "principal + significant") can differ by rounding.
# NOTE: The largest difference found in the 2007-2024 files is ~56,000 dollars (on totals of tens of billions).
SPLIT_TOLERANCE_DOLLARS = 1e5


def sanity_check_inputs(tb: Table) -> None:
    table_name = tb.metadata.short_name
    assert set(tb.columns) == {"country", "year", *DOLLAR_COLUMNS, *SHARE_COLUMNS}, (
        f"Unexpected columns in {table_name}."
    )
    assert not tb.duplicated(subset=["country", "year"]).any(), f"Duplicate (country, year) rows in {table_name}."
    assert tb[DOLLAR_COLUMNS + SHARE_COLUMNS].notnull().all().all(), f"Missing values in {table_name}."
    assert (tb[DOLLAR_COLUMNS + SHARE_COLUMNS] >= 0).all().all(), f"Negative values in {table_name}."
    # Both splits of the deduplicated climate-related total must agree.
    total_by_objective = tb[
        ["climate_mitigation_only_dollars", "climate_adaptation_only_dollars", "climate_overlap_dollars"]
    ].sum(axis=1)
    total_by_significance = tb[["climate_principal_dollars", "climate_significant_dollars"]].sum(axis=1)
    assert ((total_by_objective - total_by_significance).abs() <= SPLIT_TOLERANCE_DOLLARS).all(), (
        f"In {table_name}, mitigation-only + adaptation-only + overlap does not match principal + significant."
    )


def sanity_check_outputs(tb_given: Table, tb_received: Table) -> None:
    for tb in [tb_given, tb_received]:
        table_name = tb.metadata.short_name
        # Shares are of each entity's own total bilateral allocable ODA, so they can't exceed 100%.
        # NOTE: A few "Melanesia unspecified" rows exceed 100% in the source file; those rows are excluded in garden.
        assert (tb[SHARE_COLUMNS] <= 100).all().all(), f"Share above 100% in {table_name}."
        # Climate-related aid (principal + significant) plus other aid is the entity's total aid, so shares add to 100%.
        # NOTE: Shares are rounded to two decimals in the source file; the widest spread found is 99.99-100.1%.
        total_share = tb[["climate_principal_pct", "climate_significant_pct", "other_aid_pct"]].sum(axis=1)
        assert total_share.between(99.8, 100.2).all(), (
            f"Climate-related and other aid shares don't add to 100% in {table_name}."
        )
        assert "World" in set(tb.index.get_level_values("country")), f"World is missing in {table_name}."
        # World is the biggest entity every year; a larger country value would point to a unit or mapping error.
        world = tb.xs("World", level="country")["climate_principal_dollars"]
        max_by_year = tb["climate_principal_dollars"].groupby(level="year").max()
        assert (max_by_year <= world.reindex(max_by_year.index)).all(), f"An entity exceeds World in {table_name}."

    # World is the same deduplicated total, whether it is seen from the donor or the recipient side.
    climate_columns = [column for column in DOLLAR_COLUMNS if column != "other_aid_dollars"]
    world_given = tb_given.xs("World", level="country")[DOLLAR_COLUMNS]
    world_received = tb_received.xs("World", level="country")[DOLLAR_COLUMNS]
    assert (
        ((world_given[climate_columns] - world_received[climate_columns]).abs() <= SPLIT_TOLERANCE_DOLLARS).all().all()
    ), "World climate-related totals differ between the donor and recipient tables."
    # NOTE: For aid not targeting climate change, the donor and recipient views of World differ slightly (up to ~0.13%
    # in 2007-2024), because OECD's totals by donor don't perfectly match its totals summed by recipient.
    relative_gap = (world_given["other_aid_dollars"] - world_received["other_aid_dollars"]).abs() / world_received[
        "other_aid_dollars"
    ]
    assert (relative_gap < 0.002).all(), "World aid not targeting climate differs by more than 0.2% between tables."

    # The donors file covers every donor (including EU Institutions), so donors must add up to World.
    donors = tb_given.drop("World", level="country")["climate_principal_dollars"].groupby(level="year").sum()
    assert (
        (donors - world_given["climate_principal_dollars"]).abs() / world_given["climate_principal_dollars"] < 1e-6
    ).all(), "The sum of donors does not match World in the donors table."


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("climate_related_aid")

    #
    # Process data.
    #
    tables = []
    for table_name in TABLE_NAMES:
        tb = ds_meadow.read(table_name)
        sanity_check_inputs(tb)

        # Harmonize country names.
        # NOTE: The source file's own continent totals (built with a mapping different from OWID's regions) and OECD's
        # "... unspecified" regional rows are dropped (see the excluded countries file). "EU Institutions" is kept
        # as a donor, and "World" is kept as given (it includes all unspecified rows and EU Institutions).
        # Both tables share one mapping file (donors and recipients are mostly different countries), so warnings about
        # unused mappings or excluded entities absent from one of the tables would be expected noise.
        tb = paths.regions.harmonize_names(
            tb=tb, warn_on_unused_countries=False, warn_on_unknown_excluded_countries=False
        )

        # Improve table format.
        tables.append(tb.format(["country", "year"], short_name=table_name))

    sanity_check_outputs(*tables)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=tables, default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
