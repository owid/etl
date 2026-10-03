"""Load the LGBTI National Policy Dataset garden tables and create the grapher dataset.

Only the two combined-categorical tables are published to grapher. The long
country-level proportion table and the binary "full implementation / not"
region table stay in garden for the catalog but aren't surfaced as grapher
variables — no published chart references them, and they would bloat the
variables list without telling a useful chart story.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("lgbti_national_policy_dataset")
    tb_combined = ds_garden["lgbti_national_policy_dataset_combined"]
    tb_combined_regions = ds_garden["lgbti_national_policy_dataset_combined_regions"]

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(
        tables=[tb_combined, tb_combined_regions],
        default_metadata=ds_garden.metadata,
    )
    ds_grapher.save()
