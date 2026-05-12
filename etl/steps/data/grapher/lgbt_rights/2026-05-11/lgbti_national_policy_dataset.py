"""Load the LGBTI National Policy Dataset garden tables and create the grapher dataset.

Both tables are long-format with (country, year, law, status) — the framework
auto-expands each long row into per-(law, status) grapher variables.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("lgbti_national_policy_dataset")
    tb_country = ds_garden["lgbti_national_policy_dataset"]
    tb_regions = ds_garden["lgbti_national_policy_dataset_regions"]
    tb_combined = ds_garden["lgbti_national_policy_dataset_combined"]
    tb_combined_regions = ds_garden["lgbti_national_policy_dataset_combined_regions"]

    #
    # Save outputs.
    #
    ds_grapher = paths.create_dataset(
        tables=[tb_country, tb_regions, tb_combined, tb_combined_regions],
        default_metadata=ds_garden.metadata,
    )
    ds_grapher.save()
