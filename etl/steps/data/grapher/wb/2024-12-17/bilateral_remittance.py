"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


indicator_title = "Estimated bilateral remittance flows"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("bilateral_remittance")

    # Read table from garden dataset.
    tb = ds_garden.read("bilateral_remittance", reset_index=True)

    tbs = []

    countries = list(tb["country_origin"].unique())

    for cty in countries:
        tb_cty = tb[tb["country_origin"] == cty].copy()
        tb_cty = tb_cty.rename(columns={"country_receiving": "country"})
        tb_cty = tb_cty.drop(columns=["country_origin"])

        col_name = f"remittance_flows_from_{'_'.join([word.lower() for word in cty.split(' ')])}"
        print(col_name)
        tb_cty = tb_cty.rename(columns={"remittance_flows": col_name})

        tb_cty[col_name].metadata.title = f"{indicator_title} sent from {cty}"

        tb_cty = tb_cty.format(["country", "year"], short_name=f"bilateral_remittance_{cty}")
        tbs.append(tb_cty)
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tbs, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
