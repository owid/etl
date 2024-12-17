"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


indicator_title = "Estimated bilateral remittance flows"
description_short = "The estimated bilateral remittance flows between countries. These estimates use migrant stocks, host country incomes, and origin country incomes."
description_key = [
    "Flows between India and Pakistan, Pakistan and India, Lebanon and Israel and vice versa, and Azerbaijan and Armenia and vice versa  are assumed to be zero given the political economy situations in these corridors."
]
description_from_producer = [
    "The caveats attached to this estimate are:",
    "(a) The migrant stock data is drawn from the Bilateral Migration Matrix, which is itself based on UN Population Division and National Census data. These are by nature updated infrequently and may not appropriately capture sudden changes in migrant stock;",
    "(b) The incomes of migrants abroad and the costs of living in the migrants' country of origin are both proxied by per capita incomes in PPP terms, which is only a rough proxy;",
    "(c) Remittance behavior of second-generation migrants who may be recorded as native-born in the remittance source country are not accounted for;",
    "(d) There is no way to capture remittances flowing  through informal, unrecorded channels;",
    "(e) It does not account for cases where remittances may be miscalculated due to accounting errors arising from confusion with trade and tourism receipts;",
    "(f) It may also include cases of retirees moving to certain countries and taking out (remitting) their life long savings.",
]
unit = "Current US Dollars"
short_unit = "USD"


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("bilateral_remittance")

    # Read table from garden dataset.
    tb = ds_garden.read("bilateral_remittance", reset_index=True)

    tbs = []

    for cty in tb["country_origin"].unique():
        tb_cty = tb[tb["country_origin"] == cty].copy()
        tb_cty = tb_cty.rename(columns={"country_receiving": "country"})
        tb_cty = tb_cty.drop(columns=["country_origin"])

        col_name = f"remittance_flows_from_{cty}"
        tb_cty = tb_cty.rename(columns={"remittance_flows": col_name})

        tb_cty[col_name].metadata.title = f"{indicator_title} sent from {cty}"
        tb_cty[col_name].metadata.short_unit = short_unit
        tb_cty[col_name].metadata.unit = unit
        tb_cty[col_name].metadata.description_short = description_short
        tb_cty[col_name].metadata.description_key = description_key
        tb_cty[col_name].metadata.description_from_producer = description_from_producer

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
