"""Load a meadow dataset and create a garden dataset."""

from owid.catalog.tables import concat

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load fasttrack data (1968-2019)
    tb_historic = paths.read_snap_table("un_wpp_historical")
    ds_wpp_22 = paths.load_dataset("un_wpp", version="2022-07-11")
    ds_wpp_24 = paths.load_dataset("un_wpp", version="2024-07-12")

    #
    # Process data.
    #

    # Reshape historic (1968-2019) data.
    tb_historic = tb_historic.melt(
        id_vars=["country", "year"],
        var_name="revision",
        value_name="population",
    ).dropna(subset="population")
    tb_historic["revision"] = tb_historic["revision"].str.replace("revision_", "")

    # 2022 data
    tb_22 = ds_wpp_22["population"].loc["World", list(range(2025, 2060, 5)), "population", "all", "all", "medium"]
    tb_22 = tb_22.reset_index()
    tb_22 = (
        tb_22[["location", "year", "value"]]
        .assign(revision=2022)
        .rename(columns={"location": "country", "value": "population"})
    )

    # 2024 data
    tb_24 = ds_wpp_24["population"].loc[("World", list(range(2025, 2060, 5)), "all", "all", "medium"), ["population"]]
    tb_24 = tb_24.reset_index()
    tb_24 = tb_24[["country", "year", "population"]].assign(revision=2024)

    # Combine
    tb = concat([tb_historic, tb_22, tb_24])

    # Format
    tb = tb.format(["country", "year", "revision"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
