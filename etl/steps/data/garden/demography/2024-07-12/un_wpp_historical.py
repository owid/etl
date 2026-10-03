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

    # Projection
    ## Reshape historic (1968-2019) data.
    tb_historic = tb_historic.melt(
        id_vars=["country", "year"],
        var_name="revision",
        value_name="population",
    ).dropna(subset="population")
    tb_historic["revision"] = tb_historic["revision"].str.replace("revision_", "")

    ## 2022 data
    tb_22 = ds_wpp_22["population"].loc["World", list(range(2025, 2060, 5)), "population", "all", "all", "medium"]
    tb_22 = tb_22.reset_index()
    tb_22 = (
        tb_22[["location", "year", "value"]]
        .assign(revision=2022)
        .rename(columns={"location": "country", "value": "population"})
    )

    ## 2024 data
    tb_24_population = ds_wpp_24["population"]
    tb_24 = tb_24_population.loc[("World", list(range(2025, 2061, 5)), "all", "all", "medium"), ["population"]]
    tb_24 = tb_24.reset_index()
    tb_24 = tb_24[["country", "year", "population"]].assign(revision=2024)

    # Estimates
    tb_estimates = tb_24_population.loc[
        ("World", list(range(1950, 2021, 5)) + [2023], "all", "all", "estimates"), ["population"]
    ]
    tb_estimates = tb_estimates.reset_index()
    tb_estimates = (
        tb_estimates[["country", "year", "population"]]
        .assign(revision=2024)
        .rename(columns={"population": "population_estimates"})
    )

    # Combine
    tb = concat([tb_historic, tb_22, tb_24])

    # Format
    columns_index = ["country", "year", "revision"]
    tb = tb.format(columns_index, short_name="projections")
    tb_estimates = tb_estimates.format(columns_index, short_name="estimates")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[
            tb,
            tb_estimates,
        ],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
