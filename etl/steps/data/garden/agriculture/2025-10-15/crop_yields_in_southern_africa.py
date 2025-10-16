"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from each table, and how to rename them.
COLUMNS_MODIS = {
    "country": "country",
    "year": "year",
    "meangcvi": "yield",
}
COLUMNS_SIF = {
    "country_na": "country",
    "year": "year",
    "mean": "yield",
}
COLUMNS_FAO = {
    "area": "country",
    "year": "year",
    "element": "element",
    "item": "item",
    "value": "yield",
    # Just for sanity checks.
    "unit": "unit",
    "flag_description": "flag",
}


def adapt_and_combine_tables(tb_fao, tb_modis, tb_sif):
    # Sanity check.
    assert set(tb_fao["unit"]) == {"100 g/ha"}, "Unexpected units"
    assert set(tb_fao["flag"]) == {"Official figure", "Estimated value"}, "Unexpected flags"
    # Prepare FAO data.
    # There are two items in FAO data, namely "Maize (corn)" and "Green corn (maize)". For now, use one of them.
    tb_fao = tb_fao[(tb_fao["item"] == "Maize (corn)")][["country", "year", "yield"]].reset_index(drop=True)
    # Convert FAOSTAT yield units from 100 g/ha to tonnes/ha.
    tb_fao["yield"] /= 10000

    # Concatenate tables.
    tb = pr.concat(
        [
            tb_modis.assign(**{"source": "yield_modis"}),
            tb_sif.assign(**{"source": "yield_sif"}),
            tb_fao.assign(**{"source": "yield_fao"}),
        ],
        ignore_index=True,
    )

    return tb


def add_columns_for_rescaled_yield(tb):
    # In the paper, the MODIS and SIF time series for each country are rescaled using a linear transformation so that their minimum and maximum values matched those of the corresponding FAO maize yields, enabling direct visual comparison of trends across datasets.
    # Additionally, the FAO yield data is shifted one year earlier; I suppose they do this to align harvest-year statistics with the satellite observations from the preceding growing season.
    tb_fao_shifted = tb[["country", "year", "yield_fao"]].rename(columns={"yield_fao": "yield_fao_adapted"}).copy()
    tb_fao_shifted["year"] -= 1
    tb = tb.merge(tb_fao_shifted, on=["country", "year"], how="outer")
    for country, group in tb.groupby("country"):
        # Find absolute minimum and maximum of each source, for the current country.
        fao_min, fao_max = group["yield_fao"].min(), group["yield_fao"].max()
        modis_min, modis_max = group["yield_modis"].min(), group["yield_modis"].max()
        sif_min, sif_max = group["yield_sif"].min(), group["yield_sif"].max()
        # Rescale MODIS and SIF so that their minimum and maxima coincide with those of FAO.
        tb.loc[tb["country"] == country, "yield_modis_adapted"] = fao_min + (group["yield_modis"] - modis_min) * (
            fao_max - fao_min
        ) / (modis_max - modis_min)
        tb.loc[tb["country"] == country, "yield_sif_adapted"] = fao_min + (group["yield_sif"] - sif_min) * (
            fao_max - fao_min
        ) / (sif_max - sif_min)
    # Add metadata to the newly created columns.
    tb["yield_modis_adapted"] = tb["yield_modis_adapted"].copy_metadata(tb["yield_modis"])
    tb["yield_sif_adapted"] = tb["yield_sif_adapted"].copy_metadata(tb["yield_sif"])

    return tb


def plot_curves(tb):
    import plotly.express as px

    plot = tb[["country", "year", "yield_fao_adapted", "yield_modis_adapted", "yield_sif_adapted"]].melt(
        id_vars=["country", "year"]
    )
    for country in sorted(set(plot["country"])):
        px.line(
            plot[plot["country"] == country],
            x="year",
            y="value",
            color="variable",
            title=country,
            color_discrete_map={
                "yield_fao_adapted": "red",
                "yield_sif_adapted": "green",
                "yield_modis_adapted": "blue",
            },
        ).show()


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("crop_yields_in_southern_africa")

    # Read tables from meadow dataset.
    tb_modis = ds_meadow.read("crop_yields_in_southern_africa__modis")
    tb_fao = ds_meadow.read("crop_yields_in_southern_africa__fao")
    tb_sif = ds_meadow.read("crop_yields_in_southern_africa__sif")

    #
    # Process data.
    #
    # Select and rename columns from each table.
    tb_modis = tb_modis[list(COLUMNS_MODIS)].rename(columns=COLUMNS_MODIS, errors="raise")
    tb_sif = tb_sif[list(COLUMNS_SIF)].rename(columns=COLUMNS_SIF, errors="raise")
    tb_fao = tb_fao[list(COLUMNS_FAO)].rename(columns=COLUMNS_FAO, errors="raise")

    # Adapt tables and combine them.
    tb = adapt_and_combine_tables(tb_fao=tb_fao, tb_modis=tb_modis, tb_sif=tb_sif)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Create one column per source.
    tb = tb.pivot(index=["country", "year"], columns=["source"], values="yield", join_column_levels_with="_")

    # Add columns of yield rescaled to be able to compare them (as done for Fig 1 of the paper).
    tb = add_columns_for_rescaled_yield(tb=tb)

    # DEBUG: Uncomment to plot curves similar to those in Fig 1 of the paper.
    # plot_curves(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
