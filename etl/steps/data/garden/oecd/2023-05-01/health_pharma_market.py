"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


# Each `measure` value implies a unit and a human-readable title suffix.
# Multiple `measures` per `variable` are possible — we generate one column per (variable, measure).
MAPPING_MEASURE = {
    "% of total sales": {"unit": "%", "title": "Sales: % of total sales"},
    "% share of generics (value)": {"unit": "%", "title": "Market: % share of generics (value)"},
    "% share of generics (volume)": {"unit": "%", "title": "Market: % share of generics (volume)"},
    "/capita, US$ exchange rate": {"unit": "US$ per capita", "title": "Sales: US$ (exchange rate) per capita"},
    "/capita, US$ purchasing power parity": {
        "unit": "US$ per capita",
        "title": "Sales: US$ (purchasing power parity per capita",
    },
    "Defined daily dosage per 1 000 inhabitants per day": {
        "unit": "daily dose per 1,000 inhabitants per day",
        "title": "Consumption: DDD per 1,000 inhabitants per day",
    },
    "Million US$ at exchange rate": {"unit": "million US$", "title": "Sales: Million US$ at exchange rate"},
    "Million US$, purchasing power parity": {
        "unit": "million US$",
        "title": "Sales: Million US$, purchasing power parity",
    },
    "Million of national currency units": {
        "unit": "million of national currency units",
        "title": "Sales: Million of national currency units",
    },
}


def run() -> None:
    ds_meadow = paths.load_dataset("health_pharma_market")
    tb = ds_meadow.read("health_pharma_market")

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)

    # Pivot so each (variable, measure) pair gets its own column. The resulting Table
    # carries origins on the value-derived columns (pivot preserves column metadata).
    tb = tb.pivot(index=["country", "year"], columns=["variable", "measure"], values="value")

    tb = _apply_variable_metadata(tb)
    tb = tb.reset_index()
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()


def _apply_variable_metadata(tb: Table) -> Table:
    """Set title/unit per (variable, measure) column; preserve origins from pivot."""
    new_names = {}
    for col in tb.columns:
        # `col` is a (variable, measure) tuple from the pivot.
        variable, measure = col
        new_name = f"{variable} ({MAPPING_MEASURE[measure]['title']})"
        new_names[col] = new_name

    # Rename columns from MultiIndex tuples to flat strings.
    tb.columns = [new_names[c] for c in tb.columns]

    # Set title/unit on each renamed column without dropping its origins.
    for col in tb.columns:
        # The column's measure name lives in the title we just constructed.
        # Recover (variable, measure) by reverse-lookup from new_names.
        variable_measure = next(k for k, v in new_names.items() if v == col)
        _, measure = variable_measure
        tb[col].metadata.title = col
        tb[col].metadata.unit = MAPPING_MEASURE[measure]["unit"]

    return tb
