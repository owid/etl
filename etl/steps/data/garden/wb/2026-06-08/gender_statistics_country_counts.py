"""Country counts and population by status for binarized WBL indicators."""

from owid.catalog import Table, VariableMeta, VariablePresentationMeta

from etl.data_helpers import geo
from etl.helpers import PathFinder

paths = PathFinder(__file__)

REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]
FRAC_ALLOWED_NANS_PER_YEAR = 0.2
MIN_YEAR = 1970

# WBL indicators to include in country counts.
# Continuous 0-1 scores are binarized: score == 1 -> "yes", else -> "no".
WBL_INDICATORS = [
    "gd_wbl_ent_law_entactv",  # was sg_bus_regt_eq
    "gd_wbl_pay_law_eqremeqval",  # was sg_law_eqrm_wk
    "gd_wbl_wrk_law_nondiscemp",  # was sg_law_nodc_hr
    "gd_wbl_saf_law_domviol",  # was sg_leg_dvaw
    "gd_wbl_ast_law_ownership",  # was sg_own_prrt_im
]


def run() -> None:
    ds_garden = paths.load_dataset("gender_statistics")
    tb = ds_garden.read("gender_statistics", safe_types=False)

    # Keep only relevant columns
    tb = tb[["country", "year"] + [c for c in WBL_INDICATORS if c in tb.columns]]

    # Binarize: score == 1 -> 1, else -> 0 (NaN stays NaN)
    for col in WBL_INDICATORS:
        if col in tb.columns:
            tb[col] = (tb[col] == 1).astype("Int64").where(tb[col].notna())

    tb = add_country_counts_and_population_by_status(tb)

    # Remove the raw indicator columns
    columns_to_keep = [col for col in tb.columns if col not in WBL_INDICATORS]
    tb = tb[columns_to_keep]
    tb = tb[tb["year"] >= MIN_YEAR]
    tb = tb.format(["country", "year"])

    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_garden.save()


def add_country_counts_and_population_by_status(tb: Table) -> Table:
    """Add country counts and population by status for binary columns."""
    tb_regions = tb.copy()
    tb_regions = paths.regions.add_population(tb=tb_regions, warn_on_missing_countries=False)

    columns = [col for col in tb.columns if col not in ("country", "year")]

    # Remove years where all indicator columns are NaN
    year_mask = tb_regions.groupby("year")[columns].apply(lambda g: g.notna().any().any())
    valid_years = year_mask[year_mask].index
    tb_regions = tb_regions[tb_regions["year"].isin(valid_years)]

    columns_count_dict = {col: [] for col in columns}
    columns_pop_dict = {col: [] for col in columns}
    new_columns = {}

    for col in columns:
        column_title = tb[col].metadata.title
        description_from_producer = tb[col].metadata.description_from_producer

        for status, condition in [("yes", lambda s: s == 1), ("no", lambda s: s == 0), ("missing", lambda s: s.isna())]:
            count_col = f"{col}_{status}_count"
            pop_col = f"{col}_{status}_pop"

            mask = condition(tb_regions[col])
            count_data = mask.fillna(False).astype(int)
            count_data = count_data.copy_metadata(tb_regions[col])
            pop_data = count_data * tb_regions["population"]

            new_columns[count_col] = {
                "data": count_data,
                "metadata": _make_metadata(
                    column_title, description_from_producer, status, "count", count_data.m.origins
                ),
            }
            new_columns[pop_col] = {
                "data": pop_data,
                "metadata": _make_metadata(column_title, description_from_producer, status, "pop", pop_data.m.origins),
            }

            columns_count_dict[col].append(count_col)
            columns_pop_dict[col].append(pop_col)

    for col_name, col_info in new_columns.items():
        tb_regions[col_name] = col_info["data"]
        tb_regions[col_name].metadata = col_info["metadata"]

    tb_regions = tb_regions.copy()

    columns_count = [item for sublist in columns_count_dict.values() for item in sublist]
    columns_pop = [item for sublist in columns_pop_dict.values() for item in sublist]

    tb_regions = geo.add_regions_to_table(
        tb=tb_regions,
        ds_regions=paths.load_dataset("regions"),
        regions=REGIONS,
        frac_allowed_nans_per_year=FRAC_ALLOWED_NANS_PER_YEAR,
    )

    # Recalculate missing population for regions
    tb_regions = tb_regions.drop(columns=["population"])
    tb_regions = paths.regions.add_population(tb=tb_regions, warn_on_missing_countries=False)

    for col in columns:
        column_title = tb[col].metadata.title if hasattr(tb[col].metadata, "title") else col
        description_from_producer = getattr(tb[col].metadata, "description_from_producer", "")

        tb_regions[f"{col}_missing_pop_other_countries"] = tb_regions["population"] - tb_regions[
            columns_pop_dict[col]
        ].sum(axis=1)

        tb_regions[f"{col}_missing_pop"] = (
            tb_regions[f"{col}_missing_pop"] + tb_regions[f"{col}_missing_pop_other_countries"]
        )

        tb_regions[f"{col}_missing_pop"].metadata = _make_metadata(
            column_title, description_from_producer, "missing", "pop", tb_regions[f"{col}_missing_pop"].m.origins
        )

    tb_regions = tb_regions[tb_regions["country"].isin(REGIONS)].copy().reset_index(drop=True)
    tb_regions = tb_regions[["country", "year"] + columns_count + columns_pop]

    return tb_regions


def _make_metadata(
    column_title: str, description_from_producer: str, status: str, count_or_pop: str, origins
) -> VariableMeta:
    clean_title = column_title.replace(" (1=yes; 0=no)", "")

    if count_or_pop == "count":
        meta = VariableMeta(
            title=f"{column_title} - {status.capitalize()} (Count)",
            description_short=f"Number of countries with the status '{status}' for \"{clean_title}\".",
            description_from_producer=description_from_producer,
            unit="countries",
            short_unit="",
            sort=[],
            origins=origins,
        )
    else:
        meta = VariableMeta(
            title=f"{column_title} - {status.capitalize()} (Population)",
            description_short=f"Population of countries with the status '{status}' for \"{clean_title}\".",
            description_from_producer=description_from_producer,
            unit="people",
            short_unit="",
            sort=[],
            origins=origins,
        )

    meta.display = {"name": meta.title, "numDecimalPlaces": 0, "tolerance": 0}
    meta.presentation = VariablePresentationMeta(title_public=meta.title)
    return meta
