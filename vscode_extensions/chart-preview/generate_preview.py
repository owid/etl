"""Generate a self-contained HTML preview for an ETL dataset step.

Usage:
    .venv/bin/python ai/dataset_preview/generate_preview.py etl/steps/data/garden/biodiversity/2025-04-07/cherry_blossom.py
    # → writes ai/dataset_preview/output.html
"""

import argparse
import json
import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def parse_step_path(step_path: str) -> str:
    """Convert step file path to dataset directory path."""
    m = re.match(r"(?:.*?/)?etl/steps/data/(.*?)\.py$", step_path)
    if not m:
        raise ValueError(f"Cannot parse step path: {step_path}")
    return str(BASE_DIR / "data" / m.group(1))


def _read_columns(feather_path: str, columns: list[str]) -> "pd.DataFrame":
    """Read specific columns from a feather file using pyarrow column projection."""
    import pyarrow.feather as feather

    return feather.read_table(feather_path, columns=columns).to_pandas()


def load_table_stats(
    dataset_path: str,
    table_name: str,
    popularity: dict[str, float] | None = None,
    short_name: str = "",
    namespace: str = "",
) -> dict:
    """Load a table and compute per-indicator statistics using column projection."""
    import random as _random

    from owid.catalog import Dataset

    ds = Dataset(dataset_path)

    # Load only metadata (no data) to discover columns and their metadata
    tb_meta = ds.read(table_name, load_data=False)

    dimensions = set(tb_meta.metadata.primary_key) if tb_meta.metadata.primary_key else set()
    all_cols = list(tb_meta.columns)
    indicator_cols = [c for c in all_cols if c not in dimensions]

    # Sort by popularity if available: key is namespace/dataset/table#column
    def _pop(col: str) -> float:
        if not popularity:
            return 0.0
        key = f"{namespace}/{short_name}/{table_name}#{col}"
        return popularity.get(key, 0.0)

    if popularity:
        indicator_cols = sorted(indicator_cols, key=_pop, reverse=True)

    # Cap indicators to avoid huge JSON payloads for wide datasets (e.g. WDI ~1500 cols)
    MAX_INDICATORS = 24
    n_indicators_total = len(indicator_cols)
    truncated = n_indicators_total > MAX_INDICATORS
    indicator_cols = indicator_cols[:MAX_INDICATORS]

    has_year = "year" in dimensions or "year" in all_cols
    has_country = "country" in dimensions or "country" in all_cols

    # Find the feather file for column projection
    feather_path = Path(dataset_path) / f"{table_name}.feather"
    use_feather = feather_path.exists()

    # Columns available for sparklines/dimensions
    dim_cols_present = [c for c in ["country", "year"] if c in dimensions or c in all_cols]
    can_sparkline = has_year and has_country and use_feather

    # Load all needed columns in a single feather read for efficiency
    if use_feather:
        cols_to_load = list(dict.fromkeys(dim_cols_present + indicator_cols))  # dedup, preserve order
        tb = _read_columns(str(feather_path), cols_to_load)
    else:
        # Fallback: load full table (parquet/csv datasets)
        tb = ds.read(table_name)

    # Year range and entity list from loaded data
    year_min = year_max = entity_count = None
    sampled_entities: list[str] | None = None
    if "year" in tb.columns:
        year_col = tb["year"].astype(int)
        year_min = int(year_col.min())
        year_max = int(year_col.max())
    if "country" in tb.columns:
        all_entities = list(tb["country"].unique())
        entity_count = len(all_entities)
        MAX_SPARKLINE_ENTITIES = 50
        if len(all_entities) > MAX_SPARKLINE_ENTITIES:
            sampled_entities = _random.sample(all_entities, MAX_SPARKLINE_ENTITIES)
        else:
            sampled_entities = all_entities

    # Filter to sampled entities once for sparklines
    if can_sparkline and sampled_entities is not None:
        sparkline_df = tb[tb["country"].isin(sampled_entities)]
    else:
        sparkline_df = None

    indicators = []
    for col in indicator_cols:
        meta = tb_meta[col].metadata
        series = tb[col]

        null_count = int(series.isna().sum())
        total_count = len(series)
        null_pct = round(100 * null_count / total_count, 1) if total_count > 0 else 0

        is_numeric = series.dtype.kind in ("f", "i", "u") or str(series.dtype).startswith(("Float", "Int"))

        stats = {}
        if is_numeric:
            numeric_series = series.dropna()
            if len(numeric_series) > 0:
                stats = {
                    "min": round(float(numeric_series.min()), 4),
                    "max": round(float(numeric_series.max()), 4),
                    "mean": round(float(numeric_series.mean()), 4),
                }

        # Value distribution for string/categorical columns
        value_counts = None
        if not is_numeric:
            vc = series.dropna().value_counts()
            total_non_null = int(vc.sum()) if len(vc) > 0 else 1
            top = vc.head(10)
            value_counts = [
                {"value": str(k), "count": int(v), "pct": round(100 * v / total_non_null, 1)} for k, v in top.items()
            ]

        # Sparkline: per-entity time series
        sparkline_by_entity = None
        if is_numeric and sparkline_df is not None:
            sparkline_by_entity = {}
            for entity, grp in sparkline_df.groupby("country", observed=True):
                series_ent = grp.set_index("year")[col].dropna().sort_index()
                if len(series_ent) < 2:
                    continue
                data = list(series_ent.items())
                if len(data) > 200:
                    step_size = len(data) / 200
                    data = [data[int(i * step_size)] for i in range(200)]
                sparkline_by_entity[str(entity)] = [{"year": int(y), "value": round(float(v), 4)} for y, v in data]

        # Quality flags
        quality_flags = []
        if not meta.title:
            quality_flags.append("missing_title")
        if not meta.unit and is_numeric:
            quality_flags.append("missing_unit")
        if not meta.description_short:
            quality_flags.append("missing_description")
        if not meta.origins:
            quality_flags.append("missing_origins")

        pop_score = _pop(col) if popularity else 0.0

        indicators.append(
            {
                "short_name": col,
                "title": meta.title or col,
                "popularity": pop_score,
                "unit": meta.unit,
                "short_unit": meta.short_unit,
                "type": str(series.dtype),
                "is_numeric": is_numeric,
                "description_short": meta.description_short,
                "processing_level": meta.processing_level,
                "origins_count": len(meta.origins),
                "origins_producer": meta.origins[0].producer if meta.origins else None,
                "null_count": null_count,
                "null_pct": null_pct,
                "total_count": total_count,
                "stats": stats,
                "value_counts": value_counts,
                "sparkline_by_entity": sparkline_by_entity,
                "quality_flags": quality_flags,
            }
        )

    return {
        "table_name": table_name,
        "n_rows": len(tb),
        "n_cols": len(all_cols),
        "n_indicators": n_indicators_total,
        "n_indicators_shown": len(indicator_cols),
        "truncated": truncated,
        "dimensions": sorted(dimensions),
        "year_min": year_min,
        "year_max": year_max,
        "entity_count": entity_count,
        "indicators": indicators,
    }


def fetch_popularity(namespace: str) -> dict[str, float]:
    """Fetch indicator popularity from staging MySQL, keyed by namespace/dataset/table#column.

    Ignores channel (grapher/garden) and version so it works across dataset versions.
    Returns empty dict on any error (popularity is best-effort).
    """
    try:
        from etl.config import OWID_ENV

        df = OWID_ENV.read_sql(
            "SELECT slug, popularity FROM analytics_popularity WHERE type = 'indicator' AND slug LIKE %s",
            params=(f"grapher/{namespace}/%",),
        )
        # slug format: grapher/{namespace}/{version}/{dataset}/{table}#{column}
        # key:                  {namespace}/{dataset}/{table}#{column}  (drop channel+version)
        result = {}
        for _, row in df.iterrows():
            parts = row["slug"].split("/")
            if len(parts) >= 5:
                # parts: [grapher, namespace, version, dataset, table#column]
                key = f"{parts[1]}/{parts[3]}/{parts[4]}"
                result[key] = max(result.get(key, 0.0), float(row["popularity"]))
        return result
    except Exception:
        return {}


def build_payload(step_path: str) -> dict:
    """Build the full JSON payload for the preview."""
    from owid.catalog import Dataset

    dataset_path = parse_step_path(step_path)
    index_file = Path(dataset_path) / "index.json"
    if not index_file.exists():
        raise FileNotFoundError(
            f"Dataset not found at: {dataset_path}\n\n"
            "The dataset has not been built yet. Save the step file to trigger a build, "
            "or run it manually:\n\n"
            f"  .venv/bin/etlr {step_path} --private"
        )
    ds = Dataset(dataset_path)

    # Parse step URI components from path
    rel = str(Path(dataset_path).relative_to(BASE_DIR / "data"))
    parts = rel.split("/")
    channel, namespace, version, short_name = parts[0], parts[1], parts[2], parts[3]
    step_uri = f"data://{channel}/{namespace}/{version}/{short_name}"

    popularity = fetch_popularity(namespace)

    tables = []
    for tn in ds.table_names:
        tables.append(load_table_stats(dataset_path, tn, popularity, short_name, namespace))

    return {
        "step_uri": step_uri,
        "step_path": step_path,
        "channel": channel,
        "namespace": namespace,
        "version": version,
        "short_name": short_name,
        "title": ds.metadata.title,
        "n_tables": len(ds.table_names),
        "tables": tables,
    }


def generate_html(payload: dict) -> str:
    """Inject JSON payload into the HTML template."""
    template_path = Path(__file__).parent / "preview.html"
    template = template_path.read_text()
    json_str = json.dumps(payload, indent=2)
    return template.replace("__PREVIEW_DATA__", json_str)


def main():
    parser = argparse.ArgumentParser(description="Generate dataset preview HTML")
    parser.add_argument("step_path", help="Path to ETL step file")
    parser.add_argument(
        "-o", "--output", default=None, help="Output HTML path (default: ai/dataset_preview/output.html)"
    )
    parser.add_argument("--json", action="store_true", help="Output JSON to stdout instead of HTML file")
    args = parser.parse_args()

    payload = build_payload(args.step_path)

    if args.json:
        print(json.dumps(payload))
    else:
        output_path = args.output or str(Path(__file__).parent / "output.html")
        html = generate_html(payload)
        Path(output_path).write_text(html)
        print(f"Preview written to {output_path}")


if __name__ == "__main__":
    main()
