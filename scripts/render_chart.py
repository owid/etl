"""Render a graph step .chart.yml as a standalone HTML file.

Usage:
    .venv/bin/python scripts/render_chart.py etl/steps/graph/animal_welfare/latest/banning-of-chick-culling.chart.yml
    .venv/bin/python scripts/render_chart.py etl/steps/graph/animal_welfare/latest/banning-of-chick-culling.chart.yml -o my_chart.html
"""

import json
from pathlib import Path

import click
from owid.catalog import Dataset

from etl.dag_helpers import load_dag
from etl.files import ruamel_load
from etl.paths import DATA_DIR, BASE_DIR

STEPS_DIR = BASE_DIR / "etl" / "steps" / "graph"

EXCLUDE_CONFIG_KEYS = {"dimensions", "$schema", "originUrl"}


def generate_chart_html(csv_data: str, column_defs: list, grapher_config: dict) -> str:
    """Generate a standalone HTML page that renders an OWID Grapher chart.

    Adapted from owid-grapher-py's _generate_chart_html().
    """
    return f"""<!DOCTYPE html>
<html>
  <head>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link
      href="https://fonts.googleapis.com/css?family=Lato:300,400,400i,700,700i|Playfair+Display:400,700&display=swap"
      rel="stylesheet"
    />
    <link
      rel="stylesheet"
      href="https://expose-grapher-state.owid.pages.dev/assets/owid.css"
    />
    <style>
      body {{ margin: 0; padding: 0; }}
      figure {{ width: 100%; height: 100%; margin: 0; }}
    </style>
  </head>
  <body>
    <figure id="grapher-container"></figure>
    <script type="module" src="https://expose-grapher-state.owid.pages.dev/assets/owid.mjs"></script>
    <script type="module">
      await new Promise((resolve) => setTimeout(resolve, 500));

      const {{ Grapher, GrapherState, OwidTable, React, createRoot }} = window;
      const container = document.getElementById("grapher-container");

      if (!GrapherState || !OwidTable || !React || !createRoot) {{
        throw new Error("Required exports not available");
      }}

      const csvData = `{csv_data}`;
      const columnDefs = {json.dumps(column_defs)};
      const table = new OwidTable(csvData, columnDefs);

      const grapherState = new GrapherState({{
        table: table,
        ...{json.dumps(grapher_config)},
        isConfigReady: true,
        isDataReady: true,
      }});

      const reactRoot = createRoot(container);
      reactRoot.render(React.createElement(Grapher, {{ grapherState }}));
    </script>
  </body>
</html>"""


def chart_yml_to_step_uri(chart_yml: Path) -> str:
    """Convert a .chart.yml path to its graph:// step URI."""
    rel = chart_yml.resolve().relative_to(STEPS_DIR.resolve())
    # rel is e.g. animal_welfare/latest/banning-of-chick-culling.chart.yml
    step_path = str(rel).replace(".chart.yml", "")
    return f"graph://{step_path}"


def expand_indicator_path(catalog_path: str, dependencies: list[str]) -> tuple[str, str, str]:
    """Expand a short catalogPath to (dataset_rel, table_name, indicator).

    Handles these formats:
    - "indicator" -> uses first dependency, assumes table == dataset short_name
    - "table#indicator" -> uses first dependency
    - "dataset/table#indicator" -> matches against dependencies
    - "channel/ns/ver/dataset/table#indicator" -> already full
    """
    if "#" in catalog_path:
        path_part, indicator = catalog_path.rsplit("#", 1)
        slash_count = path_part.count("/")
        if slash_count >= 3:
            # Full path: channel/ns/ver/dataset/table#indicator
            parts = path_part.split("/")
            return "/".join(parts[:-1]), parts[-1], indicator
        elif slash_count == 1:
            # dataset/table#indicator
            _, table_name = path_part.split("/")
            dep_base = dependencies[0].split("://", 1)[1]
            return dep_base, table_name, indicator
        else:
            # table#indicator
            table_name = path_part
            dep_base = dependencies[0].split("://", 1)[1]
            return dep_base, table_name, indicator
    else:
        # Just indicator name - assume first dependency, table == dataset name
        indicator = catalog_path
        dep_base = dependencies[0].split("://", 1)[1]
        dataset_name = dep_base.split("/")[-1]
        return dep_base, dataset_name, indicator


def guess_column_type(series) -> str:
    """Guess whether a column is Numeric or Categorical."""
    import pandas.api.types as ptypes

    if ptypes.is_string_dtype(series) or ptypes.is_categorical_dtype(series):
        return "Categorical"
    return "Numeric"


def render_chart(chart_yml: Path) -> str:
    """Load config + data for a .chart.yml and return rendered HTML."""
    config = ruamel_load(chart_yml)

    # Resolve DAG dependencies
    step_uri = chart_yml_to_step_uri(chart_yml)
    dag = load_dag()
    dependencies = dag.get(step_uri)
    if not dependencies:
        raise click.ClickException(f"No DAG entry found for {step_uri}")
    dep_list = sorted(dependencies)

    # Parse dimensions to find indicator columns
    dimensions = config.get("dimensions", [])
    if not dimensions:
        raise click.ClickException("No dimensions found in config")

    # Expand each catalogPath and collect (dataset_path, table_name, indicator) tuples
    indicators = []
    for dim in dimensions:
        catalog_path = dim.get("catalogPath")
        if not catalog_path:
            continue
        dataset_rel, table_name, indicator = expand_indicator_path(catalog_path, dep_list)
        indicators.append((dataset_rel, table_name, indicator))

    if not indicators:
        raise click.ClickException("No catalogPath found in dimensions")

    # Load dataset and table, reading only the columns we need
    dataset_rel, table_name, _ = indicators[0]
    indicator_slugs = [ind[2] for ind in indicators]
    dataset_path = DATA_DIR / dataset_rel
    if not dataset_path.exists():
        raise click.ClickException(
            f"Dataset not found at {dataset_path}. Run:\n"
            f"  .venv/bin/etlr {dataset_rel} --private"
        )
    ds = Dataset(str(dataset_path))
    tb = ds.read(table_name, columns=["country", "year"] + indicator_slugs)

    # Prepare data
    tb = tb.rename(columns={"country": "entityName"})
    csv_data = tb.to_csv(index=False)
    column_defs = [
        {"slug": slug, "type": guess_column_type(tb[slug])}
        for slug in indicator_slugs
    ]

    # Build grapher_config from the YAML, removing non-rendering keys
    grapher_config = {k: v for k, v in config.items() if k not in EXCLUDE_CONFIG_KEYS}

    # Set map.columnSlug if map config exists
    if "map" in grapher_config and isinstance(grapher_config["map"], dict):
        grapher_config["map"].setdefault("columnSlug", indicator_slugs[0])

    # Add required rendering fields
    grapher_config["ySlugs"] = " ".join(indicator_slugs)
    grapher_config["selectedEntityNames"] = tb["entityName"].tolist()
    grapher_config["hideLogo"] = True

    return generate_chart_html(csv_data, column_defs, grapher_config)


@click.command()
@click.argument("chart_yml", type=click.Path(exists=True, path_type=Path))
@click.option("-o", "--output", type=click.Path(path_type=Path), default=None, help="Output HTML path (default: ai/<slug>.html)")
def main(chart_yml: Path, output: Path | None):
    """Render a graph step .chart.yml as a standalone HTML file."""
    chart_yml = chart_yml.resolve()

    if output is None:
        slug = chart_yml.stem.replace(".chart", "")
        output = BASE_DIR / "ai" / f"{slug}.html"

    html = render_chart(chart_yml)
    output.write_text(html)
    click.echo(f"Chart saved to {output}")


if __name__ == "__main__":
    main()
