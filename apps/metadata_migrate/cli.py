import json
import webbrowser
from typing import Any, Dict, List, Optional

import click
import pandas as pd
import structlog
from owid.catalog import Dataset, DatasetMeta, License, Origin, Source, Table
from rich import print
from rich.console import Console
from rich.syntax import Syntax
from rich_click.rich_command import RichCommand
from sqlalchemy.engine import Engine
from sqlmodel import Session

from etl import config
from etl import grapher_model as gm
from etl.command import main as etl_main
from etl.db import get_engine
from etl.metadata_export import merge_or_create_yaml, reorder_fields
from etl.paths import BASE_DIR, DAG_FILE, DATA_DIR, STEP_DIR

log = structlog.get_logger()


@click.command(name="metadata-migrate", cls=RichCommand)
@click.option(
    "--chart-slug",
    "-c",
    type=str,
    help="Slug of the chart to generate metadata for. Example: 'human-rights-index-vdem'.",
)
@click.option(
    "--uri",
    "-u",
    type=str,
    help="URI of the dataset to generate metadata for. Example: 'happiness/2023-03-20/happiness'.",
)
@click.option(
    "--cols",
    "-c",
    type=str,
    help="Only generate metadata for columns matching pattern. ",
)
@click.option(
    "--table-name",
    "-t",
    type=str,
    help="Table to select.",
)
@click.option(
    "--run-etl/--no-run-etl",
    default=True,
    type=bool,
    help="Refresh ETL for the given dataset.",
)
@click.option(
    "--show/--no-show",
    "-s",
    default=False,
    type=bool,
    help="Show output instead of saving it into a file.",
)
def cli(
    chart_slug: str,
    uri: str,
    cols: str,
    table_name: str,
    run_etl: bool,
    show: bool,
) -> None:
    """Generate (or update) the metadata YAML in a Grapher step based on an existing chart.

    This process pre-fills the indicator with all available metadata from the existing dataset (in the old format) and adds grapher
    configuration taken from the chart config (accessed via its chart slug).

    Fields that are missing will be prefixed with 'TBD' and should either be filled in manually or removed. The
    description field needs to be restructured into new fields. This step could potentially be automated by
    ChatGPT in the future.

    **Note:** It is designed for use with the --chart-slug option. The use of --uri in conjunction with other options
    has not been as thoroughly tested.

    **Example 1:** Show generated YAML in console

    ```
    STAGING=mojmir etl metadata-migrate --chart-slug political-regime --show
    ```

    **Example 2:** Create YAML file in Grapher step

    ```
    STAGING=mojmir etl metadata-migrate --chart-slug political-regime
    ```
    """
    assert config.STAGING, "You have to run this as STAGING=mystaging etl metadata-migrate ..."

    engine = get_engine()
    col = None
    var_id = None

    if chart_slug:
        assert not uri, "specify either chart-slug or uri, not both"
        assert not cols, "specify either chart-slug or cols, not both"
        assert not table_name, "specify either chart-slug or table-name, not both"

        q = f"""
        select config from charts
        where slug = '{chart_slug}'
        """
        df = pd.read_sql(q, engine)
        if df.empty:
            raise ValueError(f"no chart found for slug {chart_slug}")

        grapher_config = json.loads(df.iloc[0].config)

        # extract first variable of a chart
        var_id = grapher_config["dimensions"][0]["variableId"]

        with Session(engine) as session:
            variable = gm.Variable.load_variable(session, var_id)

        assert variable.catalogPath, f"Variable {var_id} does not come from ETL. Migrate it there first."

        # extract dataset URI and columns
        uri, cols = variable.catalogPath.split("#")
        cols = f"^{cols}$"
        uri = uri.split("/", 1)[1]
        uri, table_name = uri.rsplit("/", 1)
    else:
        grapher_config = None

    if uri:
        assert "grapher" not in uri, "uri should be without channel"
        assert "garden" not in uri, "uri should be without channel"

    if run_etl:
        log.info(f"Running ETL for {uri}")
        etl_main(
            dag_path=DAG_FILE,
            steps=[uri],
            grapher=False,
        )

    ds = Dataset(DATA_DIR / "grapher" / uri)

    if not table_name:
        assert len(ds.table_names) == 1, f"Multiple tables found {ds.table_names}, specify it with --table-name"
        table_name = ds.table_names[0]

    tab = ds[table_name]

    if cols:
        # no matching cols, it could be multidimensional column
        if tab.filter(regex=cols).empty:
            tab = tab.filter(regex=cols.split("__")[0])
        else:
            tab = tab.filter(regex=cols)

    origins = _get_origins(tab, ds)

    # TODO: check that sources of all indicators are the same
    var_origins = tab.iloc[:, 0].m.origins

    # variables already use origins, don't add them to YAML
    if var_origins:
        log.info("Indicator already uses origins, not adding them to YAML.")
        origins = []
    # use dataset created from source
    else:
        if len(ds.metadata.licenses) > 1:
            raise NotImplementedError("Multiple licenses not supported")
        elif len(ds.metadata.licenses) == 1:
            license = ds.metadata.licenses[0]
        else:
            license = None
        origins = [_create_origin_from_source(ds, ds.metadata.sources[0], license)]

    vars = {}
    for col in tab:
        vars[col] = {}

        # duplicate metadata from garden YAML, this adds redundancy, but it's easier to fill out the rest of the fields
        # with all metadata available
        var_meta = tab[col].metadata
        for field in ("title", "unit", "short_unit", "description", "display"):
            val = getattr(var_meta, field, None)
            if val:
                # use description_short instead of description
                if field == "description":
                    field = "description_short"
                vars[col][field] = val

        if "title" not in vars[col]:
            vars[col]["title"] = "TBD - title"

        # move description_short
        if "description_short" in vars[col]:
            vars[col]["description"] = vars[col]["description_short"]
        vars[col]["description_short"] = "TBD - Indicator's short description"
        vars[col]["description_from_producer"] = "TBD - Indicator's description given by the producer"

        vars[col]["description_key"] = [
            "TBD - List of key pieces of information about the indicator.",
        ]
        vars[col]["description_processing"] = "TBD - Indicator's processing description"

        # empty fields to be filled
        vars[col]["presentation"] = {}
        vars[col]["presentation"][
            "title_public"
        ] = "TBD - Indicator title to be shown in data pages, that overrides the indicator's title."
        vars[col]["presentation"]["title_variant"] = "TBD - Indicator's title variant"
        vars[col]["presentation"]["attribution_short"] = "TBD - Indicator's attribution (shorter version)"
        vars[col]["presentation"]["faqs"] = [
            {
                "fragment_id": "TBD - Question identifier from FAQ GDoc",
                "gdoc_id": "1gGburArxglFdHXeTLotFW4TOOLoeRq5XW6UfAdKtaAw",
            }
        ]

        # get chart for a variable if we didn't use --chart-slug
        if not grapher_config:
            grapher_config = _load_grapher_config(engine, col, ds.metadata)

        if grapher_config:
            vars[col]["presentation"]["grapher_config"] = _prune_chart_config(grapher_config)

            # use chart subtitle as description_short
            if "subtitle" in grapher_config and vars[col]["description_short"].startswith("TBD"):
                vars[col]["description_short"] = grapher_config["subtitle"]

    dataset = {
        "update_period_days": "TBD - Number of days between OWID updates",
    }

    definitions = {
        "common": {
            "processing_level": "TBD - major or minor",
            "presentation": {"topic_tags": ["TBD - Indicator's topic tags"]},
        }
    }

    if origins:
        dataset["sources"] = []  # type: ignore
        definitions["common"]["sources"] = []
        definitions["common"]["origins"] = [origin.to_dict() for origin in origins]

    meta_dict = {"dataset": dataset, "definitions": definitions, "tables": {table_name: {"variables": vars}}}

    meta_dict = reorder_fields(meta_dict)

    output_path = STEP_DIR / "data/grapher" / (uri + ".meta.yml")
    yaml_str = merge_or_create_yaml(meta_dict, output_path)

    if show:
        Console().print(Syntax(yaml_str, "yaml", line_numbers=True))
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(yaml_str)  # type: ignore

        if run_etl:
            log.info(f"Running ETL for {uri}")
            config.GRAPHER_FILTER = f"^{col}$"
            etl_main(
                dag_path=DAG_FILE,
                steps=[uri],
                grapher=True,
            )

        data_page_url = f"http://staging-site-{config.STAGING}/admin/datapage-preview/{var_id}"
        webbrowser.open_new_tab(data_page_url)

        print("\n[bold yellow]Follow-up instructions:[/bold yellow]")
        print(
            "[green]1.[/green] Pick topic tags [link=http://datasette-private/owid?sql=SELECT+tags.%60name%60+from+tags+where+slug+is+not+null+ORDER+BY+tags.%60name%60%0D%0A]from the list[/link].",
        )
        print(
            "[green]2.[/green] Check [link=https://docs.google.com/document/d/1gGburArxglFdHXeTLotFW4TOOLoeRq5XW6UfAdKtaAw/edit]FAQs GDoc document[/link].",
        )
        print(
            f"[green]3.[/green] Review opened [link={data_page_url}]Data Page[/link].",
        )
        print(
            f"[green]4.[/green] Update generated YAML file {output_path.relative_to(BASE_DIR)}.",
        )
        print(
            "[green]5.[/green] (Optional) Move YAML from grapher to garden.",
        )
        print(
            f"[green]6.[/green] Re-run ETL with `STAGING={config.STAGING} GRAPHER_FILTER={col} etl {uri} --grapher`.",
        )


def _get_origins(tab: Table, ds: Dataset) -> List[Origin]:
    # TODO: check that sources of all indicators are the same
    var_origins = tab.iloc[:, 0].m.origins

    # variables already use origins, don't add them to YAML
    if var_origins:
        log.info("Indicator already uses origins, not adding them to YAML.")
        return []

    # use dataset created from source
    else:
        if len(ds.metadata.licenses) > 1:
            raise NotImplementedError("Multiple licenses not supported")
        elif len(ds.metadata.licenses) == 1:
            license = ds.metadata.licenses[0]
        else:
            license = None

        if not ds.metadata.sources:
            raise ValueError(f"No sources or origins found in dataset metadata for {tab.columns[0]}")

        return [_create_origin_from_source(ds, ds.metadata.sources[0], license)]


def _create_origin_from_source(ds: Dataset, source: Source, license: Optional[License]) -> Origin:
    description = ""
    if ds.metadata.description:
        description = ds.metadata.description + "\n"
    if source.description:
        description += source.description

    origin = Origin(
        title=ds.metadata.title,
        producer=source.name,
        citation_full=source.published_by,
        license=license,
        description=description,
        url_main=source.url,
        url_download=source.source_data_url,
        date_accessed=source.date_accessed,
        date_published=source.publication_date or source.publication_year,
    )

    if not origin.date_published:
        log.warning(
            f"missing publication_date and publication_year in source, using date_accessed: {origin.date_accessed}"
        )
        origin.date_published = origin.date_accessed
    return origin


def _load_grapher_config(engine: Engine, col: str, ds_meta: DatasetMeta) -> Dict[Any, Any]:
    """TODO: This is work in progress! Update this function as you like."""
    q = f"""
    select
        c.config
    from variables as v
    join datasets as d on v.datasetId = d.id
    join chart_dimensions as cd on v.id = cd.variableId
    join charts as c on cd.chartId = c.id
    where
        v.shortName = '{col}' and
        d.namespace = '{ds_meta.namespace}' and
        d.version = '{ds_meta.version}' and
        d.shortName = '{ds_meta.short_name}'
    """
    cf = pd.read_sql(q, engine)
    if len(cf) == 0:
        log.warning(f"no chart found for variable {col}")
        return {}
    # TODO: be smarter about more than one chart and merge them
    # we could even use git pattern to leverage VSCode merging features
    # <<<<<<< chart1
    # foo
    # =======
    # bar
    # >>>>>>> chart2
    elif len(cf) > 1:
        log.warning(f"multiple charts found for variable {col}, using the first one")

    return json.loads(cf.iloc[0]["config"])


def _prune_chart_config(config: Dict[Any, Any]) -> Dict[Any, Any]:
    # prune fields not useful for ETL grapher_config
    for field in ("id", "version", "dimensions", "isPublished", "data", "slug"):
        config.pop(field, None)
    if "map" in config:
        config["map"].pop("columnSlug", None)
    return config


if __name__ == "__main__":
    cli()
