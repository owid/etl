#!/usr/bin/env python
"""
CLI tool to update WDI metadata.

Replaces update_metadata.ipynb with modular, testable commands.

Usage:
    python update_wdi_metadata.py update-titles --dry-run
    python update_wdi_metadata.py update-sources --dry-run
    python update_wdi_metadata.py update-charts --dry-run
    python update_wdi_metadata.py all --dry-run
"""

import json
import os
import re
from importlib import import_module
from pathlib import Path
from typing import Union

import click
from openai import OpenAI
from owid.catalog import Dataset
from rich.console import Console
from rich.table import Table

from apps.chart_sync.admin_api import AdminAPI
from etl.config import OWID_ENV
from etl.db import get_engine, read_sql
from etl.files import ruamel_dump, ruamel_load
from etl.paths import DATA_DIR

console = Console()

# OMM and other patterns to keep even if not in current dataset
KEEP_PATTERNS = {
    "armed_forces_share_population",
    "eg_cft_.*",
    "eg_elc_.*",
    "articles_per_million_people",
    "patents_per_million_people",
}


def should_keep_variable(var_name: str, keep_patterns: set) -> bool:
    """Check if variable should be kept based on regex patterns."""
    for pattern in keep_patterns:
        if re.match(pattern, var_name):
            return True
    return False


def replace_years(s: str, year: Union[int, str]) -> str:
    """Replace all years in string with {year}.

    Example:
        >>> replace_years("GDP (constant 2010 US$)", 2015)
        "GDP (constant 2015 US$)"
    """
    year_regex = re.compile(r"\b([1-2]\d{3})\b")
    s_new = year_regex.sub(str(year), s)
    return s_new


def load_metadata(version: str):
    """Load meadow dataset and metadata."""
    ds_meadow = Dataset(DATA_DIR / "meadow/worldbank_wdi" / version / "wdi")
    tb = ds_meadow["wdi"]
    indicator_codes = [tb[col].m.title for col in tb.columns]
    tb_metadata = ds_meadow.read("wdi_metadata", safe_types=False)

    # Import wdi module dynamically
    wdi_module = import_module(f"etl.steps.data.garden.worldbank_wdi.{version}.wdi")
    df_vars = wdi_module.load_variable_metadata(tb_metadata, indicator_codes)

    return tb, df_vars


@click.group()
def cli():
    """WDI metadata update CLI."""
    pass


@cli.command()
@click.option("--dry-run", is_flag=True, help="Preview changes without writing")
def update_titles(dry_run: bool):
    """Update variable titles and years from metadata."""
    version = Path.cwd().name
    yaml_path = Path("wdi.meta.yml")

    console.print(f"[bold]Loading metadata for version {version}...[/bold]")
    tb, df_vars = load_metadata(version)

    console.print(f"[bold]Loading {yaml_path}...[/bold]")
    yml = ruamel_load(yaml_path)
    variables = yml["tables"]["wdi"]["variables"]

    # Delete variables that are not in the dataset
    missing_variables = set(variables.keys()) - set(tb.columns)
    missing_variables = {
        v for v in missing_variables if not v.startswith("omm_") and not should_keep_variable(v, KEEP_PATTERNS)
    }

    if missing_variables:
        table = Table(title=f"Variables to Delete ({len(missing_variables)})")
        table.add_column("Variable Code", style="red")
        for var in sorted(missing_variables):
            table.add_row(var)
        console.print(table)

        if not dry_run:
            for var in missing_variables:
                del variables[var]
            console.print(f"[green]✓ Deleted {len(missing_variables)} variables[/green]")
    else:
        console.print("[green]No variables to delete[/green]")

    # Update titles and years
    updates = []
    new_vars = []

    for indicator_code in df_vars.index:
        try:
            indicator_name = df_vars.loc[indicator_code].indicator_name
        except KeyError:
            continue

        if indicator_code in variables:
            var = variables[indicator_code]
            old_title = var.get("title", "")
            if old_title != indicator_name:
                updates.append((indicator_code, old_title, indicator_name))
        else:
            var = {}
            variables[indicator_code] = var
            new_vars.append((indicator_code, indicator_name))

        # Update title (strip whitespace)
        var["title"] = indicator_name.strip()

        # If title contains year, try to update units too
        year_regex = re.compile(r"\b([1-2]\d{3})\b")
        regex_res = year_regex.search(indicator_name)
        if regex_res:
            year = regex_res.groups()[0]

            if "unit" in var:
                var["unit"] = replace_years(var["unit"], year)

            if "short_unit" in var:
                var["short_unit"] = replace_years(var["short_unit"], year)

            for k in ["name", "unit", "short_unit"]:
                if var.get("display", {}).get("unit"):
                    var["display"]["unit"] = replace_years(var["display"]["unit"], year)

                if var.get("display", {}).get("short_unit"):
                    var["display"]["short_unit"] = replace_years(var["display"]["short_unit"], year)

            if "presentation" in var:
                for k in ["title_public", "title_variant"]:
                    if k in var["presentation"]:
                        var["presentation"][k] = replace_years(var["presentation"][k], year)

    # Show summary
    if updates:
        table = Table(title=f"Title Updates ({len(updates)})")
        table.add_column("Code", style="cyan")
        table.add_column("Old Title", style="yellow")
        table.add_column("New Title", style="green")
        for code, old, new in updates[:10]:  # Show first 10
            table.add_row(code, old[:50] + "..." if len(old) > 50 else old, new[:50] + "..." if len(new) > 50 else new)
        if len(updates) > 10:
            table.add_row("...", f"... and {len(updates) - 10} more", "...")
        console.print(table)

    if new_vars:
        table = Table(title=f"New Variables ({len(new_vars)})")
        table.add_column("Code", style="cyan")
        table.add_column("Title", style="green")
        for code, title in new_vars[:10]:  # Show first 10
            table.add_row(code, title[:70] + "..." if len(title) > 70 else title)
        if len(new_vars) > 10:
            table.add_row("...", f"... and {len(new_vars) - 10} more")
        console.print(table)

    if not dry_run:
        with open(yaml_path, "w") as f:
            f.write(ruamel_dump(yml))
        console.print(f"[green]✓ Updated {yaml_path}[/green]")
        console.print(f"  - {len(updates)} title updates")
        console.print(f"  - {len(new_vars)} new variables")
        console.print(f"  - {len(missing_variables)} deleted variables")
    else:
        console.print("[yellow]Dry run - no changes written[/yellow]")


@cli.command()
@click.option("--dry-run", is_flag=True, help="Preview changes without writing")
def update_sources(dry_run: bool):
    """Update source citations using GPT-4o-mini."""
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        console.print("[red]Error: OPENAI_API_KEY environment variable not set.[/red]")
        raise click.Abort()

    version = Path.cwd().name
    sources_path = Path("wdi.sources.json")

    console.print(f"[bold]Loading metadata for version {version}...[/bold]")
    tb, df_vars = load_metadata(version)

    console.print(f"[bold]Loading {sources_path}...[/bold]")
    with open(sources_path, "r") as f:
        sources = json.load(f)

    # Remove TODO sources
    sources = [s for s in sources if not s["name"].startswith("TODO")]

    # Find missing sources
    missing_sources = list(set(df_vars["source"]) - {s["rawName"] for s in sources})

    if not missing_sources:
        console.print("[green]No missing sources to update[/green]")
        return

    console.print(f"[yellow]Found {len(missing_sources)} missing sources[/yellow]")

    # Show examples
    table = Table(title="Missing Sources (first 5)")
    table.add_column("Raw Source Name", style="cyan")
    for source in missing_sources[:5]:
        table.add_row(source[:100] + "..." if len(source) > 100 else source)
    if len(missing_sources) > 5:
        table.add_row(f"... and {len(missing_sources) - 5} more")
    console.print(table)

    if dry_run:
        console.print("[yellow]Dry run - would fetch source names from GPT-4[/yellow]")
        return

    # Load good examples from file
    examples_path = Path(__file__).parent / "source_examples.json"
    with open(examples_path, "r") as f:
        GOOD_EXAMPLES = json.load(f)

    console.print(f"[green]Loaded {len(GOOD_EXAMPLES)} example source citations[/green]")

    SYSTEM_PROMPT = f"""
You are tasked with creating short citation names for data sources based on their raw names and data publisher sources.

Input format: You will receive rawName and dataPublisherSource fields.
Output format: Return a JSON object with a "sources" field containing an array of objects with rawName and name fields.

Rules for creating the "name" field in addition to what you infer from examples:
1. World Bank MUST appear in every citation name
2. rawName may contain multiple sources separated by \\n, but the output name should be a single citation.

Check out these good examples. Make sure these examples are followed closely.
{json.dumps(GOOD_EXAMPLES, indent=2)}
"""

    client = OpenAI(api_key=openai_key)
    MAX_BATCH_SIZE = 30
    all_new_sources = []

    # Process in batches
    for i in range(0, len(missing_sources), MAX_BATCH_SIZE):
        batch_missing_sources = missing_sources[i : i + MAX_BATCH_SIZE]
        console.print(
            f"[bold]Processing batch {i//MAX_BATCH_SIZE + 1}: {len(batch_missing_sources)} sources (total: {len(missing_sources)})[/bold]"
        )

        # Create input data for this batch
        missing_sources_data = []
        for raw_name in batch_missing_sources:
            matching_rows = df_vars[df_vars["source"] == raw_name]
            if not matching_rows.empty:
                data_publisher_source = matching_rows.iloc[0].get("dataPublisherSource", "")
                missing_sources_data.append({"rawName": raw_name, "dataPublisherSource": data_publisher_source})

        if not missing_sources_data:
            continue

        input_text = json.dumps(missing_sources_data, ensure_ascii=False, indent=2)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": input_text},
        ]

        # Use GPT-4o-mini (same as notebook)
        response = client.chat.completions.create(
            model="gpt-4o-mini", messages=messages, response_format={"type": "json_object"}
        )

        r = json.loads(response.choices[0].message.content)

        assert len(r["sources"]) == len(
            batch_missing_sources
        ), f"Expected {len(batch_missing_sources)} sources, got {len(r['sources'])}"

        all_new_sources.extend(r["sources"])

    console.print(
        f"[green]✓ Processed {len(all_new_sources)} sources across {(len(missing_sources) + MAX_BATCH_SIZE - 1) // MAX_BATCH_SIZE} batches[/green]"
    )

    # Show first 5 results
    table = Table(title="Generated Source Names (first 5)")
    table.add_column("Raw Name", style="yellow")
    table.add_column("Clean Name", style="green")
    for source in all_new_sources[:5]:
        table.add_row(
            source["rawName"][:50] + "..." if len(source["rawName"]) > 50 else source["rawName"],
            source["name"],
        )
    console.print(table)

    # Update sources.json
    for new_source in all_new_sources:
        # Handle newline characters in rawName
        new_source["rawName"] = new_source["rawName"].replace("\\n", "\n")
        # Normalize comma formatting (e.g., "[WHO] uri:" -> "[WHO], uri:")
        # new_source["rawName"] = re.sub(r'\] (uri|note):', r'], \1:', new_source["rawName"])

        for s in sources:
            if s["rawName"] == new_source["rawName"]:
                console.print(f"[yellow]Updating existing source: {new_source['name']}[/yellow]")
                s["name"] = new_source["name"]
                break
        else:
            # New source, add it
            sources.append(
                {
                    "rawName": new_source["rawName"],
                    "name": new_source["name"],
                    "dataPublisherSource": new_source.get("dataPublisherSource", ""),
                }
            )

    # Save updated sources
    with open(sources_path, "w") as f:
        json.dump(sources, f, ensure_ascii=False, indent=2)

    console.print(f"[green]✓ Updated {sources_path} with {len(all_new_sources)} new sources[/green]")


@cli.command()
@click.option("--dry-run", is_flag=True, help="Preview changes without making updates")
@click.option("--old-year", default="2017", help="Old base year to replace")
@click.option("--new-year", default="2021", help="New base year to use")
def update_charts(dry_run: bool, old_year: str, new_year: str):
    """Update chart configurations for base year changes."""
    version = Path.cwd().name

    console.print(f"[bold]Finding charts using WDI indicators from version {version}...[/bold]")

    # Get GDP variable
    q = f"""
    select id from variables
    where name = 'GDP per capita, PPP (constant {new_year} international $)'
        and catalogPath LIKE 'grapher/worldbank_wdi/%/wdi/wdi#ny_gdp_pcap_pp_kd'
    ORDER BY catalogPath DESC
    LIMIT 1
    """
    engine = get_engine()
    result = read_sql(q, engine)

    if result.empty:
        console.print("[yellow]No GDP variable found - skipping chart updates[/yellow]")
        return

    var_id = result.id.iloc[0]
    console.print(f"[green]Found GDP variable ID: {var_id}[/green]")

    # Get all charts using that variable
    q = f"""
    select chartId from chart_dimensions where variableId = {var_id};
    """
    chart_ids = list(read_sql(q, engine)["chartId"])
    console.print(f"[yellow]Found {len(chart_ids)} charts using this variable[/yellow]")

    if not chart_ids:
        console.print("[green]No charts to update[/green]")
        return

    if dry_run:
        console.print(
            f"[yellow]Dry run - would update {len(chart_ids)} charts replacing {old_year} → {new_year}[/yellow]"
        )
        return

    admin_api = AdminAPI(OWID_ENV)
    updated_count = 0

    for chart_id in chart_ids:
        chart_config = admin_api.get_chart_config(chart_id)

        fields = ["subtitle", "note"]
        update = False

        for field in fields:
            if field in chart_config:
                if old_year in (chart_config.get(field, "") or ""):
                    chart_config[field] = chart_config[field].replace(old_year, new_year)
                    update = True

        if update:
            console.print(f"[yellow]Updating chart {chart_id}[/yellow]")
            admin_api.update_chart(chart_id, chart_config)
            updated_count += 1

    console.print(f"[green]✓ Updated {updated_count} charts[/green]")


@cli.command()
@click.option("--dry-run", is_flag=True, help="Preview all changes without writing")
@click.option("--skip-sources", is_flag=True, help="Skip source updates (requires OpenAI)")
@click.option("--skip-charts", is_flag=True, help="Skip chart updates")
def all(dry_run: bool, skip_sources: bool, skip_charts: bool):
    """Run all metadata updates in sequence."""
    console.print("[bold cyan]Running full metadata update pipeline...[/bold cyan]\n")

    # 1. Update titles
    console.print("[bold]Step 1: Update titles and years[/bold]")
    ctx = click.Context(update_titles)
    ctx.invoke(update_titles, dry_run=dry_run)
    console.print()

    # 2. Update sources
    if not skip_sources:
        console.print("[bold]Step 2: Update sources[/bold]")
        try:
            ctx = click.Context(update_sources)
            ctx.invoke(update_sources, dry_run=dry_run)
            console.print()
        except click.Abort:
            console.print("[yellow]Skipping source updates (no OpenAI key)[/yellow]\n")

    # 3. Update charts
    if not skip_charts:
        console.print("[bold]Step 3: Update charts[/bold]")
        ctx = click.Context(update_charts)
        ctx.invoke(update_charts, dry_run=dry_run, old_year="2017", new_year="2021")
        console.print()

    console.print("[bold green]✓ All updates complete![/bold green]")


if __name__ == "__main__":
    cli()
