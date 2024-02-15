"""ETL services CLI."""
import rich_click as click

from apps.backport.backport import backport_cli as cli_backport
from apps.backport.bulk_backport import bulk_backport as cli_bulk_backport
from apps.backport.fasttrack_backport import cli as cli_fasttrack_backport
from apps.backport.migrate.migrate import cli as cli_backport_migrate
from apps.metadata_migrate import cli as cli_metadata_migrate
from apps.metagpt.cli import main as cli_meta_upgrader
from apps.staging_sync.cli import cli as cli_staging_sync
from etl.chart_revision.cli import main_cli as cli_chart_revision
from etl.chart_revision.v2.chartgpt import cli as cli_chartgpt

# from apps.wizard.cli import cli as cli_wizard
from etl.command import main_cli as cli_run
from etl.compare import cli as cli_compare
from etl.datadiff import cli as cli_datadiff
from etl.harmonize import harmonize as cli_harmonize
from etl.match_variables import main_cli as cli_match_variables
from etl.metadata_export import metadata_export as cli_metadata_export
from etl.prune import prune_cli as cli_prune
from etl.publish import publish_cli as cli_publish
from etl.reindex import reindex_cli as cli_reindex
from etl.run_python_step import main as cli_run_python_step
from etl.to_graphviz import to_graphviz as cli_graphviz
from etl.variable_mapping_translate import main_cli as cli_variable_mapping_translate
from etl.version_tracker import run_version_tracker_checks as cli_vtracker

################################
#
# DEV SUBCOMMAND
# Configuration of the command `etlcli dev`
#
################################
COMMANDS_DEV = {
    "version-tracker": cli_vtracker,
    "prune": cli_prune,
    "publish": cli_publish,
    "reindex": cli_reindex,
    "run-python-step": cli_run_python_step,
}


@click.group("dev", help="Development commands.")
def cli_dev() -> None:
    """Development utils."""
    pass


for name, cmd in COMMANDS_DEV.items():
    cli_dev.add_command(cmd=cmd, name=name)


################################
#
# VARIABLE-MAPPING SUBCOMMAND
# Configuration of the command `etlcli variable-mapping`
#
################################
COMMANDS_VARIABLE_MAPPING = {
    "translate": cli_variable_mapping_translate,
    "match": cli_match_variables,
}


@click.group("variable-mapping", help="Variable mapping commands.")
def cli_variable_mapping() -> None:
    """Variable mapping utils."""
    pass


for name, cmd in COMMANDS_VARIABLE_MAPPING.items():
    cli_variable_mapping.add_command(cmd=cmd, name=name)


################################
#
# METADATA SUBCOMMAND
# Configuration of the command `etlcli metadata`
#
################################
COMMANDS_METADATA = {
    "migrate": cli_metadata_migrate,
    "export": cli_metadata_export,
    "upgrader": cli_meta_upgrader,
}


@click.group("metadata", help="Metadata commands.")
def cli_metadata() -> None:
    """Metadata mapping utils."""
    pass


for name, cmd in COMMANDS_METADATA.items():
    cli_metadata.add_command(cmd=cmd, name=name)


################################
#
# BACKPORT SUBCOMMAND
# Configuration of the command `etlcli backport`
#
################################
COMMANDS_BACKPORT = {
    "run": cli_backport,
    "bulk": cli_bulk_backport,
    "fasttrack": cli_fasttrack_backport,
    "migrate": cli_backport_migrate,
}


@click.group("backport", help="Backport commands.")
def cli_backport() -> None:
    """Metadata mapping utils."""
    pass


for name, cmd in COMMANDS_BACKPORT.items():
    cli_backport.add_command(cmd=cmd, name=name)

################################
#
# MAIN CLIENT
# Configuration of the command `etlcli`
#
################################
COMMANDS = {
    "dev": cli_dev,
    "variable-mapping": cli_variable_mapping,
    "run": cli_run,
    "compare": cli_compare,
    "harmonize": cli_harmonize,
    "diff": cli_datadiff,
    # "meta-up": cli_meta_upgrader,
    # "wiz": cli_wizard,
    "chart-sync": cli_staging_sync,
    "chart-revisions": cli_chart_revision,
    "chart-gpt": cli_chartgpt,
    "graphviz": cli_graphviz,
    "metadata": cli_metadata,
    "backport": cli_backport,
}


@click.group(name="etl", help="ETL operations")
def cli() -> None:
    """Run etl operations."""
    pass


for name, cmd in COMMANDS.items():
    cli.add_command(cmd=cmd, name=name)
