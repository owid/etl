"""ETL services CLI."""
import rich_click as click

from apps.metagpt.cli import main as meta_upgrader_cli
from apps.staging_sync.cli import cli as cli_staging_sync

# from apps.wizard.cli import cli as cli_wizard
from etl.command import main_cli as cli_run
from etl.compare import cli as cli_compare
from etl.datadiff import cli as cli_datadiff
from etl.harmonize import harmonize as cli_harmonize
from etl.prune import prune_cli as cli_prune
from etl.publish import publish_cli as cli_publish
from etl.reindex import reindex_cli as cli_reindex
from etl.run_python_step import main as run_python_step_cli
from etl.to_graphviz import to_graphviz as graphviz_cli
from etl.version_tracker import run_version_tracker_checks as vtracker_cli

################################
#
# DEV SUBCOMMAND
# Configuration of the command `etlcli dev`
#
################################
COMMANDS_DEV = {
    "version-tracker": vtracker_cli,
    "prune": cli_prune,
    "publish": cli_publish,
    "reindex": cli_reindex,
    "run-python-step": run_python_step_cli,
}


@click.group("dev", help="Development commands")
def cli_dev() -> None:
    """Development utils."""
    pass


for name, cmd in COMMANDS_DEV.items():
    cli_dev.add_command(cmd=cmd, name=name)


################################
#
# MAIN CLIENT
# Configuration of the command `etlcli`
#
################################
COMMANDS = {
    "dev": cli_dev,
    "run": cli_run,
    "compare": cli_compare,
    "harmonize": cli_harmonize,
    "diff": cli_datadiff,
    "metaup": meta_upgrader_cli,
    # "wiz": cli_wizard,
    "ssync": cli_staging_sync,
    "graphviz": graphviz_cli,
}


@click.group(name="etl", help="ETL operations")
def cli() -> None:
    """Run etl operations."""
    pass


for name, cmd in COMMANDS.items():
    cli.add_command(cmd=cmd, name=name)
