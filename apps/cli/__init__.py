"""ETL services CLI."""
import rich_click as click

from apps.backport.backport import backport_cli as cli_backport_run
from apps.backport.bulk_backport import bulk_backport as cli_bulk_backport
from apps.backport.fasttrack_backport import cli as cli_fasttrack_backport
from apps.backport.migrate.migrate import cli as cli_backport_migrate
from apps.metadata_migrate.cli import cli as cli_metadata_migrate
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
from etl.metadata_export import cli as cli_metadata_export
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
# Configuration of the command `etlcli dev`.
# We define it first because we need to reference it.
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
# MAIN CLIENT
# Configuration of the command `etlcli`
#
################################

# DEFINE GROUPS, AND HOW THEY ARE SHOWN
## List with all groups and their commands.
## Each item is a group, with a name and the commands it contains.
## The commands are provided as a dictionary, with the command name as key, and the actual command function as value.
##
## Add here your new command! Make sure to add it as a (key, value)-pair in the "commands" dictionary of the group you want it to belong to. Alternatively, you can also create a new group.
GROUPS = [
    {
        "name": "Run ETL step",
        "commands": {
            "run": cli_run,
        },
    },
    {
        "name": "Charts",
        "commands": {
            "chart-sync": cli_staging_sync,
            "chart-gpt": cli_chartgpt,
            "chart-revisions": cli_chart_revision,
        },
    },
    {
        "name": "Metadata",
        "commands": {
            "metadata-export": cli_metadata_export,
            "metadata-migrate": cli_metadata_migrate,
            "metadata-upgrader": cli_meta_upgrader,
        },
    },
    {
        "name": "Data",
        "commands": {
            "harmonize": cli_harmonize,
            "diff": cli_datadiff,
            "graphviz": cli_graphviz,
            "compare": cli_compare,
        },
    },
    {
        "name": "Backport",
        "commands": {
            "backport-run": cli_backport_run,
            "backport-bulk": cli_bulk_backport,
            "backport-fasttrack": cli_fasttrack_backport,
            "backport-migrate": cli_backport_migrate,
        },
    },
    {
        "name": "Others",
        "commands": {
            # "dev": cli_dev,
            "variable-match": cli_match_variables,
            "variable-mapping-translate": cli_variable_mapping_translate,
        },
    },
]


# MAIN CLIENT ENTRYPOINT (no action actually)
## Note that the entrypoint has no action, it is just a group. The commands that fall under it do actually have actions.
@click.group(name="etl")
# @click.rich_config(help_config=help_config)
def cli() -> None:
    """OWID's ETL client.

    Create ETL step templates, compare different datasets, generate dependency visualisations, synchronise charts across different servers, import datasets from non-ETL OWID sources, improve your metadata, etc.
    """
    pass


# ADD ALL COMMANDS TO THE CLI
for group in GROUPS:
    for name, cmd in group["commands"].items():
        cli.add_command(cmd=cmd, name=name)
# Add dev
cli.add_command(cli_dev)


################################
#
# RICH CLICK CONFIG
# Configuration rich_click
#
################################

# RICH-CLICK CONFIGURATION
click.rich_click.USE_MARKDOWN = True
# click.rich_click.SHOW_ARGUMENTS = True
# click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
# Show variable types under description
# click.rich_click.SHOW_METAVARS_COLUMN = False
# click.rich_click.APPEND_METAVARS_HELP = True

## Convert GROUPS to the format expected by rich-click, and submit the ordering and groups so they are shown in the terminal (--help).
command_groups = [
    {
        "name": group["name"],
        "commands": list(group["commands"].keys()),
    }
    for group in GROUPS
]
click.rich_click.COMMAND_GROUPS = {"etlcli": command_groups}
