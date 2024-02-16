"""ETL services CLI.

If you want to add a new service, make sure to add it to the `GROUPS` list. If it is a subgroup, you can add it to the `SUBGROUPS` list.
"""
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
# SUBGROUPS
#
# Configuration of the subcommands like `etlcli d` or `etlcli b`.
# We define it first because we need to reference it later.
#
# You can edit `SUBGROUPS` to add new subcommands!
#
################################
SUBGROUPS = [
    {
        "description": "Development commands.",
        "alias": "d",
        "name": "Development",
        "commands": {
            "version-tracker": cli_vtracker,
            "prune": cli_prune,
            "publish": cli_publish,
            "reindex": cli_reindex,
            "run-python-step": cli_run_python_step,
        },
    },
    {
        "alias": "b",
        "name": "Backport",
        "description": "Backport commands.",
        "commands": {
            "fasttrack": cli_fasttrack_backport,
            "migrate": cli_backport_migrate,
            "bulk": cli_bulk_backport,
            "run": cli_backport_run,
        },
    },
]

# Convert this so we can use it in `GROUPS`
subgroups = []
for subgroup in SUBGROUPS:
    # Define group command
    def _cli() -> None:
        f"""{subgroup['description']}"""
        pass

    _cli = click.group(subgroup["alias"])(_cli)
    _group = {
        "name": subgroup["name"],
        "commands": {
            subgroup["alias"]: _cli,
        },
    }

    # Define subgroup commands, add to group
    for name, cmd in subgroup["commands"].items():
        _cli.add_command(cmd=cmd, name=name)
        _group["commands"][f"{subgroup['alias']} {name}"] = cmd
    subgroups.append(_group)


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
## The list is sorted in the order we want the groups to be shown in the terminal (--help).
##
## Add here your new command! Make sure to add it as a (key, value)-pair in the "commands" dictionary of the group you want it to belong to. Alternatively, you can also create a new group.
#
GROUPS = (
    # Main groups
    [
        {
            "name": "Run ETL steps",
            "commands": {
                "run": cli_run,
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
            "name": "Metadata",
            "commands": {
                "metadata-export": cli_metadata_export,
                "metadata-migrate": cli_metadata_migrate,
                "metadata-upgrade": cli_meta_upgrader,
            },
        },
        {
            "name": "Charts",
            "commands": {
                "chart-sync": cli_staging_sync,
                "chart-gpt": cli_chartgpt,
                "chart-upgrade": cli_chart_revision,
            },
        },
    ]
    # Add subroups (don't moddify)
    + subgroups
    # Others (not so relevant, maybe deprecated one day...)
    + [
        {
            "name": "Others",
            "commands": {
                "variable-match": cli_match_variables,
                "variable-mapping-translate": cli_variable_mapping_translate,
            },
        },
    ]
)


# MAIN CLIENT ENTRYPOINT (no action actually)
## Note that the entrypoint has no action, it is just a group. The commands that fall under it do actually have actions.
@click.group(name="etlcli")
# @click.rich_config(help_config=help_config)
def cli() -> None:
    """Run OWID's ETL client.

    Create ETL step templates, compare different datasets, generate dependency visualisations, synchronise charts across different servers, import datasets from non-ETL OWID sources, improve your metadata, etc.

    **Note: For a UI experience, refer to CLI `etlwiz`.**
    """
    pass


# ADD ALL COMMANDS TO THE CLI
for group in GROUPS:
    for name, cmd in group["commands"].items():
        cli.add_command(cmd=cmd, name=name)
# Add dev
# cli.add_command(cli_dev)


################################
#
# RICH CLICK CONFIG
# Configuration rich_click
#
################################

# RICH-CLICK CONFIGURATION
# click.rich_click.USE_RICH_MARKUP = True
click.rich_click.USE_MARKDOWN = True
click.rich_click.SHOW_ARGUMENTS = True
# click.rich_click.STYLE_HEADER_TEXT = "bold"
# click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
# Show variable types under description
click.rich_click.SHOW_METAVARS_COLUMN = False
click.rich_click.APPEND_METAVARS_HELP = True
click.rich_click.OPTION_ENVVAR_FIRST = True
# click.rich_click.USE_CLICK_SHORT_HELP = True

## Convert GROUPS and SUBROUPS to the format expected by rich-click, and submit the ordering and groups so they are shown in the terminal (--help).
command_groups = [
    {
        "name": group["name"],
        "commands": list(group["commands"].keys()),
    }
    for group in GROUPS
]
commands_subgroups = {
    f"etlcli {subgroup['alias']}": [
        {
            "name": "Commands",
            "commands": list(subgroup["commands"].keys()),
        }
    ]
    for subgroup in SUBGROUPS
}
click.rich_click.COMMAND_GROUPS = {
    "etlcli": command_groups,
    **commands_subgroups,
}
