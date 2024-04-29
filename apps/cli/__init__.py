"""ETL services CLI.

If you want to add a new service, make sure to add it to the `GROUPS` list. If it is part of a subgroup, add it to the corresponding subgroup in the  `SUBGROUPS` list.
"""
import importlib

import rich_click as click

# Styling
# from apps.utils.style import set_rich_click_style
# set_rich_click_style()
click.rich_click.USE_MARKDOWN = True


# Lazy load
# Ref: https://click.palletsprojects.com/en/8.1.x/complex/#lazily-loading-subcommands
class LazyGroup(click.RichGroup):
    """Ref: https://click.palletsprojects.com/en/8.1.x/complex/#lazily-loading-subcommands"""

    def __init__(self, *args, lazy_subcommands=None, **kwargs):
        super().__init__(*args, **kwargs)
        # lazy_subcommands is a map of the form:
        #
        #   {command-name} -> {module-name}.{command-object-name}
        #
        self.lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, ctx):
        base = super().list_commands(ctx)
        lazy = sorted(self.lazy_subcommands.keys())
        return base + lazy

    def get_command(self, ctx, cmd_name):
        if cmd_name in self.lazy_subcommands:
            return self._lazy_load(cmd_name)
        return super().get_command(ctx, cmd_name)

    def _lazy_load(self, cmd_name):
        # lazily loading a command, first get the module name and attribute name
        import_path = self.lazy_subcommands[cmd_name]
        modname, cmd_object_name = import_path.rsplit(".", 1)
        # do the import
        mod = importlib.import_module(modname)
        # get the Command object from that module
        cmd_object = getattr(mod, cmd_object_name)
        # check the result to make debugging easier
        if not isinstance(cmd_object, click.BaseCommand):  # type: ignore
            raise ValueError(f"Lazy loading of {import_path} failed by returning " "a non-command object")
        return cmd_object


################################################################
#
# SUBGROUPS
#
# Configuration of the subcommands `etl d` (development) and `etl b` (backport).
# We define it first because we need to reference it later.
#
# You can edit `SUBGROUPS` to add new subgroups!
#
################################################################
SUBGROUPS = {
    "d": {
        "entrypoint": "apps.cli.cli_dev",
        "description": "Development commands.",
        "name": "Development",
        "commands": {
            "version-tracker": "etl.version_tracker.run_version_tracker_checks",
            "prune": "etl.prune.prune_cli",
            "publish": "etl.publish.publish_cli",
            "reindex": "etl.reindex.reindex_cli",
            "run-python-step": "etl.run_python_step.main",
        },
    },
    "b": {
        "entrypoint": "apps.cli.cli_back",
        "name": "Backport",
        "description": "Backport commands.",
        "commands": {
            "fasttrack": "apps.backport.fasttrack_backport.cli",
            "migrate": "apps.backport.migrate.migrate.cli",
            "bulk": "apps.backport.bulk_backport.bulk_backport",
            "run": "apps.backport.backport.backport_cli",
        },
    },
}


# Development
@click.group(
    name="d",
    context_settings=dict(show_default=True),
    cls=LazyGroup,
    lazy_subcommands=SUBGROUPS["d"]["commands"],
)
def cli_dev() -> None:
    """Run development tools."""
    pass


# Backport
@click.group(
    name="b",
    context_settings=dict(show_default=True),
    cls=LazyGroup,
    lazy_subcommands=SUBGROUPS["b"]["commands"],
)
def cli_back() -> None:
    """Run Backport tools."""
    pass


# Create `subgroups` list, to be used to display commands in `etl --help`
# subgroups = []
# for alias, props in SUBGROUPS.items():
#     subgroups.append(
#         {
#             "name": props["name"],
#             "commands": {
#                 alias: props["entrypoint"],
#                 **{f"{alias} {k}": v for k, v in props["commands"].items()},
#             },
#         }
#     )
subgroups = [
    {
        "name": "Subcommands",
        "commands": {alias: props["entrypoint"] for alias, props in SUBGROUPS.items()},
    }
]
################################################################
#
# MAIN CLIENT
# Configuration of the command `etl`.
#
# You can edit `GROUPS` to add new commands!
#
################################################################

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
                "run": "etl.command.main_cli",
            },
        },
        {
            "name": "Data",
            "commands": {
                "harmonize": "etl.harmonize.harmonize",
                "diff": "etl.datadiff.cli",
                "graphviz": "etl.to_graphviz.to_graphviz",
                "compare": "etl.compare.cli",
                "update": "apps.step_update.cli.cli",
                "archive": "apps.step_update.cli.archive_cli",
                "explorer-update": "apps.explorer_update.cli.cli",
            },
        },
        {
            "name": "Metadata",
            "commands": {
                "metadata-export": "etl.metadata_export.cli",
                "metadata-migrate": "apps.metadata_migrate.cli.cli",
                "metadata-upgrade": "apps.metagpt.cli.main",
            },
        },
        {
            "name": "Charts",
            "commands": {
                "chart-sync": "apps.staging_sync.cli.cli",
                "chart-gpt": "etl.chart_revision.v2.chartgpt.cli",
                "chart-upgrade": "etl.chart_revision.cli.main_cli",
            },
        },
    ]
    # Add subgroups (don't modify)
    + subgroups
    # Others (not so relevant, maybe deprecated one day...)
    + [
        {
            "name": "Others",
            "commands": {
                "variable-match": "etl.match_variables.main_cli",
                "variable-mapping-translate": "etl.variable_mapping_translate.main_cli",
            },
        },
    ]
)


# MAIN CLIENT ENTRYPOINT (no action actually)
## Note that the entrypoint has no action, it is just a group. The commands that fall under it do actually have actions.
lazy_cmds = {}
for group in GROUPS:
    lazy_cmds.update(group["commands"])


@click.group(
    name="etl",
    context_settings=dict(show_default=True),
    cls=LazyGroup,
    lazy_subcommands=lazy_cmds,  # {k: v for group in GROUPS for k, v in group["commands"].items()},
)
def cli() -> None:
    """Run OWID's ETL client.

    Create ETL step templates, compare different datasets, generate dependency visualisations, synchronise charts across different servers, import datasets from non-ETL OWID sources, improve your metadata, etc.

    **Note: For a UI experience, refer to CLI `etlwiz`.**
    """
    pass


################################
#
# RICH CLICK CONFIG
# Configuration rich_click
#
################################

# RICH-CLICK CONFIGURATION
# set_rich_click_style()

# Actually use GROUPS to show the commands in the terminal in the right order.
# We do some tweaking of SUBROUPS, so that they are also shown in their corresponding subcommands (`etl d --help` and `etl b --help`).
commands_subgroups = {
    f"etl {alias}": [
        {
            "name": "Commands",
            "commands": list(subgroup["commands"].keys()),
        }
    ]
    for alias, subgroup in SUBGROUPS.items()
}
click.rich_click.COMMAND_GROUPS = {
    "etl": GROUPS,
    **commands_subgroups,
}
