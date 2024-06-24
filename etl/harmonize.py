#
#  harmonize.py
#  etl
#

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Literal, Optional, Set, cast

import click
import ipywidgets as widgets
import pandas as pd
import questionary
from IPython.display import display
from owid.catalog import Dataset, Table, Variable
from rapidfuzz import process
from rich_click.rich_command import RichCommand

from etl.exceptions import RegionDatasetNotFound
from etl.helpers import PathFinder
from etl.paths import LATEST_REGIONS_DATASET_PATH, LATEST_REGIONS_YML

# Style for questionary
SHELL_FORM_STYLE = questionary.Style(
    [
        ("qmark", "fg:#fac800 bold"),  # token in front of the question
        ("question", "bold"),  # question text
        ("answer", "fg:#fac800 bold"),  # submitted answer text behind the question
        ("pointer", "fg:#fac800 bold"),  # pointer used in select and checkbox prompts
        ("highlighted", "bg:#fac800 fg:#000000 bold"),  # pointed-at choice in select and checkbox prompts
        ("selected", "fg:#54cc90"),  # style for a selected item of a checkbox
        ("separator", "fg:#cc5454"),  # separator in lists
        # ('instruction', ''),                # user instructions for select, rawselect, checkbox
        ("text", ""),  # plain text
        # ('disabled', 'fg:#858585 italic')   # disabled choices for select and checkbox prompts
    ]
)
# Default number of suggestions
DEFAULT_NUM_SUGGESTIONS = 5
# Default column-to-harmonize name
COLUMN_HARMONIZE = "country"


@click.command(name="harmonize", cls=RichCommand)
@click.argument("data_file")
@click.argument("column")
@click.argument("output_file")
@click.option(
    "--institution",
    "-i",
    required=False,
    default=None,
    help="Append '(institution)' to countries",
)
@click.option(
    "--num-suggestions",
    "-n",
    required=False,
    default=DEFAULT_NUM_SUGGESTIONS,
    help=f"Number of suggestions to show per entity. Default is {DEFAULT_NUM_SUGGESTIONS}",
)
def harmonize(
    data_file: str, column: str, output_file: str, num_suggestions: int, institution: Optional[str] = None
) -> None:
    """Generate a dictionary with the mapping of country names to OWID's canonical names.

    Harmonize the country names in `COLUMN` of a `DATA_FILE` (CSV or feather) and save the mapping to `OUTPUT_FILE` as a JSON file. The harmonization process is done according to OWID's canonical country names.

    The harmonization process is done interactively, where the user is prompted with a list of ambiguous country names and asked to select the correct country name from a list of suggestions (ranked by similarity).

    When the mapping is ambiguous, you can use:

    - Choose Option [custom] to enter a custom name.
    - Type `Ctrl-C` to exit and save the partially complete mapping


    If a mapping file already exists, it will resume where the mapping file left off.
    """
    # Load
    df = read_table(data_file)

    # Create Harmonizer
    harmonizer = Harmonizer(
        tb=df,
        colname=column,
        output_file=output_file,
    )

    # Run automatic harmonization
    harmonizer.run_automatic(logging="shell")

    # Need user input
    harmonizer.run_interactive_terminal(institution, num_suggestions)

    # Export
    harmonizer.export_mapping()


def harmonize_ipython(
    tb: Table,
    column: Optional[str] = None,
    output_file: Optional[str] = None,
    paths: Optional[PathFinder] = None,
    num_suggestions: int = 100,
    institution: Optional[str] = None,
):
    # Create Harmonizer
    harmonizer = Harmonizer(
        tb=tb,
        colname=column,
        output_file=output_file,
        paths=paths,
    )

    # Run automatic harmonization
    harmonizer.run_automatic(logging="ipython")

    # Need user input
    harmonizer.run_interactive_ipython(
        institution=institution,
        num_suggestions=num_suggestions,
    )


def read_table(input_file: str) -> pd.DataFrame:
    if input_file.endswith(".feather"):
        df = pd.read_feather(input_file)

    elif input_file.endswith(".csv"):
        df = pd.read_csv(input_file, index_col=False, na_values=[""], keep_default_na=False)

    else:
        raise ValueError(f"Unsupported file type: {input_file}")

    return cast(pd.DataFrame, df)


class CountryRegionMapper:
    # known aliases of our canonical geo-regions
    aliases: Dict[str, str]
    valid_names: Set[str]

    def __init__(self) -> None:
        try:
            tb_regions = Dataset(LATEST_REGIONS_DATASET_PATH)["regions"]
        except FileNotFoundError:
            raise RegionDatasetNotFound(
                "Region dataset not found. Please run `etl run regions` to generate it locally. It should live in `data/`."
            )
        rc_df = tb_regions[["name", "short_name", "region_type", "is_historical", "defined_by"]]
        # Convert strings of lists of aliases into lists of aliases.
        tb_regions["aliases"] = [json.loads(alias) if pd.notnull(alias) else [] for alias in tb_regions["aliases"]]
        # Explode list of aliases to have one row per alias.
        aliases_s = tb_regions["aliases"].explode().dropna()
        aliases = {}
        valid_names = set()
        for row in rc_df.itertuples():
            name = row.name  # type: ignore
            code = row.Index  # type: ignore
            valid_names.add(name)
            aliases[name.lower()] = name
            if code in aliases_s.index:
                for alias in aliases_s.loc[[code]]:
                    aliases[alias.lower()] = name
            # Include the region code itself as another alias.
            aliases[code.lower()] = name

        self.aliases = aliases
        self.valid_names = valid_names

    def __contains__(self, key: str) -> bool:
        return key.lower() in self.aliases

    def __getitem__(self, key: str) -> str:
        return self.aliases[key.lower()]

    def suggestions(self, region: str, institution: Optional[str] = None, num_suggestions: int = 5) -> List[str]:
        # get the aliases which score highest on fuzzy matching
        results = process.extract(region.lower(), self.aliases.keys(), limit=1000)

        if not results:
            return []

        # some of these aliases will actually be for the same country/region,
        # just take the best score for each match
        best: DefaultDict[str, int] = defaultdict(int)
        for match, score, _ in results:
            key = self.aliases[match]
            best[key] = max(best[key], int(score))

        # return them in descending order
        pairs = sorted([(s, m) for m, s in best.items()], reverse=True)

        # only keep top N
        pairs = pairs[:num_suggestions]

        if institution is not None:
            # Prepend the option to include this region as a custom entry for the given institution.
            pairs = [(0, f"{region} ({institution})")] + pairs
        return [m for _, m in pairs]


def save_alias_to_regions_yaml(name: str, alias: str) -> None:
    """
    Save alias to regions.yml definitions. It doesn't modify original formatting of the file, but assumes
    that `alias` is always the last element in the region block.
    """
    with open(LATEST_REGIONS_YML, "r") as f:
        yaml_content = f.read()

    with open(LATEST_REGIONS_YML, "w") as f:
        f.write(_add_alias_to_regions(yaml_content, name, alias))


def _add_alias_to_regions(yaml_content, target_name, new_alias):
    # match block that contains target name
    pattern = f'name: "{re.escape(target_name)}"(?:.(?!- code:))*'
    match = re.search(pattern, yaml_content, re.DOTALL)

    if match:
        existing_aliases = re.search(r'(aliases:\s*\n(?:\s+- "[^"]+"\n)*)', match.group(0))
        if existing_aliases:
            # Add the new alias to the existing aliases
            updated_aliases = existing_aliases.group(1) + f'    - "{new_alias}"\n'
            yaml_content = (
                yaml_content[: match.start()]
                + match.group(0).replace(existing_aliases.group(1), updated_aliases)
                + yaml_content[match.end() :]
            )
        else:
            # Add the aliases key with the new alias
            aliases = f'  aliases:\n    - "{new_alias}"\n'
            yaml_content = yaml_content[: match.end()] + aliases + yaml_content[match.end() :]
    else:
        raise ValueError(f"Could not find region {target_name} in {LATEST_REGIONS_YML}")

    return yaml_content


class Harmonizer:
    def __init__(
        self,
        tb: Optional[Table | pd.DataFrame] = None,
        colname: Optional[str] = None,
        indicator: Optional[Variable | pd.Series] = None,
        output_file: Optional[str] = None,
        paths: Optional[PathFinder] = None,
    ):
        """Constructor

        Parameters:
            * tb: Table that contains country column.
            * colname: Name of the column that contains country names.
            * indicator: Optional. Variable that contains country names. This is an alternative way of providing country names. That is, either give `tb` and `colname` or `indicator`.
            * output_file: Optional. File to save the mapping. If the file exists, it will resume from the existing mapping.
        """
        self.geo = self._get_geo(tb, colname, indicator)
        self.mapper = CountryRegionMapper()
        self.output_file = self._get_output_file(output_file, paths)
        self.ambiguous = None

        # Mapping
        self._mapping = None
        self.countries_mapped_automatic = None

    def _get_geo(self, tb, colname, indicator):
        """Get set of country names to map."""
        if (tb is None) and (indicator is None):
            raise ValueError("Either `tb` or `indicator` must be provided")
        elif indicator is None:
            if colname is None:
                colname = COLUMN_HARMONIZE
            if colname not in tb.columns:
                raise ValueError(f"Column '{colname}' not found in table")
            indicator = tb[colname]

        return sorted(set(indicator.dropna().astype("string").unique()))

    def _get_output_file(self, output_file: Optional[str], paths: Optional[PathFinder]):
        """Get set of country names to map."""
        if (output_file is None) and (paths is None):
            raise ValueError("Either `output_file` or `paths` must be provided")
        elif paths is not None:
            return paths.country_mapping_path
        else:
            return output_file

    @property
    def mapping(self):
        if self._mapping is None:
            # Reload previous work
            if (self.output_file is not None) and Path(self.output_file).exists():
                print("Resuming from existing mapping...\n")
                with open(self.output_file, "r") as istream:
                    self._mapping = json.load(istream)
            else:
                self._mapping = {}
        return self._mapping

    def run_automatic(
        self,
        logging: Optional[Literal["shell", "ipython"]] = None,
    ):
        """Build country mappings that can be done automatically.

        Automation comes from the fact that some country-mapping are already tracked by our regions dataset.

        Parameters:
            `logging`: If `shell`, print messages to the shell. If `ipython`, print messages to the IPython console.
        Returns:
            `ambiguous`: List of countries that could not be mapped automatically.

        NOTE: It also updates class attribute `mapping`, to reflect the mapping done so far.
        """

        # do the easy cases first
        ambiguous = []
        for region in self.geo:
            if region in self.mapping:
                # we did this one in a previous run
                continue

            if region in self.mapper:
                # it's an exact match for a country/region or its known aliases
                name = self.mapper[region]
                self.mapping[region] = name
                continue

            ambiguous.append(region)

        # logging
        if logging == "ipython":
            questionary.print(f"{len(self.geo)} countries/regions to harmonize")
            questionary.print(f"  └ {len(self.geo) - len(ambiguous)} automatically matched")
            questionary.print(f"  └ {len(ambiguous)} ambiguous countries/regions")
            questionary.print("")
        elif logging == "shell":
            print(f"{len(self.geo)} countries/regions to harmonize")
            print(f"  └ {len(self.geo) - len(ambiguous)} automatically matched")
            print(f"  └ {len(ambiguous)} ambiguous countries/regions")
            print("")

        self.ambiguous = ambiguous

    def get_suggestions(self, region: str, institution: Optional[str], num_suggestions: int):
        """Get suggestions for region."""
        return self.mapper.suggestions(region, institution=institution, num_suggestions=num_suggestions)

    def run_interactive_terminal(
        self,
        institution: Optional[str] = None,
        num_suggestions: int = DEFAULT_NUM_SUGGESTIONS,
    ) -> None:
        """Ask user to map countries."""
        if self.ambiguous is None:
            raise ValueError("Run `run_automatic` first!")

        questionary.print("Beginning interactive harmonization...")
        questionary.print("  Select [skip] to skip a country/region mapping")
        questionary.print("  Select [custom] to enter a custom name\n")

        instruction = "(Use shortcuts or arrow keys)"
        # start interactive session
        n_skipped = 0
        try:
            for i, region in enumerate(self.ambiguous, 1):
                # no exact match, get nearby matches
                suggestions = self.get_suggestions(region, institution=institution, num_suggestions=num_suggestions)

                # show suggestions
                name = questionary.select(
                    f"[{i}/{len(self.ambiguous)}] {region}:",
                    choices=suggestions + ["[custom]", "[skip]"],
                    use_shortcuts=True,
                    style=SHELL_FORM_STYLE,
                    instruction=instruction,
                ).unsafe_ask()

                # use custom mapping
                if name == "[custom]":
                    name = questionary.text("Enter custom name:", style=SHELL_FORM_STYLE).unsafe_ask()
                    name = name.strip()
                    if name in self.mapper.valid_names:
                        confirm = questionary.confirm(
                            "Save this alias", default=True, style=SHELL_FORM_STYLE
                        ).unsafe_ask()
                        if confirm:
                            save_alias_to_regions_yaml(name, region)
                    else:
                        # it's a manual entry that does not correspond to any known country
                        questionary.print(
                            f"Using custom entry '{name}' that does not match a country/region from the regions set"
                        )
                elif name == "[skip]":
                    n_skipped += 1
                    continue

                self.mapping[region] = name
        except KeyboardInterrupt:
            questionary.print("Saving session...\n")
        questionary.print(f"\nDone! ({len(self.mapping)} mapped, {n_skipped} skipped)")

    def run_interactive_ipython(
        self,
        institution: Optional[str] = None,
        num_suggestions: int = DEFAULT_NUM_SUGGESTIONS,
    ):
        if self.ambiguous is None:
            raise ValueError("Run `run_automatic` first!")

        # Render widgets
        mappings_raw = []
        for i, country in enumerate(self.ambiguous, start=1):
            ## Get suggestions for country
            options = self.get_suggestions(
                country,
                institution=institution,
                num_suggestions=num_suggestions,
            )

            ## Render widgets
            params = self._widgets_ipython_country(
                label=f"({i}/{len(self.ambiguous)}) {country}",
                country=country,
                options=options,
            )

            ## Add to mapping
            mappings_raw.append(params)

        if mappings_raw != []:
            style = """
            <style>
            .jupyter-widgets-output-area .output_scroll {
                    height: unset !important;
                    border-radius: unset !important;
                    -webkit-box-shadow: unset !important;
                    box-shadow: unset !important;
                }
                .jupyter-widgets-output-area  {
                    height: auto !important;
                }
            </style>
            """
            display(widgets.HTML(style))

            # Show button
            btn = widgets.Button(
                description="Export mapping",
                button_style="info",
                icon="check",
            )

            # Action if clicked on button
            def _on_submit(_):
                widgets.Widget.close_all()
                # Generate mapping from input by user
                mapping_manual = self.generate_country_mapping(mappings_raw)
                # Combine manual mapping with auto mapping
                self._mapping = {**self.mapping, **mapping_manual}
                self.export_mapping()

                def _export_new_name(country_name, alias):
                    def _on_export(country_name, alias, vbox):
                        save_alias_to_regions_yaml(country_name, alias)
                        vbox.close()

                    label = widgets.Label(value=f"Save new name '{country_name}' for alias '{alias}'")
                    btn = widgets.Button(description="Export", icon="save")
                    vbox = widgets.VBox(
                        [label, btn],
                        layout=widgets.Layout(margin="0px 0px 0px 0px", padding="0px", align_items="flex-start"),
                    )
                    btn.on_click(lambda _: _on_export(country_name, alias, vbox))

                    display(vbox)

                # Save new aliases
                for alias, country_name in mapping_manual.items():
                    if country_name in self.mapper.valid_names:
                        _export_new_name(country_name, alias)

            btn.on_click(_on_submit)


            display(btn)

    def _widgets_ipython_country(self, label, country, options):
        """Render ipython widgets for a single country."""
        # Label
        label_w = widgets.HTML(value=f"<b>{label}</b>:")

        # Mapping type: default, custom, ignore
        mapping_type = widgets.RadioButtons(
            options=[
                ("From default list", "DEFAULT"),
                ("Custom name", "CUSTOM"),
                ("Ignore", "IGNORE"),
            ],
            layout=widgets.Layout(width="max-content", padding="0px 10px 0px 0px"),
        )

        # Dropdown (default list)
        selection = widgets.Select(
            options=options,
            # value=options[0],
            layout=widgets.Layout(margin="0px 0px 0px 0px", padding="0px"),  # Adjust the width as needed
            style={"description_width": "initial"},
            disabled=False,
        )
        # Text (custom)
        text = widgets.Text(value="", placeholder="Enter custom name", description="", disabled=False)

        # HBox to hold the Select and Text widgets, initially showing Select
        selection_container = widgets.HBox([selection])

        # Update visibility based on RadioButtons selection
        def _update_visibility(change):
            if change["new"] == "DEFAULT":
                selection_container.children = [selection]
            elif change["new"] == "CUSTOM":
                selection_container.children = [text]
            else:
                selection_container.children = []

        # Observe changes in the RadioButtons
        mapping_type.observe(_update_visibility, names="value")

        hbox = widgets.HBox([mapping_type, selection_container])

        # VBox layout to stack the label and dropdown vertically
        vbox = widgets.VBox(
            [label_w, hbox], layout=widgets.Layout(margin="0px 0px 0px 0px", padding="0px", align_items="flex-start")
        )

        # Align
        hbox = widgets.HBox([vbox])

        display(hbox)

        return {
            "country": country,
            "suggestions": options,
            "widgets": {
                "mapping_type": mapping_type,
                "selection": selection,
                "text": text,
            },
        }

    # Build mapping
    def generate_country_mapping(self, mappings_raw):
        mapping = {}
        for m in mappings_raw:
            if m["widgets"]["mapping_type"].value == "DEFAULT":
                country_name_new = m["widgets"]["selection"].value
            elif m["widgets"]["mapping_type"].value == "CUSTOM":
                country_name_new = m["widgets"]["text"].value
            else:
                continue
            mapping[m["country"]] = country_name_new
        return mapping

    def export_mapping(self):
        if self.output_file is None:
            raise ValueError("`output_file` not provided")
        with open(self.output_file, "w") as ostream:
            json.dump(self.mapping, ostream, indent=2)


if __name__ == "__main__":
    harmonize()
