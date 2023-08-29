#
#  harmonize.py
#  etl
#

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Optional, Set, cast

import click
import pandas as pd
import questionary
from owid.catalog import Dataset
from rapidfuzz import process
from rich_click.rich_command import RichCommand

from etl.paths import LATEST_REGIONS_DATASET_PATH, LATEST_REGIONS_YML

custom_style_fancy = questionary.Style(
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


@click.command(cls=RichCommand)
@click.argument("data_file")
@click.argument("column")
@click.argument("output_file")
@click.argument("institution", required=False)
@click.argument("num_suggestions", required=False, default=5)
def harmonize(
    data_file: str, column: str, output_file: str, num_suggestions: int, institution: Optional[str] = None
) -> None:
    """Given a DATA_FILE in feather or CSV format, and the name of the COLUMN representing
    country or region names, interactively generate the JSON mapping OUTPUT_FILE from the given names
    to OWID's canonical names. Optionally, can use INSTITUTION to append "(institution)" to countries.


    When a name is ambiguous, you can use:

    - Choose Option (9) [custom] to enter a custom name

    - Type `Ctrl-C` to exit and save the partially complete mapping


    If a mapping file already exists, it will resume where the mapping file left off.
    """

    df = read_table(data_file)
    geo_column = cast(pd.Series, df[column].dropna().astype("str"))

    if Path(output_file).exists():
        print("Resuming from existing mapping...\n")
        with open(output_file, "r") as istream:
            mapping = json.load(istream)
    else:
        mapping = {}

    mapping = interactive_harmonize(geo_column, mapping, institution=institution, num_suggestions=num_suggestions)
    with open(output_file, "w") as ostream:
        json.dump(mapping, ostream, indent=2)


def read_table(input_file: str) -> pd.DataFrame:
    if input_file.endswith(".feather"):
        df = pd.read_feather(input_file)

    elif input_file.endswith(".csv"):
        df = pd.read_csv(input_file, index_col=False, na_values=[""], keep_default_na=False)

    else:
        raise ValueError(f"Unsupported file type: {input_file}")

    return cast(pd.DataFrame, df)


def interactive_harmonize(
    geo: pd.Series,
    mapping: Optional[Dict[str, str]] = None,
    institution: Optional[str] = None,
    num_suggestions: int = 5,
) -> Dict[str, str]:
    mapping = mapping or {}

    # prepare data
    to_map = sorted(set(geo))
    questionary.print(f"{len(to_map)} countries/regions to harmonize")
    mapper = CountryRegionMapper()

    # do the easy cases first
    ambiguous = []
    for region in to_map:
        if region in mapping:
            # we did this one in a previous run
            continue

        if region in mapper:
            # it's an exact match for a country/region or its known aliases
            name = mapper[region]
            mapping[region] = name
            continue

        ambiguous.append(region)

    # first summary
    questionary.print(f"  └ {len(to_map) - len(ambiguous)} automatically matched")
    questionary.print(f"  └ {len(ambiguous)} ambiguous countries/regions")
    questionary.print("")

    # actually ask user
    prompt_user(ambiguous, mapping, mapper, institution, num_suggestions)

    return mapping


def prompt_user(
    ambiguous: List[str],
    mapping: Dict[str, str],
    mapper: "CountryRegionMapper",
    institution: Optional[str],
    num_suggestions: int,
) -> Dict[str, str]:
    """Ask user to map countries."""
    questionary.print("Beginning interactive harmonization...")
    questionary.print("  Select [skip] to skip a country/region mapping")
    questionary.print("  Select [custom] to enter a custom name\n")

    instruction = "(Use shortcuts or arrow keys)"
    # start interactive session
    n_skipped = 0
    try:
        for i, region in enumerate(ambiguous, 1):
            # no exact match, get nearby matches
            suggestions = mapper.suggestions(region, institution=institution, num_suggestions=num_suggestions)

            # show suggestions
            name = questionary.select(
                f"[{i}/{len(ambiguous)}] {region}:",
                choices=suggestions + ["[custom]", "[skip]"],
                use_shortcuts=True,
                style=custom_style_fancy,
                instruction=instruction,
            ).unsafe_ask()

            # use custom mapping
            if name == "[custom]":
                name = questionary.text("Enter custom name:", style=custom_style_fancy).unsafe_ask()
                name = name.strip()
                if name in mapper.valid_names:
                    confirm = questionary.confirm(
                        "Save this alias", default=True, style=custom_style_fancy
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

            mapping[region] = name
    except KeyboardInterrupt:
        questionary.print("Saving session...\n")
    questionary.print(f"\nDone! ({len(mapping)} mapped, {n_skipped} skipped)")

    return mapping


class CountryRegionMapper:
    # known aliases of our canonical geo-regions
    aliases: Dict[str, str]
    valid_names: Set[str]

    def __init__(self) -> None:
        tb_regions = Dataset(LATEST_REGIONS_DATASET_PATH)["regions"]
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


if __name__ == "__main__":
    harmonize()
