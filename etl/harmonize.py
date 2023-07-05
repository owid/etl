#
#  harmonize.py
#  etl
#

import cmd
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Optional, Set, cast

import click
import pandas as pd
from owid.catalog import Dataset
from rapidfuzz import process

from etl.paths import LATEST_REGIONS_DATASET_PATH, LATEST_REGIONS_YML


@click.command()
@click.argument("data_file")
@click.argument("column")
@click.argument("output_file")
@click.argument("institution", required=False)
def harmonize(data_file: str, column: str, output_file: str, institution: Optional[str] = None) -> None:
    """Given a DATA_FILE in feather or CSV format, and the name of the COLUMN representing
    country or region names, interactively generate the JSON mapping OUTPUT_FILE from the given names
    to OWID's canonical names. Optionally, can use INSTITUTION to append "(institution)" to countries.

    When a name is ambiguous, you can use:

    n: to ignore and skip to the next one

    s: to suggest matches; you can also enter a manual match here

    q: to quit and save the partially complete mapping

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

    mapping = interactive_harmonize(geo_column, mapping, institution=institution)
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
) -> Dict[str, str]:
    mapping = mapping or {}

    to_map = sorted(set(geo))
    print(f"{len(to_map)} countries/regions to harmonize")
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

    print(f"  └ {len(to_map) - len(ambiguous)} automatically matched")
    print(f"  └ {len(ambiguous)} ambiguous countries/regions")
    print()

    print("Beginning interactive harmonization...")
    n_skipped = 0
    for i, region in enumerate(ambiguous, 1):
        print(f"\n[{i}/{len(ambiguous)}] {region}")
        # no exact match, get nearby matches
        suggestions = mapper.suggestions(region, institution=institution)

        # ask interactively how to proceed
        picker = GeoPickerCmd(region, suggestions, mapper.valid_names)
        picker.cmdloop()

        if picker.match:
            # we found a match or manually gave a valid one
            name = picker.match
            mapping[region] = name

            if picker.save_alias:
                # update the regions dataset to include this alias
                save_alias_to_regions_yaml(name, region)
                print(f'Saved alias: "{region}" -> "{name}"')
        else:
            n_skipped += 1

        if picker.quit_flag:
            break

    print(f"\nDone! ({len(mapping)} mapped, {n_skipped} skipped)")

    return mapping


class CountryRegionMapper:
    # known aliases of our canonical geo-regions
    aliases: Dict[str, str]
    valid_names: Set[str]

    def __init__(self) -> None:
        ds_regions = Dataset(LATEST_REGIONS_DATASET_PATH)
        rc_df = ds_regions["definitions"]
        aliases_s = ds_regions["aliases"]["alias"]
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

        self.aliases = aliases
        self.valid_names = valid_names

    def __contains__(self, key: str) -> bool:
        return key.lower() in self.aliases

    def __getitem__(self, key: str) -> str:
        return self.aliases[key.lower()]

    def suggestions(self, region: str, institution: Optional[str] = None) -> List[str]:
        # get the aliases which score highest on fuzzy matching
        results = process.extract(region.lower(), self.aliases.keys(), limit=5)
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

        if institution is not None:
            # Prepend the option to include this region as a custom entry for the given institution.
            pairs = [(0, f"{region} ({institution})")] + pairs
        return [m for _, m in pairs]


class GeoPickerCmd(cmd.Cmd):
    """
    An interactive command meant to resolve a single ambiguous geo-region name.
    If there are multiple ambiguous names, you make a new command for each one.

    During this step, you can type "help" to see a list of commands.
    """

    geo: str
    suggestions: List[str]
    valid_names: Set[str]

    match: Optional[str] = None

    quit_flag: bool = False
    save_alias: bool = False

    def __init__(self, geo: str, suggestions: List[str], valid_names: Set[str]) -> None:
        super().__init__()
        self.geo = geo
        self.suggestions = suggestions
        self.valid_names = valid_names
        print("(n) next, (s) suggest, (q) quit")

    def do_n(self, arg: str) -> bool:
        # go to the next item
        return True

    def help_n(self) -> None:
        print("Ignore and skip to the next item")

    def do_s(self, arg: str) -> Optional[bool]:
        for i, s in enumerate(self.suggestions):
            print(f"{i}: {s}")
        print("(or type a name manually)")
        choice = input("> ")
        if not choice:
            return None

        if choice.isdigit():
            # it's one of the suggested options
            i = int(choice)
            self.match = self.suggestions[i]
        else:
            # it's a manual entry, make sure it's valid
            choice = choice.strip()
            if choice in self.valid_names:
                self.match = choice.strip()
                self.save_alias = input_bool("Save this alias")
            else:
                # it's a manual entry that does not correspond to any known country
                print(f"Using custom entry '{choice}' that does not match a country/region from the regions set")
                self.match = choice

        return True

    def help_s(self) -> None:
        print("Suggest possible matches, or manually enter one yourself")

    def do_q(self, arg: str) -> bool:
        self.quit_flag = True
        return True

    def help_q(self) -> None:
        print("Quit and save progress so far")


def input_bool(query: str, default: str = "y") -> bool:
    if default == "y":
        options = "(Y/n)"
    elif default == "n":
        options = "(y/N)"
    else:
        raise ValueError(f"Invalid default: {default}")

    print(f"{query}? {options}")
    while True:
        c = input("> ")
        print()
        if c.lower() in ("y", "n", ""):
            break

        print("ERROR: please press y, n, or return")

    return (c.lower() or default) == "y"


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


def print_mapping(region: str, name: str) -> None:
    if region == name:
        click.echo(click.style(f"{region} -> {name}", fg=246))
    else:
        click.echo(click.style(f"{region} -> {name}", fg="blue"))


if __name__ == "__main__":
    harmonize()
