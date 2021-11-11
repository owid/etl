#
#  harmonize.py
#  etl
#

from pathlib import Path
from typing import DefaultDict, Dict, List, Optional, Set
from collections import defaultdict
import json
import cmd

import click
import pandas as pd
from thefuzz import process

from owid.catalog import Table, Dataset

from etl.paths import REFERENCE_DATASET


@click.command()
@click.argument("data_file")
@click.argument("column")
@click.argument("output_file")
def harmonize(data_file: str, column: str, output_file: str) -> None:
    """
    Given a data file in feather or CSV format, and the name of the columnn representing
    country or region names, interactively generate a JSON mapping from the given names
    to OWID's canonical names.

    When a name is ambiguous, you can use:

    n: to ignore and skip to the next one

    s: to suggest matches; you can also enter a manual match here

    q: to quit and save the partially complete mapping

    If a mapping file already exists, it will resume where the mapping file left off.
    """
    t = read_table(data_file)
    geo_column = t.reset_index()[column]

    if Path(output_file).exists():
        print("Resuming from existing mapping...")
        with open(output_file, "r") as istream:
            mapping = json.load(istream)
    else:
        mapping = {}

    mapping = interactive_harmonize(geo_column, mapping)
    with open(output_file, "w") as ostream:
        json.dump(mapping, ostream, indent=2)


def read_table(input_file: str) -> Table:
    if input_file.endswith(".feather"):
        return Table.read_feather(input_file)

    if input_file.endswith(".csv"):
        return Table.read_csv(input_file)

    raise ValueError(f"Unsupported file type: {input_file}")


def interactive_harmonize(geo: pd.Series, mapping: Dict[str, str]) -> Dict[str, str]:
    mapper = CountryRegionMapper()

    for region in sorted(set(geo)):
        if region in mapping:
            # we did this one in a previous run
            print_mapping(region, mapping[region])
            continue

        if region in mapper:
            # it's an exact match for a country/region or its known aliases
            name = mapper[region]
            mapping[region] = name
            print_mapping(region, name)
            continue

        # no exact match, get nearby matches
        suggestions = mapper.suggestions(region)

        # ask interactively how to proceed
        picker = GeoPickerCmd(region, suggestions, mapper.valid_names)
        picker.cmdloop()

        if picker.match:
            # we found a match or manually gave a valid one
            name = picker.match
            mapping[region] = name
            print_mapping(region, name)

            if picker.save_alias:
                # update the reference dataset to include this alias
                save_alias(name, region)

        if picker.quit_flag:
            break

    return mapping


class CountryRegionMapper:
    # known aliases of our canonical geo-regions
    aliases: Dict[str, str]
    valid_names: Set[str]

    def __init__(self) -> None:
        rc = Dataset(REFERENCE_DATASET)["countries_regions"]
        aliases = {}
        valid_names = set()
        for _, row in rc.iterrows():
            name = row["name"]
            valid_names.add(name)
            aliases[row["name"].lower()] = name
            if not pd.isnull(row.aliases):
                for alias in json.loads(row.aliases):
                    aliases[alias.lower()] = name

        self.aliases = aliases
        self.valid_names = valid_names

    def __contains__(self, key: str) -> bool:
        return key.lower() in self.aliases

    def __getitem__(self, key: str) -> str:
        return self.aliases[key.lower()]

    def suggestions(self, region: str) -> List[str]:
        # get the aliases which score highest on fuzzy matching
        results = process.extract(region.lower(), self.aliases.keys(), limit=5)
        if not results:
            return []

        # some of these aliases will actually be for the same country/region,
        # just take the best score for each match
        best: DefaultDict[str, int] = defaultdict(int)
        for match, score in results:
            key = self.aliases[match]
            best[key] = max(best[key], score)

        # return them in descending order
        pairs = sorted([(s, m) for m, s in best.items()], reverse=True)
        return [m for _, m in pairs]


class GeoPickerCmd(cmd.Cmd):
    """
    An interactive command meant to resolve a single ambiguous geo-region name.
    If there are multipe ambiguous names, you make a new command for each one.

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
        print('No match found for "{}"'.format(geo))

    def do_n(self, arg: str) -> bool:
        # go to the next item
        return True

    def help_n(self) -> None:
        print("Go to the next item")

    def do_s(self, arg: str) -> Optional[bool]:
        for i, s in enumerate(self.suggestions):
            print(f"{i}: {s}")
        choice = input("> ")
        if not choice:
            return None

        if choice.isdigit():
            i = int(choice)
            self.match = self.suggestions[i]

        elif choice in self.valid_names:
            self.match = choice

        else:
            print(f"{choice} is not a valid country name")
            return None

        self.save_alias = input_bool("Save this alias")
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


def save_alias(name: str, alias: str) -> None:
    """
    Update the reference country/region dataset to include this alias.
    """
    # load it
    ref = Dataset(REFERENCE_DATASET)
    rc = ref["countries_regions"]

    # get the existing aliases for this country/region
    aliases_json = rc.loc[rc.name == name, "aliases"].iloc[0]
    aliases = set(json.loads(aliases_json if not pd.isnull(aliases_json) else "[]"))

    # add our new one
    aliases.add(alias)

    # pack up and save
    rc.loc[rc.name == name, "aliases"] = json.dumps(sorted(aliases))
    ref.add(rc, "csv")


def print_mapping(region: str, name: str) -> None:
    if region == name:
        click.echo(click.style(f"{region} -> {name}", fg=246))
    else:
        click.echo(click.style(f"{region} -> {name}", fg="blue"))


if __name__ == "__main__":
    harmonize()
