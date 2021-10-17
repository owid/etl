#
#  open_numbers.py
#  etl
#

"""
Convert repositories with data in DDF format to OWID's Dataset and Table
format.
"""

from pathlib import Path
from typing import Dict, List, Tuple
import hashlib

import frictionless

from owid.catalog import Dataset, Table


def run(dest_dir: str) -> None:
    # identify the short name and the repo
    short_name = dest_dir.split("/")[-1]
    repo_short_name = short_name.replace("__", "--")
    repo_path = Path(f"~/.owid/git/open-numbers/ddf--{repo_short_name}").expanduser()
    assert repo_path.is_dir()

    # load it as a frictionless package
    package = frictionless.Package((repo_path / "datapackage.json"))

    # make an empty dataset
    ds = Dataset.create_empty(dest_dir)

    # copy metadata from frictionless
    ds.metadata.short_name = short_name
    ds.metadata.title = package.title
    if package.description and package.title != package.description:
        ds.metadata.description = package.description
    ds.save()

    # name remapping
    named_resources = remap_names(package.resources)

    # copy tables one by one
    for short_name, resource in named_resources.items():
        print(f"- {short_name}")
        df = resource.to_pandas()
        t = Table(df)
        t.metadata.short_name = short_name
        ds.add(t)


def remap_names(
    resources: List[frictionless.Resource],
) -> Dict[str, frictionless.Resource]:
    "Short names must be unique, so fix name collisions."
    blacklist = set()
    names = {}
    for resource in resources:
        # ignore categories for now
        if not resource.name.startswith("ddf--datapoints"):
            continue

        preferred_name, hashed_name = parse_name(resource.name)
        if preferred_name not in names and preferred_name not in blacklist:
            names[preferred_name] = resource

        else:
            # collision!
            if preferred_name not in blacklist:
                # move the original entry
                blacklist.add(preferred_name)
                r = names.pop(preferred_name)
                r_hash = parse_name(r.name)[1]
                names[r_hash] = r

            names[hashed_name] = resource

    return names


def parse_name(name: str) -> Tuple[str, str]:
    # ddf files have names like "ddf--datapoints--deaths--by--country--age--year.csv""""
    parts = name[len("ddf--datapoints--") : -4].split("--")
    assert parts[1] == "by"

    preferred_name = parts[0]
    # suffix is a content-based hash of the dimension names
    hash_suffix = hashlib.md5("__".join(parts[2:]).encode("utf8")).hexdigest()[:4]
    hashed_name = f"{preferred_name}_{hash_suffix}"

    return preferred_name, hashed_name
