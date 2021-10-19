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

import pandas as pd
import frictionless

from owid.catalog import Dataset, Table
from etl import frames


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
    resource_map = remap_names(package.resources)

    # copy tables one by one
    for short_name, resources in resource_map.items():
        print(f"- {short_name}")
        all_frames = []
        for resource in resources:
            df = resource.to_pandas()

            # use more accurate column types that minimise space
            frames.repack_frame(df, {"global": "geo"})

            all_frames.append(df)

        df = pd.concat(all_frames)
        frames.repack_frame(df, {})

        t = Table(df)
        t.metadata.short_name = short_name
        ds.add(t)


def remap_names(
    resources: List[frictionless.Resource],
) -> Dict[str, frictionless.Resource]:
    "Short names must be unique, so fix name collisions."
    rows = []
    for resource in resources:
        # ignore categories for now
        if not resource.name.startswith("ddf--datapoints"):
            continue

        name, hashed_name = parse_name(resource.name)
        rows.append(
            {
                "name": name,
                "hashed_name": hashed_name,
                "primary_key": tuple(norm_primary_key(resource.schema.primary_key)),
                "resource": resource,
            }
        )

    df = pd.DataFrame.from_records(rows)

    names = {}
    for name, group in df.groupby("name"):
        if len(group.primary_key.unique()) == 1:
            names[name] = list(group.resource)
            continue

        # multiple primary keys found for the same name, give each a unique suffix
        for primary_key, subgroup in group.groupby("primary_key"):
            suffix = hashlib.md5(",".join(primary_key).encode("utf8")).hexdigest()[:4]
            names[f"{name}_{suffix}"] = list(subgroup.resource)

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


def norm_primary_key(primary_key: List[str]) -> List[str]:
    return [k if k != "global" else "geo" for k in primary_key]
