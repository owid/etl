"""Join explorer views to MDIM views into a wide mapping proposal.

Reads, from ``--out``:
- ``explorer_views.csv``        (from extract_views.py)
- ``multidim_<short>_views.csv`` (one per MDIM, from extract_views.py)
- ``mapping_rules.py``          (written by you, per explorer — see _scaffold.md)

``mapping_rules.py`` must define:
- ``EXPLORER_DIMENSIONS``: list[str] naming dimension_1..N (the explorer column order)
- ``MDIMS``: list[str] of MDIM short names, in the prefix order A, B, C, ...
- ``route(dims) -> str``: explorer-view dims dict -> target MDIM short name
- ``translate(dims, mdim) -> dict``: -> {mdim_dim_slug: choice_slug} for that MDIM

Writes ``mapping_proposal.csv`` (one row per explorer view):
    id, dimension_1..N,
    target_mdim, target_view_id,
    <mdim>_<dimslug> ... (wide; only the target MDIM's columns are filled),
    shared_target_explorer_ids  (when >1 explorer view hits the same MDIM view,
                                 the comma-joined list of all those explorer ids)

Usage:
    .venv/bin/python .claude/skills/map-explorer-to-mdim/scripts/build_mapping.py --out ai/<folder>
"""

import argparse
import csv
import importlib.util
from collections import defaultdict
from pathlib import Path


def load_rules(out: Path):
    spec = importlib.util.spec_from_file_location("mapping_rules", out / "mapping_rules.py")
    if spec is None or spec.loader is None:
        raise SystemExit(f"Could not load {out / 'mapping_rules.py'}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for attr in ("EXPLORER_DIMENSIONS", "MDIMS", "route", "translate"):
        if not hasattr(mod, attr):
            raise SystemExit(f"mapping_rules.py is missing `{attr}`")
    return mod


def read_csv(path: Path):
    with open(path) as f:
        r = csv.reader(f)
        header = next(r)
        return header, list(r)


def main():
    ap = argparse.ArgumentParser(description="Build the explorer->MDIM mapping proposal.")
    ap.add_argument(
        "--out", required=True, help="Folder with explorer_views.csv, multidim_*_views.csv, mapping_rules.py"
    )
    args = ap.parse_args()
    out = Path(args.out)

    rules = load_rules(out)

    # Explorer views
    exp_header, exp_rows = read_csv(out / "explorer_views.csv")
    n_dims = len(exp_header) - 1  # minus id
    if len(rules.EXPLORER_DIMENSIONS) != n_dims:
        raise SystemExit(
            f"EXPLORER_DIMENSIONS has {len(rules.EXPLORER_DIMENSIONS)} names but "
            f"explorer_views.csv has {n_dims} dimension columns."
        )

    # MDIM views: short -> (dim_slugs, {tuple(values): view_id})
    mdim_dims = {}
    mdim_lut = {}
    for short in rules.MDIMS:
        header, rows = read_csv(out / f"multidim_{short}_views.csv")
        dim_slugs = header[1:]  # minus id
        mdim_dims[short] = dim_slugs
        lut = {}
        for row in rows:
            lut[tuple(row[1:])] = row[0]
        mdim_lut[short] = lut

    # Wide MDIM dimension columns, in MDIMS order.
    mdim_cols = []  # list of (short, dim_slug, column_name)
    for short in rules.MDIMS:
        for dslug in mdim_dims[short]:
            mdim_cols.append((short, dslug, f"{short}_{dslug}"))

    header = (
        ["id"]
        + [f"dimension_{i + 1}" for i in range(n_dims)]
        + ["target_mdim", "target_view_id"]
        + [c for _, _, c in mdim_cols]
        + ["shared_target_explorer_ids"]
    )

    out_rows = []
    flags = []
    ids_by_target = defaultdict(list)  # (mdim, view_id) -> [explorer ids]

    for er in exp_rows:
        eid, evals = er[0], er[1:]
        dims = dict(zip(rules.EXPLORER_DIMENSIONS, evals))

        mdim = rules.route(dims)
        if mdim not in rules.MDIMS:
            raise SystemExit(f"route() returned unknown MDIM {mdim!r} (not in MDIMS)")
        target = rules.translate(dims, mdim)

        key = tuple(target.get(s, "") for s in mdim_dims[mdim])
        view_id = mdim_lut[mdim].get(key, "")

        row = {c: "" for c in header}
        row["id"] = eid
        for i, v in enumerate(evals):
            row[f"dimension_{i + 1}"] = v
        row["target_mdim"] = mdim
        row["target_view_id"] = view_id
        for s, dslug, col in mdim_cols:
            if s == mdim and dslug in target:
                row[col] = target[dslug]

        if view_id:
            ids_by_target[(mdim, view_id)].append(eid)
        else:
            flags.append(f"id={eid}: no {mdim} view for {target} (from {dims})")
        out_rows.append((row, mdim, view_id))

    # shared_target_explorer_ids: fill when >1 explorer view shares a target MDIM view.
    for row, mdim, view_id in out_rows:
        if view_id:
            sharers = ids_by_target[(mdim, view_id)]
            if len(sharers) > 1:
                row["shared_target_explorer_ids"] = ",".join(sharers)

    path = out / "mapping_proposal.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(r for r, _, _ in out_rows)

    # Report
    resolved = sum(1 for _, _, v in out_rows if v)
    print(f"-> {path}")
    print(f"explorer views: {len(out_rows)}  |  resolved: {resolved}  |  unresolved: {len(out_rows) - resolved}")
    per_mdim = defaultdict(lambda: [0, set()])
    for _, mdim, v in out_rows:
        per_mdim[mdim][0] += 1
        if v:
            per_mdim[mdim][1].add(v)
    for mdim, (n, vids) in per_mdim.items():
        print(f"  {mdim}: {n} explorer views -> {len(vids)} distinct MDIM views")
    shared = sum(1 for r, _, _ in out_rows if r["shared_target_explorer_ids"])
    print(f"rows pointing at a shared MDIM view: {shared}")
    if flags:
        print("\nFLAGS (unresolved):")
        for fl in flags:
            print("  -", fl)
    else:
        print("\nNo flags: every explorer view resolved to exactly one MDIM view.")


if __name__ == "__main__":
    main()
