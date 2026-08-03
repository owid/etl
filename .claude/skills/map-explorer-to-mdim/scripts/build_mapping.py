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

Also writes ``mapping.json`` — a redirect payload for an owid-grapher API to consume.
It carries the full source + target identifiers (explorer slug + dimension name→value
for the source; MDIM catalogPath + dimension slug→choice-slug + our internal view id for
the target), so a redirect from an explorer view URL to an MDIM view URL can be built
without re-reading the DB. Requires ``_sources.json`` (written by extract_views.py).

Usage:
    .venv/bin/python .claude/skills/map-explorer-to-mdim/scripts/build_mapping.py --out ai/<folder>
"""

import argparse
import csv
import importlib.util
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from redirect_rules import (  # noqa: E402
    build_source_rules,
    duplicate_conditions,
    strip_empty,
    views_fingerprint,
)


@dataclass
class ViewMapping:
    """One explorer view and where it maps to."""

    eid: str  # explorer view id, "1".."N"
    dims: dict  # {explorer dimension name: display value}
    mdim: str  # target MDIM short name
    view_id: str  # target MDIM view id ("A2", ...); "" when unresolved
    target: dict  # {mdim dim slug: choice slug}; partial when unresolved
    csv_row: dict  # the wide mapping_proposal.csv row

    @property
    def resolved(self) -> bool:
        return bool(self.view_id)


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


def pick_default_mdim(rules, mappings) -> str:
    """The catch-all target: the best-fitting MDIM for the bare explorer URL.

    ``mapping_rules.py`` may set ``DEFAULT_MDIM = "<short>"`` to force it; otherwise it's
    the MDIM that receives the most resolved explorer views (tie-break: earliest in MDIMS).
    """
    override = getattr(rules, "DEFAULT_MDIM", None)
    if override is not None:
        if override not in rules.MDIMS:
            raise SystemExit(f"DEFAULT_MDIM {override!r} is not in MDIMS {rules.MDIMS}")
        return override
    counts = {short: 0 for short in rules.MDIMS}
    for m in mappings:
        if m.resolved:
            counts[m.mdim] += 1
    # Higher count wins; on a tie, the earlier MDIM (smaller index) wins.
    return max(rules.MDIMS, key=lambda s: (counts[s], -rules.MDIMS.index(s)))


def write_mapping_json(
    out: Path, explorer_slug, explorer_dims, mdims_meta, ids_by_target, mappings, default_mdim
) -> Path:
    """Write mapping.json — a redirect payload for an owid-grapher API to consume.

    Structure::

        {
          "explorer": {"slug": ..., "dimensions": [<names>]},
          "targets":  [{"mdim": ..., "catalogPath": ..., "dimensions": [<slugs>]}],
          "stats":    {"total": N, "resolved": N, "unresolved": N},
          "catchAll": {                     # bare explorer URL -> best-fitting MDIM default view
            "source": {"explorerSlug": ...},                 # no query params
            "target": {"mdim": ..., "catalogPath": ...,
                       "viewId": null, "dimensions": {}}     # no query params = MDIM default view
          },
          "redirects": [
            {
              "sourceViewId": 1,
              "source": {"explorerSlug": ..., "dimensions": {<name>: <value>}},
              "target": {  # null when unresolved
                "mdim": ..., "catalogPath": ..., "viewId": "A2",
                "dimensions": {<slug>: <choiceSlug>}
              },
              "sharedTargetSourceIds": [1, 12],   # present only when >1 source shares this view
              "unresolvedReason": "..."           # present only when target is null
            },
            ...
          ]
        }

    All identifiers a redirect needs are here: the source view is (explorer slug +
    dimension name→value), the target view is (MDIM catalogPath + dimension
    slug→choice-slug, plus our internal ``viewId`` for cross-referencing the CSVs).
    ``catchAll`` is the fallback for the bare explorer URL (and any view a consumer
    chooses not to route individually): it points at the best-fitting MDIM with no query
    params, which grapher renders as that MDIM's default view.
    """
    catalog_path_of = {m["short"]: m["catalogPath"] for m in mdims_meta}
    # `mdimSlug` is what a /grapher/<slug> URL needs. Carrying it here means the audit and
    # preflight can build target URLs from the mapping alone; the bulk endpoint's Zod schema
    # strips unknown keys, so adding it does not affect what can be posted. It is OMITTED
    # rather than emitted empty when unknown (a run extracted before slugs were recorded),
    # because an empty string reads like a real empty slug — the consumers resolve a missing
    # one from the DB instead.
    slug_of = {m["short"]: m.get("slug") for m in mdims_meta if m.get("slug")}
    # {mdim short -> {view id -> {configId, md5}}}: the rendering each target was reviewed
    # against. Carried so preflight can detect a view whose config was edited in place, which
    # changes neither its id nor its dimensions.
    views_of = {m["short"]: (m.get("views") or {}) for m in mdims_meta}

    def with_slug(mdim: str, target: dict, view_id: str = "") -> dict:
        if slug_of.get(mdim):
            target["mdimSlug"] = slug_of[mdim]
        reviewed = views_of.get(mdim, {}).get(view_id) if view_id else None
        if reviewed:
            target["viewConfigId"] = reviewed.get("configId", "")
            target["viewConfigMd5"] = reviewed.get("md5", "")
        return target

    catch_all = {
        "source": {"explorerSlug": explorer_slug},
        "target": with_slug(
            default_mdim,
            {
                "mdim": default_mdim,
                "catalogPath": catalog_path_of.get(default_mdim),
                "viewId": None,
                "dimensions": {},
            },
        ),
    }
    redirects = []
    for m in mappings:
        record = {
            "sourceViewId": int(m.eid),
            "source": {"explorerSlug": explorer_slug, "dimensions": m.dims},
        }
        if m.resolved:
            record["target"] = with_slug(
                m.mdim,
                {
                    "mdim": m.mdim,
                    "catalogPath": catalog_path_of.get(m.mdim),
                    "viewId": m.view_id,
                    "dimensions": m.target,
                },
                m.view_id,
            )
            sharers = ids_by_target[(m.mdim, m.view_id)]
            if len(sharers) > 1:
                record["sharedTargetSourceIds"] = [int(s) for s in sharers]
        else:
            record["target"] = None
            record["unresolvedReason"] = f"No view in MDIM '{m.mdim}' matches the translated dimensions {m.target}"
        redirects.append(record)

    resolved = sum(1 for m in mappings if m.resolved)
    payload = {
        "explorer": {"slug": explorer_slug, "dimensions": explorer_dims},
        "targets": [
            {
                "mdim": m["short"],
                "catalogPath": m["catalogPath"],
                "dimensions": m["dimensions"],
                # Only present for runs extracted after these were recorded.
                **({"mdimSlug": m["slug"]} if m.get("slug") else {}),
                **({"published": m["published"]} if m.get("published") is not None else {}),
            }
            for m in mdims_meta
        ],
        "stats": {"total": len(mappings), "resolved": resolved, "unresolved": len(mappings) - resolved},
        "catchAll": catch_all,
        "redirects": redirects,
    }
    path = out / "mapping.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    return path


def write_admin_payload(out: Path, mapping: dict, allow_duplicates: bool) -> tuple[Path, int, list[str]]:
    """Write the apply-ready `admin_bulk_payload.json` — one explorer per file.

    Two differences from `mapping.json`, both load-bearing:

    1. **Empty-valued source dimensions are deleted.** The request-time matcher compares a
       condition against the incoming URL's params, and an absent param is not an empty
       string — so a condition of `{"Period": ""}` can never match, and every view carrying
       one silently falls through to the catch-all instead of its intended target. This is
       the one mistake that yields a payload the endpoint accepts and then serves wrongly.
    2. **Duplicate conditions abort.** Once stripped, two views can collapse to the same
       condition; the endpoint rejects the second, so the row would be unreachable. Failing
       here beats discovering it at apply time, when there is no bulk undo.

    `mapping.json` keeps the empties: it is the faithful record of the explorer's view grid.
    One file per explorer is a correctness requirement, not a convention — the endpoint's
    schema has a single `catchAll`, so a merged file would silently drop all but one.
    """
    payload = json.loads(json.dumps(mapping))  # deep copy; mapping.json must stay unstripped
    stripped = 0
    for holder in [payload.get("catchAll") or {}, *(payload.get("redirects") or [])]:
        dims = (holder.get("source") or {}).get("dimensions")
        if not dims:
            continue
        kept = strip_empty(dims)
        stripped += len(dims) - len(kept)
        holder["source"]["dimensions"] = kept

    clashes = duplicate_conditions(build_source_rules(payload))
    errors = [
        f"views {a.source_view_id or 'catchAll'} and {b.source_view_id or 'catchAll'} both reduce to "
        f"condition {a.condition or '{}'} — the endpoint rejects the second, so it would never serve"
        for a, b in clashes
    ]
    if errors and not allow_duplicates:
        raise SystemExit(
            "Duplicate source conditions — refusing to write a payload with unreachable rows:\n  "
            + "\n  ".join(errors)
            + "\nFix the mapping so each view has a distinct condition, or pass "
            "--allow-duplicate-conditions to drop the later duplicates and report them."
        )
    if errors:
        # Match on sourceViewId: `clashes` holds SourceRule objects built from the payload, so
        # comparing object identity against the payload's own dicts could never match and the
        # rows the report claimed to drop would still be posted — and then rejected.
        drop = {b.source_view_id for _, b in clashes if b.source_view_id is not None}
        payload["redirects"] = [r for r in payload.get("redirects") or [] if r.get("sourceViewId") not in drop]

    path = out / "admin_bulk_payload.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    return path, stripped, errors


def check_fingerprint(out: Path, sources: dict, dim_names: list[str], rows: list[list[str]]) -> None:
    """Abort when `explorer_views.csv` no longer matches what extraction recorded.

    Catches a hand-edited CSV, which would silently renumber the positional view ids that
    every downstream artifact keys on. The live-vs-recorded check is preflight's job.
    """
    recorded = (sources.get("explorer") or {}).get("viewsFingerprint")
    if not recorded:
        return  # extracted before fingerprints existed; preflight still checks against live
    actual = views_fingerprint(dim_names, rows)
    if actual != recorded:
        raise SystemExit(
            f"explorer_views.csv does not match _sources.json ({actual} != {recorded}).\n"
            "The view grid changed since extraction, so the positional ids in every artifact "
            "are no longer trustworthy. Re-run extract_views.py, re-review, and rebuild."
        )


def main():
    ap = argparse.ArgumentParser(description="Build the explorer->MDIM mapping proposal.")
    ap.add_argument(
        "--out", required=True, help="Folder with explorer_views.csv, multidim_*_views.csv, mapping_rules.py"
    )
    ap.add_argument(
        "--allow-duplicate-conditions",
        action="store_true",
        help="Drop later duplicate source conditions instead of aborting (they can never serve).",
    )
    args = ap.parse_args()
    out = Path(args.out)

    rules = load_rules(out)

    # Source/target identifiers (explorer slug, MDIM catalogPaths) for the redirect JSON.
    sources_path = out / "_sources.json"
    if not sources_path.exists():
        raise SystemExit(
            f"Missing {sources_path}. Re-run extract_views.py (it now writes _sources.json), "
            "then re-run this script. Your mapping_rules.py is preserved."
        )
    sources = json.loads(sources_path.read_text())
    explorer_slug = sources["explorer"]["slug"]

    # Explorer views
    exp_header, exp_rows = read_csv(out / "explorer_views.csv")
    n_dims = len(exp_header) - 1  # minus id
    if len(rules.EXPLORER_DIMENSIONS) != n_dims:
        raise SystemExit(
            f"EXPLORER_DIMENSIONS has {len(rules.EXPLORER_DIMENSIONS)} names but "
            f"explorer_views.csv has {n_dims} dimension columns."
        )

    check_fingerprint(out, sources, rules.EXPLORER_DIMENSIONS, [r[1:] for r in exp_rows])

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

    mappings: list[ViewMapping] = []
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
        mappings.append(ViewMapping(eid=eid, dims=dims, mdim=mdim, view_id=view_id, target=target, csv_row=row))

    # shared_target_explorer_ids: fill when >1 explorer view shares a target MDIM view.
    for m in mappings:
        if m.resolved and len(ids_by_target[(m.mdim, m.view_id)]) > 1:
            m.csv_row["shared_target_explorer_ids"] = ",".join(ids_by_target[(m.mdim, m.view_id)])

    path = out / "mapping_proposal.csv"
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(m.csv_row for m in mappings)

    # Redirect payload for the owid-grapher API — full source + target identifiers.
    default_mdim = pick_default_mdim(rules, mappings)
    json_path = write_mapping_json(
        out, explorer_slug, rules.EXPLORER_DIMENSIONS, sources["mdims"], ids_by_target, mappings, default_mdim
    )

    mapping = json.loads(json_path.read_text())
    payload_path, stripped, dup_errors = write_admin_payload(out, mapping, args.allow_duplicate_conditions)

    # Report
    resolved = sum(1 for m in mappings if m.resolved)
    print(f"-> {path}")
    print(f"-> {json_path}  (record: empty source dimensions kept)")
    print(f"-> {payload_path}  (APPLY THIS: paste into the admin bulk-redirect modal)")
    n_entries = len(mapping.get("redirects") or []) + (1 if (mapping.get("catchAll") or {}).get("target") else 0)
    print(
        f"   payload entries: {n_entries} (of which skipped at apply: {len(mappings) - resolved})"
        f" | empty source-dim values stripped: {stripped}"
    )
    for err in dup_errors:
        print(f"   DROPPED duplicate: {err}")
    print(f"explorer views: {len(mappings)}  |  resolved: {resolved}  |  unresolved: {len(mappings) - resolved}")
    n_views = defaultdict(int)
    view_ids = defaultdict(set)
    for m in mappings:
        n_views[m.mdim] += 1
        if m.resolved:
            view_ids[m.mdim].add(m.view_id)
    for mdim in n_views:
        print(f"  {mdim}: {n_views[mdim]} explorer views -> {len(view_ids[mdim])} distinct MDIM views")
    shared = sum(1 for m in mappings if m.csv_row["shared_target_explorer_ids"])
    print(f"rows pointing at a shared MDIM view: {shared}")
    print(f"catch-all: bare explorer '{explorer_slug}' -> MDIM '{default_mdim}' default view")
    if flags:
        print("\nFLAGS (unresolved):")
        for fl in flags:
            print("  -", fl)
    else:
        print("\nNo flags: every explorer view resolved to exactly one MDIM view.")


if __name__ == "__main__":
    main()
