"""Print the prose around every placement of a library image, to check it against a refreshed static viz.

A refreshed static viz can change what the text beside it is allowed to say: a new title, a new
encoding (a band instead of two, a dashed cutoff), a number that moved, a claim the new design
dropped on purpose. This prints, for every gdoc that shows the image, the page's title, its links
(Google Doc, live page) and the blocks just before and after each placement, so each sentence that
describes the chart can be read against the new frames.

READ-ONLY. Point it at a database with the gdocs: a staging server's is a recent copy of production's.

Usage:
    STAGING=<branch> .venv/bin/python .claude/skills/create-static-viz/scripts/image_context.py \\
        <filename> [<filename> ...] [--blocks 4]

A gdoc showing a static viz built on the image (a `{.static-viz}` block) is read too: it
picks up new images by name, but its prose still has to match them.

For a fragment (a data page FAQ doc), only the FAQ entries holding the image are read, plus its body.
"""

import argparse
import json
import re

from etl.config import OWID_ENV

SITE = "https://ourworldindata.org"
WHITESPACE = re.compile(r"\s+")


def span_text(span) -> str:
    """Plain text of one span, following nested children (links, bold)."""
    if isinstance(span, dict):
        if "text" in span:
            return span["text"]
        return "".join(span_text(c) for c in span.get("children", []))
    return ""


def flatten(node, filenames: set[str], out: list[tuple[str, str]]) -> None:
    """Body blocks in reading order, as (kind, text): prose and headings by text, images by filename.

    An image's caption follows it as its own block, since a caption usually describes the chart.
    """
    if isinstance(node, dict):
        kind = node.get("type")
        if kind in ("image", "static-viz"):
            value = node.get("value") if isinstance(node.get("value"), dict) else node
            if kind == "image":
                # The mobile `smallFilename` is the same placement: name the block by whichever
                # of its two files was asked for, so a mobile-only use is found too.
                fn, small = value.get("filename", ""), value.get("smallFilename", "")
                out.append(("IMAGE", small if fn not in filenames and small in filenames else fn))
            else:
                out.append(("STATIC VIZ", value.get("name", "")))
            caption = value.get("caption")
            if isinstance(caption, list):
                out.append(("caption", "".join(span_text(s) for s in caption)))
            elif isinstance(caption, str) and caption.strip():
                out.append(("caption", caption))
            return
        if kind == "text" and isinstance(node.get("value"), list):
            out.append(("text", "".join(span_text(s) for s in node["value"])))
            return
        if kind == "heading" and isinstance(node.get("text"), list):
            out.append(("heading", "".join(span_text(s) for s in node["text"])))
            return
        for value in node.values():
            flatten(value, filenames, out)
    elif isinstance(node, list):
        for value in node:
            flatten(value, filenames, out)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("filenames", nargs="+", help="image filenames, every name of the viz")
    ap.add_argument("--blocks", type=int, default=4, help="blocks to show before and after each placement")
    args = ap.parse_args()
    names = set(args.filenames)

    # A `{.static-viz}` block records no image row, only a link to the static viz by name.
    viz = set(
        OWID_ENV.read_sql(
            "SELECT DISTINCT sv.name FROM static_viz sv JOIN images i ON i.id IN (sv.imageId, sv.mobileImageId) "
            "WHERE i.filename IN %(f)s",
            params={"f": tuple(names)},
        )["name"]
    )
    df = OWID_ENV.read_sql(
        "SELECT pg.id, pg.slug, pg.type, pg.published, pg.content FROM posts_gdocs pg WHERE pg.id IN ("
        "  SELECT x.gdocId FROM posts_gdocs_x_images x JOIN images i ON i.id = x.imageId WHERE i.filename IN %(f)s"
        "  UNION SELECT pgl.sourceId FROM posts_gdocs_links pgl"
        "  WHERE pgl.linkType = 'static-viz' AND pgl.target IN %(v)s"
        ") ORDER BY pg.slug",
        params={"f": tuple(names), "v": tuple(viz) or ("",)},
    )
    static = f" or static viz {', '.join(sorted(viz))}" if viz else ""
    print(f"grapher DB: {OWID_ENV.name} — {len(df)} gdoc(s) show {', '.join(sorted(names))}{static}")

    def placed(kind: str, name: str) -> bool:
        return (kind == "IMAGE" and name in names) or (kind == "STATIC VIZ" and name in viz)

    for r in df.to_dict("records"):
        content = json.loads(r["content"])
        path = f"/data-insights/{r['slug']}" if r["type"] == "data-insight" else f"/{r['slug']}"
        page = "" if r["type"] == "fragment" else f" · page {SITE}{path}"
        draft = "" if r["published"] else " · DRAFT"
        print(f"\n######## {r['slug']} ({r['type']}){draft}")
        print(f"title: {content.get('title')!r}")
        print(f"doc https://docs.google.com/document/d/{r['id']}/edit{page}")
        if content.get("featured-image") in names:
            print("[featured image: the page's thumbnail, set in its front matter]")
        sections: list[tuple[str, object]] = []
        if r["type"] == "fragment" and isinstance(content.get("parsedFaqs"), dict):
            for k, v in content["parsedFaqs"].items():
                blocks: list[tuple[str, str]] = []
                flatten(v, names, blocks)
                if any(placed(kind, name) for kind, name in blocks):
                    sections.append((f"FAQ {k}", v))
        # A fragment can also hold the image in its ordinary body, outside any FAQ entry.
        sections.append(("body", content.get("body", [])))
        for label, node in sections:
            blocks: list[tuple[str, str]] = []
            flatten(node, names, blocks)
            for i in [i for i, (kind, name) in enumerate(blocks) if placed(kind, name)]:
                print(f"--- {label}, block {i + 1} of {len(blocks)}")
                for kind, txt in blocks[max(0, i - args.blocks) : i + args.blocks + 1]:
                    plain = WHITESPACE.sub(" ", txt).strip()
                    print(f"[{kind}] {plain}")


if __name__ == "__main__":
    main()
