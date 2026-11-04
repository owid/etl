"""Download the published image a static-viz refresh replaces, ready to place on its Figma page.

A refresh is reviewed against what readers see today, and that is the published image, not the step's
own previous render. So the Figma page carries it, leftmost, as the "before" every reviewer compares
against. This script finds it in the grapher `images` table by filename, downloads it from Cloudflare
Images, and prints the layer name to give it in Figma.

Usage:
    fetch_original_image.py <filename-or-fragment> [--out <dir>] [--width <px>]

Run it with the repo venv (`.venv/bin/python`): it reads the grapher DB through `etl.db`, so it
honours `STAGING=<branch>` like every other DB read. A fragment matches with `LIKE %…%`; more than one
match is listed and nothing is downloaded, so pass the exact filename then.

What the file is, and is not:
- The served bytes are usually JPEG whatever the stored filename's extension says (Cloudflare picks
  the format), so the file is saved with the extension of the content actually returned.
- `--width` defaults to 2550, three times the 850px desktop template, which is plenty for a reference
  copy; old originals run to 5000+px and do not need to be pulled in full.
- In a cloud session there is no DB. Then take the filename's `cloudflare_id` from wherever you have
  it (the static-viz popularity cache's `images.json`) and build the URL by hand from `IMAGE_URL`.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from etl.db import read_sql
from etl.http import session as http_session

# The public delivery URL of every image on ourworldindata.org: the account hash is the fixed part of
# each `<img src>` on the site, the image id is the row's `cloudflareId`.
IMAGE_URL = "https://ourworldindata.org/cdn-cgi/imagedelivery/qLq-8BTgXU8yG0N6HnOy8g/{cloudflare_id}/w={width}"

EXTENSIONS = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/svg+xml": ".svg"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("filename", help="the image's stored filename, or a fragment of it")
    parser.add_argument("--out", type=Path, default=Path("ai/static-viz-originals"), help="directory to save into")
    parser.add_argument("--width", type=int, default=2550, help="width to request, in px")
    args = parser.parse_args()

    rows = read_sql(
        "SELECT id, filename, cloudflareId, originalWidth, originalHeight, updatedAt FROM images "
        "WHERE filename = %(name)s OR filename LIKE %(like)s",
        params={"name": args.filename, "like": f"%{args.filename}%"},
    )
    exact = rows[rows["filename"] == args.filename]
    if len(exact) == 1:
        rows = exact
    if rows.empty:
        print(f"No image in the grapher `images` table matches {args.filename!r}.", file=sys.stderr)
        return 1
    if len(rows) > 1:
        print(f"{len(rows)} images match {args.filename!r}; pass the exact filename:", file=sys.stderr)
        for name in rows["filename"]:
            print(f"  {name}", file=sys.stderr)
        return 1

    row = rows.iloc[0]
    if not row["cloudflareId"]:
        print(f"{row['filename']} has no cloudflareId, so it is not served from Cloudflare Images.", file=sys.stderr)
        return 1

    width = min(args.width, int(row["originalWidth"]))
    url = IMAGE_URL.format(cloudflare_id=row["cloudflareId"], width=width)
    response = http_session.get(url, timeout=60)
    response.raise_for_status()
    content_type = response.headers.get("content-type", "").split(";")[0]
    extension = EXTENSIONS.get(content_type, Path(row["filename"]).suffix)

    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / (Path(row["filename"]).stem + extension)
    path.write_bytes(response.content)

    updated = datetime.fromtimestamp(int(row["updatedAt"]) / 1000, tz=timezone.utc)
    height = round(width * int(row["originalHeight"]) / int(row["originalWidth"]))
    print(f"saved:       {path} ({content_type}, {width}x{height}, {len(response.content):,} bytes)")
    print(f"source:      {url}")
    print(f"original:    {row['originalWidth']}x{row['originalHeight']}, uploaded {updated:%Y-%m-%d}")
    print(f"Figma layer: original published image — {row['filename']} ({updated:%Y})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
