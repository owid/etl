# Publishing a static viz on the site

Read this at Step 9, item 7, once the Figma frames are final. A flagship static viz goes on the site as
a **static viz** (Admin → Static viz, `/admin/static-viz/`), not as a bare image. The `{.static-viz}`
gdoc block renders the image like `{.image}` does, and adds a Download button whose modal shows the
description, both image versions and the data link. The admin is behind Cloudflare Access, so a cloud
session cannot reach it: prepare the files and the texts, and the user enters them.

## 1. Export and upload the images

- **Export each frame at its family's scale** (`/owid-staff:create-figma-chart` →
  `reference/NODE-MAP.md` → *Export scale per family*: 2× for the 850-wide desktop frames, 3× for
  the 540-wide mobile ones). From a session, clone the frame, `rescale` the clone, `get_screenshot` it
  at its natural size, and delete the clone. Save the files under the user's `ai/` folder.
- **Upload them in Admin → Images under a filename the library does not hold yet**: each frame's name
  plus `.png` the first time, and the frame's name plus the refresh's date
  (`<frame name>-2027-03-14.png`) when a later refresh finds that taken; if that is taken too, add
  `-2`, `-3` until the upload goes through. The library rejects a duplicate filename, and the old
  file has to stay reachable for anything still pointing at it.
- **Write each image's alt text** (its default alt): what the chart shows, how to read it, and one or
  two values read off the data, not off the render.

## 2. Create the static viz, or update the existing one

**If the step's docstring already records a static viz name, the viz is published: edit that entry**
(Admin → Static viz → its name) rather than creating one. Keep its name, so every `{.static-viz}`
block already pointing at it picks up the change. Set the new images, and revise the description and
data source if the refresh changed them. Then skip section 3, apart from any page still showing an
`{.image}` block. Otherwise create the entry:

| Field | What goes in |
|---|---|
| Name | The desktop frame's slug, made valid: lowercase letters and single hyphens only, so drop any digits with the hyphen next to them (a year or a count: `poverty-2025-update` → `poverty-update`) and turn underscores into hyphens. It must be unique: if another static viz holds it, add a word that tells the two apart. The site saves downloads as `<name>-desktop.png` and `<name>-mobile.png`, so the name itself carries no `-desktop` or `-mobile`. |
| Desktop image | The desktop PNG. |
| Mobile image | The mobile PNG, when the viz has a mobile frame. |
| Description | Markdown, shown publicly under "How this visualization was made". Say where the data comes from, linking each product; what is plotted, and any quantity derived from it; the processing a reader can see on the chart (a join between two products, interpolation, values shown selectively); and how it was made, linking the step on GitHub `master` and saying the design was finished in Figma. Follow `.claude/rules/plain-writing.md`. |
| Data source | A Grapher slug when a Grapher chart shows the same data: the modal then offers that chart's CSV and links to it. Otherwise one URL, the producer's own page for the dataset; link any further sources in the description. Our catalog publishes garden tables as feather only, which is no use to a reader. |

## 3. Swap the block in every gdoc that shows the old image

The same viz is often in the library under more than one name: an older version, a copy re-uploaded
as `-v2` or `-new`, a mobile file. The static viz report (the artifact Step 9 links) merges those into
one row, and its filenames for the viz (`all_filenames` in the report's CSV) are the ones to search.
List the pages, with the links to open each one, by running `/find-chart-references` on all of them:

```bash
STAGING=<any live staging server> .venv/bin/python \
  .claude/skills/find-chart-references/scripts/find_references.py \
  --image-filenames <every filename of the viz, comma-separated> --host https://ourworldindata.org \
  --markdown ai/<slug>_image_references.md
```

A staging server's database is a recent copy of production's. `--host` points the page links at the
live site; the admin preview links still open that staging server, so hand on the Google Doc and
page links. A filename the library does not hold is reported as a coverage gap, never as unused.
Give the user the Google Doc and published-page links for every article and topic page. A row whose
context says `featured image` holds the image in the gdoc's `featured-image:` front matter, not in a
body block: point that field at the new upload, which for a thumbnail-sized slot is the viz's own
thumbnail export rather than the chart PNG, and leave the body alone. For a
fragment, say which FAQ holds the image and whether a published data page shows it. For every
`image block` row in an article or topic page, replace

```archie
{.image}
filename: <old filename>
{}
```

with

```archie
{.static-viz}
name: <static viz name>
{}
```

keeping any `size`, `hasOutline` and `caption` the old block set. A `static-viz block` row already
shows the static viz built on the image and picks up its new images by name: it needs no swap, only
the text check below. Omitted, `size` is `wide` and the
outline is drawn, the same as an `{.image}` block. A fragment (a data page's FAQ or text, such as
`gcp-faqs`) keeps an `{.image}` block, repointed to the new filename with the mobile file as its
`smallFilename`, until a `{.static-viz}` block is confirmed to render there. An article needs its
author's agreement before the swap (Step 9, item 3).

Then record the static viz's name in the step's docstring, beside the frames' slugs, so the next
refresh updates the same entry rather than creating a second one.

## 4. Check the text around it

The paragraphs beside the image describe the old chart, and the swap puts the new one under them. Read
them against the new frames before the swap goes live. For each gdoc that shows the image, or a
`{.static-viz}` block of the static viz built on it,
`scripts/image_context.py <every filename of the viz>` prints its title, its Google Doc and page links,
and the blocks just before and after each placement. Check every sentence that describes the chart:

- **What it says the chart shows:** its subject, its range, and any word the new title or subtitle
  dropped on purpose ("healthy", "global"). The step's docstring records why each was dropped.
- **The marks it names** ("the thick line", "the ribbons", "the dotted line"): does the new frame
  still draw them, and under the same name?
- **Every number or comparison read off the chart:** recompute it from the step's data.
- **The sources it credits**, against the viz's data source.

As in `/update-dataset`'s referencing-prose audit, never edit published prose yourself, since it
belongs to its author. Report each finding with its page and Doc links, the current sentence and a
proposed one, and repeat the unsettled ones in every reply until the user decides. The text and the
image go live together, because a new image under an uncorrected description reads as an error. A
fragment that no published data page shows is low priority, but its text gets the same read when it
is repointed.
