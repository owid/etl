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
  plus `.png` the first time, and the frame's name plus the refresh's year (`<frame name>-2027.png`)
  when a later refresh finds that taken. The library rejects a duplicate filename, and the old file
  has to stay reachable for anything still pointing at it.
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
| Name | The desktop frame's slug. Lowercase letters and hyphens only (no digits or underscores), and unique. The site saves downloads as `<name>-desktop.png` and `<name>-mobile.png`, so the name itself carries no `-desktop` or `-mobile`. |
| Desktop image | The desktop PNG. |
| Mobile image | The mobile PNG, when the viz has a mobile frame. |
| Description | Markdown, shown publicly under "How this visualization was made". Say where the data comes from, linking each product; what is plotted, and any quantity derived from it; the processing a reader can see on the chart (a join between two products, interpolation, values shown selectively); and how it was made, linking the step on GitHub `master` and saying the design was finished in Figma. Follow `.claude/rules/plain-writing.md`. |
| Data source | A Grapher slug when a Grapher chart shows the same data: the modal then offers that chart's CSV and links to it. Otherwise one URL, the producer's own page for the dataset; link any further sources in the description. Our catalog publishes garden tables as feather only, which is no use to a reader. |

## 3. Swap the block in every gdoc that shows the old image

List them, with the links to open each one, by running `/find-chart-references` on the old file:

```bash
STAGING=<any live staging server> .venv/bin/python \
  .claude/skills/find-chart-references/scripts/find_references.py \
  --image-filenames <old filename> --host https://ourworldindata.org \
  --markdown ai/<slug>_image_references.md
```

A staging server's database is a recent copy of production's. `--host` points the page links at the
live site; the admin preview links still open that staging server, so hand on the Google Doc and
page links.
Give the user the Google Doc and published-page links for every article and topic page. For a
fragment, say which FAQ holds the image and whether a published data page shows it. In each article
and topic page, replace

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

keeping any `size`, `hasOutline` and `caption` the old block set. Omitted, `size` is `wide` and the
outline is drawn, the same as an `{.image}` block. A fragment (a data page's FAQ or text, such as
`gcp-faqs`) keeps an `{.image}` block, repointed to the new filename with the mobile file as its
`smallFilename`, until a `{.static-viz}` block is confirmed to render there. An article needs its
author's agreement before the swap (Step 9, item 3).

Then record the static viz's name in the step's docstring, beside the frames' slugs, so the next
refresh updates the same entry rather than creating a second one.
