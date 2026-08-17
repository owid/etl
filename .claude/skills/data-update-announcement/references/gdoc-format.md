# The Google Doc: format, styling, and traps

The published artifact is a **Google Doc** in the team's `/Data updates` Drive folder (`1oL0uLHKI6f2qi1rJA6-qFFRYEBw_-rfm`), which OWID's CMS ingests into the `/latest` feed. This file covers producing it. For what to *write*, see `../references/examples.md` and the SKILL.

Create the Doc **only after the author has picked one of the two drafts.** The Drive MCP has no edit-content tool and no delete tool, so a Doc created early — or created wrong — is an orphan the user has to delete by hand. Get it right on the first `create_file`, and don't recreate over a trivial issue; tell the user the one-line fix instead.

## Doc title

`YYYY-MM Data update: [short data source]` — e.g. `2026-06 Data update: UN IGME`, `2026-06 Data update: Homicides`.

Name the **source**, not the topic: one source often spans several topics, and the data scientist knows what theirs covers.

## The CMS format

```
:skip
preview
:endskip

title: …
excerpt: …
type: announcement
authors: …
kicker: data-update

[+body]

Body paragraph.

Another body paragraph.

{.cta}
url: …
text: …
{}

{.image}
filename: YYYY-MM-data-update-<slug>.png
{}

[]
```

- `excerpt` is the social-media preview text — that's what the template Doc's own placeholder says it's for.
- **The five frontmatter fields above are the whole set, and `filename:` always uses the date pattern.** One post in `examples.md` (the static-viz refresh) adds a `featured-image:` line and names its image `world_population_growth.png`; neither is the house pattern, so don't reproduce them.
- `[+body]` and `[]` are **plain brackets** — never add backslash escapes. Backslashes showing up in a `read_file_content` dump are an artifact of that markdown rendering, not the real Doc content.
- The top `:skip` / `preview` / `:endskip` block is **optional**. The team's own template Doc does not have one, but some published docs carry it to park the admin GDoc preview URL. Include it only if the author asks; if you do, keep the literal word `preview` as the placeholder they replace once the doc is registered.
- A `:skip` … `:endskip` block *after* `[]` is where a human parks paragraphs they cut. Don't generate one; mention it only if asked.
- Frontmatter: one field per line, no blank lines between fields.
- Body: blank line between paragraphs.
- `{.cta}` / `{.image}`: opening tag, fields, closing tag on consecutive lines with no blank lines inside. One blank line between the body and `{.cta}`, and between `{.cta}` and `{.image}`.

The finished Doc is **compact** — every line is its own paragraph, with no empty spacer paragraphs. Google Docs makes a paragraph per Enter, so blank lines in the upload become empty paragraphs the user has to delete.

The block above, not the SKILL's spaced "Template" section or the posts in `examples.md`, is what the upload has to reproduce. Those two space the fields out for reading; copying that spacing into the HTML is what produces the spacer paragraphs.

## Styling

The OWID GDocs Add-on (Extensions menu in the Doc) is the team's canonical formatter, and it can't be driven through the API. You can reproduce its output directly on `create_file` by uploading `contentMimeType: "text/html"` with inline styles — verified: Google Docs' HTML import preserves colors, `margin-left` indents, and hyperlinks.

**Read the current scheme rather than trusting the table below.** Before building the upload, export the team's template Doc and read its live values:

```
download_file_content(fileId="1BbdcV2xhYqpC1DScRyxLFyQQ65_-elavaMx9UKv28SA", exportMimeType="text/html")
```

Pull the `color:` and `margin-left:` values off it. This is self-correcting if the team restyles; the table is only the shape to expect and the fallback if the export fails.

Everything is Arial 11pt, `line-height:1.0`, `text-align:left`. Last read off the template Doc **2026-08-17**:

| Element | Text color | Left indent |
|---|---|---|
| Field keys (`title: `, `excerpt: `, `type: `, `authors: `, `kicker: `, `url: `, `text: `, `filename: `) | blue `#0094ff` | — |
| Frontmatter **values** (title/excerpt/type/authors/kicker) | black `#000000` | 0 |
| `[+body]` and `[]` | orange `#f47835` | 0 |
| Body paragraphs, and `url:` / `text:` / `filename:` **values** | grey `#666666` | body 10pt; url/text/filename 20pt |
| `{.cta}`, `{}`, `{.image}` | green `#23974a` | 10pt |
| Inline links | blue `#1155cc`, underlined | — |

Per-line shape: `<p style="margin-left:10pt"><span style="color:#666666">…</span></p>`; links `<a href="…" style="color:#1155cc;text-decoration:underline">…</a>`. The `<p>` also carries `color:#666666` in the template even where the inner span overrides it, so set both if you want a byte-close match.

**Encode all non-ASCII as HTML entities** — `&mdash;` for an em-dash, `&#128073;` for an emoji. Entities decode correctly on import; raw 4-byte characters mojibake to `ð`.

**Verify afterwards** with `download_file_content(exportMimeType="text/html")` and confirm the `color:` and `margin-left:` styles survived.

## Traps

**Dod links do not survive the HTML import.** A fragment href like `#dod:oda` gets rewritten by Google's importer into an internal bookmark id (`#id.xxxxxxxx`), silently breaking the dod. External `https://` links come through fine (wrapped in Google's redirector, which is normal).

After creating the doc, list every dod the body uses in the handoff and ask the user to re-add each by hand — select the anchor text, edit link, paste the literal `#dod:…`. A manual re-add through the Docs UI *does* store the bare fragment, which is what the CMS needs (dod detection matches `url.startsWith("#dod:")`; see `MarkdownTextWrap.tsx` / `SimpleMarkdownText.tsx` in owid-grapher).

Verify the fix from the **exported HTML** (`download_file_content(exportMimeType="text/html")`, grep for `href="#dod:…"`). Do not judge it from `read_file_content` — its markdown rendering resolves the fragment against the doc's own URL and makes a correct link look broken.

**`copy_file` sometimes fails silently or returns a quota error.** Retry once with identical parameters before reporting it. (Same for `create_file`.)

**Drive `parentId` search often returns nothing** for this shared drive. To read existing docs in the folder, fall back to `fullText contains 'datasetProducts'` (matches the search-URL CTA) or `title contains '<recent dataset>'`, then `read_file_content` each.

**If the shared folder rejects the write**, create in My Drive and share the link.

## After the Doc exists

A Google Doc only reaches the `/latest` feed once it's registered at [admin.owid.io/admin/gdocs](https://admin.owid.io/admin/gdocs), where the doc ID is added so it can be previewed and published. That's a human action (admin login required) — the SKILL's admin-reminder block covers it.
