class Channels:
    """Channels for metadata files.

    Using this to avoid hardcoding strings."""

    SNAPSHOT = "snapshot"
    GRAPHER = "grapher"
    GARDEN = "garden"


# Additional instructions or configurations
ADDITIONAL_INSTRUCTIONS = """
Metadata Field Guidelines:

1. attribution (string, optional)
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Ends with year of date_published in parenthesis.
   - No semicolons.

2. attribution_short (string, recommended)
   - Use if producer name is long; shorten the producer name in an informative way.
   - Capital letter start, except names like 'van Haasteren'.
   - No ending period.
   - Refers to producer or well-known data product, not year.
   - Use acronym if well-known.

3. citation_full (string, required)
   - Capital letter start.
   - Ends with period.
   - Includes year of publication.
   - Match producer's format with minor edits.
   - List multiple sources for compilations.

4. date_accessed (string, required)
   - Format: YYYY-MM-DD.
   - Reflects access date of current version.

5. date_published (string, required)
   - Format: YYYY-MM-DD or YYYY.
   - Reflects publication date of current version.

6. description (string, recommended)
   - Capital letter start and period end.
   - Avoid other metadata fields unless crucial.
   - Succinct description of data product.

7. description_snapshot (string, recommended)
   - Capital letter start and period end.
   - Define if data product and snapshot differ.
   - Describe snapshot specifically.

8. license (string, required)
   - Standard license names or producer's custom text.
   - CC BY 4.0 if unspecified, pending confirmation.

9. license.url (string, required if existing)
   - Complete URL to license on producer's site.
   - Avoid generic license pages.

10. producer (string, required)
    - Capital letter start, except names like 'van Haasteren'.
    - No ending period, except 'et al.'.
    - Exclude dates, semicolons, OWID references.

11. title (string, required)
    - Capital letter start.
    - No ending period.
    - Identify data product, not snapshot.

12. title_snapshot (string, required if different)
    - Capital letter start.
    - No ending period.
    - Use if snapshot differs from data product.

13. url_download (string, required if existing)
    - Direct download URL or S3 URI.

14. url_main (string, required)
    - URL to data product's main site.

15. version_producer (string or number, recommended if existing)
    - Use producer's version naming.
"""
