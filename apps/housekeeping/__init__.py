"""Assist with housekeeping tasks.

The initial motivation for this was to help with the problem of chart maintenance:

"A growing problem we have at OWID is that our database contains a very high number of charts, and this number keeps growing month by month. Many charts are good and worth keeping, but several hundred at least are charts that aren't maintained, updated, and generally up to our current standards.

These charts get few views but still "clog" our internal admin and search results (on OWID and search engines). Overall, these charts take mental space that we could instead allocate to maintaining our most important charts."

TODOs:

1) However, housekeeping can be applied to other areas of the codebase, such as datasets, tables, and other objects.

2) Smarter track of "reviewed" objects. An option could be a structured table "suggested_reviews": id, type, date. Where
type can be 'chart', 'dataset', etc.
"""
