Check all `.meta.yml` files in `etl/steps/data/garden/` for common spelling typos.

Focus on common spelling errors in metadata fields like:
- "desciption" instead of "description"
- "titile" or "titel" instead of "title"
- "soruce" instead of "source"
- "indictor" instead of "indicator"
- "varialbe" instead of "variable"
- "procesing" instead of "processing"
- "emmissions" instead of "emissions"
- "temperture" instead of "temperature"
- "populaton" instead of "population"
- "governemnt" or "goverment" instead of "government"
- "recieve" instead of "receive"
- "occured" instead of "occurred"
- "seperately" instead of "separately"
- "definately" instead of "definitely"
- "accomodate" instead of "accommodate"
- "exculding" instead of "excluding"
- "comparision" instead of "comparison"
- "wheter" instead of "whether"
- Other common English typos

**Search Strategy:**
1. Use grep or efficient search methods across all `.meta.yml` files
2. Exclude legitimate variable names with numbers (like "classif1", "DTPCV1", etc.)
3. Focus on text in description fields, titles, units, and human-readable content
4. Ignore technical identifiers and code

**Report Format:**
For each typo found, provide:
- File path (relative to etl root)
- Line number
- Field name where typo was found
- The incorrect text with typo highlighted
- Suggested correction

After identifying typos, ask the user if they want you to fix them automatically.
