Check `.meta.yml` files for common spelling typos.

**First, ask the user which scope they want to check:**
1. **Current step only** - Check only the `.meta.yml` file(s) in the current working directory/step
2. **All ETL metadata** - Check all `.meta.yml` files in `etl/steps/data/garden/` (approximately 1,262 files)

Once the user specifies the scope, proceed with the typo check.

**Focus on common spelling errors in metadata fields like:**
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
1. Based on user's choice, search either:
   - Current directory: Find `.meta.yml` files in the current working directory
   - All ETL: Search all `.meta.yml` files in `etl/steps/data/garden/`
2. Use grep or efficient search methods to find typos
3. Exclude legitimate variable names with numbers (like "classif1", "DTPCV1", etc.)
4. Focus on text in description fields, titles, units, and human-readable content
5. Ignore technical identifiers and code

**Report Format:**
For each typo found, provide:
- File path (relative to etl root)
- Line number
- Field name where typo was found
- The incorrect text with typo highlighted
- Suggested correction

After identifying typos, ask the user if they want you to fix them automatically.
