"""Generate documentation files dynamically"""

import mkdocs_gen_files

from etl.docs import render_dataset, render_indicator, render_origin, render_table
from etl.paths import BASE_DIR, LIB_DIR

header_metadata = """---
tags:
  - Metadata
---

# Metadata reference

<div class="grid cards" markdown>

- __[Indicator](#variable)__ (variable)
- __[Origin](#origin)__
- __[Table](#table)__
- __[Dataset](#dataset)__
</div>

"""

############################################################
# METADATA
############################################################

# Combined reference (BEING used now)
with mkdocs_gen_files.open("architecture/metadata/reference/index.md", "w") as f:
    text_origin = render_origin(level=2)
    text_dataset = render_dataset(level=2)
    text_table = render_table(level=2)
    text_indicator = render_indicator(level=2)
    text = header_metadata + text_indicator + text_origin + text_table + text_dataset
    print(text, file=f)

# # Origin reference
# with mkdocs_gen_files.open("architecture/metadata/reference/origin.md", "w") as f:
#     text_origin = header_metadata + render_origin()
#     print(text_origin, file=f)

# # Dataset reference
# with mkdocs_gen_files.open("architecture/metadata/reference/dataset.md", "w") as f:
#     text_dataset = header_metadata + render_dataset()
#     print(text_dataset, file=f)

# # Tables reference
# with mkdocs_gen_files.open("architecture/metadata/reference/tables.md", "w") as f:
#     text_table = header_metadata + render_table()
#     print(text_table, file=f)

# # Indicator reference
# with mkdocs_gen_files.open("architecture/metadata/reference/indicator.md", "w") as f:
#     text_indicator = header_metadata + render_indicator()
#     print(text_indicator, file=f)


############################################################
# owid-catalog
############################################################
# Load index.md and concatenate with catalog README.md
with open(BASE_DIR / "docs/api/index.md", "r") as f2:
    docs_api = f2.readlines()

with open(LIB_DIR / "catalog/README.md", "r") as f2:
    docs_catalog = f2.readlines()

docs_catalog1 = "    ".join(docs_catalog)
docs_catalog = "".join(docs_catalog)
docs_api = "".join(docs_api)

docs_api = ""
docs = """
{docs_api}


{docs_catalog}
""".format(docs_catalog=f"    {docs_catalog1}", docs_api=docs_api)

# docs = """
# {docs_catalog}
# """.format(docs_catalog=f"    {docs_catalog}")

# Dynamically create the API documentation
with mkdocs_gen_files.open("api/example-usage.md", "w") as f:
    print(docs_catalog, file=f)
