"""Generate documentation files dynamically"""
import mkdocs_gen_files

from etl.docs import render_dataset, render_indicator, render_origin, render_table
from etl.paths import LIB_DIR

header_metadata = """---
tags:
  - Metadata
---

# Metadata reference

<div class="grid cards" markdown>

- __[Indicator](#variable)__ (variable)
- __[Origin](#origin)__
- __[Table](#tables)__
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
docs_api = """# Public data API

Our mission is to make research and data on the world's biggest problems accessible and understandable to the public. As part of this work, we provide an experimental API to the datasets.

When using the API, you have access to the public catalog of data processed by our data team. The catalog indexes _tables_ of data, rather than datasets or individual indicators. To learn more, read about our [data model](../architecture/design/common-format.md).

At the moment, we only support Python.


!!! warning "Our API is in beta"

    We currently only provide a python API. Our hope is to extend this to other languages in the future. Please [report any issue](https://github.com/owid/etl) that you may find.

=== "Python"

    (see [example notebook](python.ipynb))

{docs_api_python}

"""
with mkdocs_gen_files.open("api/index.md", "w") as f:
    with open(LIB_DIR / "catalog/README.md", "r") as f2:
        docs = f2.readlines()
    docs = "    ".join(docs)
    docs = docs_api.format(docs_api_python=f"    {docs}")
    print(docs, file=f)
