import mkdocs_gen_files

from etl.docs import render_dataset, render_indicator, render_origin, render_table

tags = """---
tags:
  - Metadata
---

"""

# Origin reference
with mkdocs_gen_files.open("architecture/metadata/reference/origin.md", "w") as f:
    text_origin = tags + render_origin()
    print(text_origin, file=f)

# Dataset reference
with mkdocs_gen_files.open("architecture/metadata/reference/dataset.md", "w") as f:
    text_dataset = tags + render_dataset()
    print(text_dataset, file=f)

# Tables reference
with mkdocs_gen_files.open("architecture/metadata/reference/tables.md", "w") as f:
    text_table = tags + render_table()
    print(text_table, file=f)

# Indicator reference
with mkdocs_gen_files.open("architecture/metadata/reference/indicator.md", "w") as f:
    text_indicator = tags + render_indicator()
    print(text_indicator, file=f)
