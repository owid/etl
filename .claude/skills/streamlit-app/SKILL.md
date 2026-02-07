---
name: streamlit-app
description: Create or modify Streamlit apps in the Wizard. Use when building new wizard apps, adding Streamlit pages, or working with apps/wizard/ code.
---

# Streamlit Apps in Wizard

All apps live in `apps/wizard/` as part of the multi-page Wizard application.

## Running

```bash
make wizard
.venv/bin/etlwiz your-app  # Direct to app by alias
```

## Creating a New App

1. Create `apps/wizard/app_pages/your_app/app.py`:

```python
import streamlit as st

st.set_page_config(
    page_title="Wizard: Your App",
    page_icon="🪄",
    layout="wide",
)

def main():
    st.title("Your App")

main()
```

2. Register in `apps/wizard/config/config.yml`:

```yaml
sections:
  - title: "Section Name"
    apps:
      - title: "Your App"
        alias: your-app
        entrypoint: app_pages/your_app/app.py
        description: "What it does"
        maintainer: "@slack-handle"
        icon: ":material/icon:"
        image_url: "https://..."
```

## Key Patterns

### Session State
```python
st.session_state.data = st.session_state.get("data", default)
```

### Caching
```python
from apps.wizard.utils.components import st_cache_data

@st_cache_data(custom_text="Loading...")
def load_data():
    return expensive_operation()
```

### URL Persistence
```python
from apps.wizard.utils.components import url_persist

url_persist(st.selectbox)(label="Option", options=["a", "b"], key="my_key")
```

### Data Loading
```python
from apps.wizard.utils.cached import load_variables_in_dataset, load_variable_data

indicators = load_variables_in_dataset(dataset_uri=["data://garden/ns/ver/ds"])
data = load_variable_data(variable_id=123)
```

## Custom Components

From `apps.wizard.utils.components`:

- `st_horizontal()` - Horizontal layout context manager
- `Pagination` - Paginated lists
- `grapher_chart()` - Render OWID charts
- `st_wizard_page_link()` - Link to other wizard pages
- `st_toast_success()` / `st_toast_error()` - Toast notifications

## Rules

- `st.set_page_config()` must be first Streamlit command
- Use `@st.cache_data` for expensive operations
- Use `url_persist()` for shareable widget state
