# Jupyter Notebook Integration with Zensical

This directory contains Jupyter notebooks that are automatically converted to HTML for documentation.

## How It Works

1. **Conversion Script**: `docs/ignore/convert_notebooks.py` - Automatically finds and converts all `.ipynb` files from `docs/` to `site/`
2. **Build Integration**: The conversion runs AFTER Zensical builds the docs in `make docs.build`
3. **Template**: Uses nbconvert's "classic" template for clean, standard Jupyter notebook styling

## Usage

### Automatic Conversion (Recommended)

When building documentation, notebooks are automatically converted:

```bash
make docs.build
```

This will:
1. Clean previous builds
2. Generate dynamic documentation files
3. Build documentation with Zensical
4. **Convert all Jupyter notebooks to HTML in the `site/` directory** (NEW)

### Manual Conversion

You can also convert notebooks manually (after building docs):

```bash
# Convert all notebooks (requires site/ directory to exist)
.venv/bin/python docs/ignore/convert_notebooks.py

# Convert with verbose output
.venv/bin/python docs/ignore/convert_notebooks.py --verbose

# Specify different directories
.venv/bin/python docs/ignore/convert_notebooks.py --docs-dir path/to/docs --output-dir path/to/output
```

**Important**: The script requires the output directory (default: `site/`) to exist, so run it after `make docs.build`.

## Output Location

- **Source**: Notebooks in `docs/` directory (e.g., `docs/analyses/media_deaths/media_deaths_analysis.ipynb`)
- **Output**: HTML files in `site/` directory (e.g., `site/analyses/media_deaths/media_deaths_analysis.html`)
- **No HTML in docs/**: HTML files are NOT stored in the `docs/` directory, only in `site/`

## Template

Uses nbconvert's "classic" template which provides:
- Clean, traditional Jupyter notebook styling
- Standard code cell formatting with In/Out prompts
- Syntax highlighting for code
- Proper rendering of markdown, tables, and images

## Viewing Notebooks

After running `make docs.build` and starting the docs server with `make docs.serve`, notebooks can be accessed at:

```
http://localhost:8000/projects/etl/[path-to-notebook].html
```

For example:
- `http://localhost:8000/projects/etl/analyses/media_deaths/media_deaths_analysis.html`
- `http://localhost:8000/projects/etl/api/python.html`

## Adding New Notebooks

Simply add your `.ipynb` file anywhere in the `docs/` directory. The next time you run `make docs.build`, it will be automatically converted to HTML in the `site/` directory.

## Customizing the Template

The script uses nbconvert's "classic" template. To change the template or add custom styling:

1. Modify the `template_name` parameter in `docs/ignore/convert_notebooks.py`
2. Or add custom CSS to `docs/css/extra.css` that targets Jupyter HTML classes

## Troubleshooting

### Notebooks not converting

Check that:
1. The notebook file has `.ipynb` extension
2. It's not in a `.ipynb_checkpoints` directory
3. The notebook is valid (can be opened in Jupyter)

### Styling issues

If the styling doesn't look right:
1. Check the template file exists: `docs/jupyter_template.html.j2`
2. Verify CSS custom properties match your Zensical theme
3. Inspect the generated HTML for CSS conflicts

### Script errors

Run with `--verbose` flag to see detailed error messages:

```bash
.venv/bin/python scripts/convert_notebooks.py --verbose
```
