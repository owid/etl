---
tags:
    - 👷 Staff
icon: lucide/book-open-text
---
# Documentation


Our documentation is built using [:octicons-link-external-16: Zensical](https://zensical.org), which renders markdown files into HTML/CSS files.

The markdown files powering the documentation are in the same repository, under the [:fontawesome-brands-github: `docs/`](https://github.com/owid/etl/tree/master/docs) directory. Along with this file, there is the [:fontawesome-brands-github: `zensical.toml`](https://github.com/owid/etl/tree/master/zensical.toml) configuration file, which organizes the markdown files hierarchically, sets the site theme, and much more.

!!! note "Reflect your changes in the documentation!"

    Whenever you are working on a project and you create a pull request, make sure that the documentation still makes sense with your changes. If necessary, please adapt the documentation it in the same PR.



## What can Zensical do?
Zensical is a static site generator that converts markdown files into a website. It offers several features that are useful for our documentation: code blocks, syntax highlighting, automatic table of contents generation, search functionality, navigation, and much more. You can learn more about its features in [their official documentation](https://zensical.org/docs/get-started/).


In the past, we relied on [:octicons-link-external-16: Material for MkDocs](https://squidfunk.github.io/mkdocs-material/) to build our documentation. However, we are transitioning to Zensical due to its better support for modern web standards, improved performance, and enhanced customization options.

!!! tip "Before you start, make sure you are familiar with markdown and [Zensical](https://zensical.org/docs/get-started/)."


## Step-by-step guide to modify the documentation


First, make sure that you have all the development libraries installed. To preview the documentation on your local machine, run `make docs.serve` and go to [localhost:8000](http://localhost:8000).

Any change you make to markdown files in  `docs/` will be reflected live on your local documentation site.


!!! info "Modifying notebooks"

    If you are modifying a notebook, you will need to build the documentation from scratch with `make docs.build` before serving it with `make docs.serve`. This might take an e xtra 10 seconds.


### Adding a new entry
To add a new entry, simply create a markdown file in the `docs/` directory (or a directory that falls under it). Next, if you want this entry to be listed in the navigation bar, you'll need to add a reference to the file in the `zensical.toml` file.

### Update the metadata reference
All the reference documentation is generated using the schema files (see files under the `schemas` directory). If you want to tweak the description, examples or other fields in the reference, please take a look at [:fontawesome-brands-github: this guideline](https://github.com/owid/etl/issues/1566#issue-1875783217).

### Pull request
Once you are happy with your documentation tweaks, make sure to create a pull request so that others can review your text.

## Adding a technical publication
We are adding [technical reports](../../analyses/#technical-publications){data-preview} to our documentation to better support our data work. These provide in-depth explanations of methodologies, data sources, and analyses that underpin our datasets and visualizations.

### Step-by-step guide
Add your report in the `docs/analyses` directory, as a folder with all the required files.

Exploit the multiple features that Zensical provides to make your report more engaging, such as code blocks, images, tables, and more! You can also embed interactive charts with Plotly.

Finally, make sure to link your report in the documentation navigation by adding it to the `zensical.toml` file.

### Previewing your work
Run `make docs.serve` and go to [localhost:8000](http://localhost:8000) to preview your report locally.

If you are adding notebooks, you will need to run `make docs.build` before serving it with `make docs.serve`. This is because notebooks need to be converted to markdown before being served.


!!! danger "Links to notebooks in the docs"

    If you want to link to a notebook in the documentation, please replace its extension from `.ipynb` to `.html`. This is because we convert the notebook to an HTML during build time so that it can be previewed.

    Leaving a reference with `.ipynb` will cause the notebook to be downloaded on click.

