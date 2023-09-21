Our documentation is built using `mkdocs`, which renders markdown files into HTML/CSS files.

!!! info "[Learn more about `mkdocs` :octicons-arrow-right-24:](https://squidfunk.github.io/mkdocs-material/getting-started/)"

The markdown files powering the documentation are in the same repository, under the [`docs/`](https://github.com/owid/etl/tree/master/docs) directory. Along with this file, there is the [`mkdocs.yml`](https://github.com/owid/etl/tree/master/mkdocs.yml) configuration file, which organizes the markdown files hierarchically, sets the site theme, and much more.

!!! info "Whenever you do substantial changes to the ETL project, make sure that this is reflected in the documentation"
    That is, whenever you are working on a project and you create a pull request, make sure that the documentation still makes sense with your changes. If necessary, adapt it in the same PR.


## Updating the documentation
First, make sure that you have all the development libraries installed

```bash
poetry install --with dev
```

### Preview the documentation
Next, to preview the documentation on your local machine, run

```bash
mkdocs serve
```

and go to [localhost:8000](http://localhost:8000).

Now, you can test this by modifying one of the files in `docs/` and see how this is reflected automatically on the local site.

### Adding a new entry
To add a new entry, simply create a markdown file in the `docs/` directory (or a directory that falls under it). Next, if you want this entry to be listed in the navigation bar, you'll need to add a reference to the file in the `mkdocs.yml` file.

### Update the metadata reference
All the reference documentation is generated using the schema files (see files under the `schemas` directory). If you want to tweak the description, examples or other fields in the reference, please take a look at [this guideline](https://github.com/owid/etl/issues/1566#issue-1875783217).

### Pull request
Once you are happy with your documentation tweaks, make sure to create a pull request so that others can review your text.
