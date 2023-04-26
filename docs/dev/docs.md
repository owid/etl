Our documentation is built using `mkdocs`, which renders markdown files into HTML/CSS files.

!!! info "[Learn more about `mkdocs` :octicons-arrow-right-24:](https://squidfunk.github.io/mkdocs-material/getting-started/)"

The markdown files powering the documentation are in the same repository, under the [`docs/`](https://github.com/owid/etl/tree/master/docs) directory. Along with this file, there is the [`mkdocs.yml`](https://github.com/owid/etl/tree/master/mkdocs.yml) configuration file, which organizes the markdown files hierarchically, sets the site theme, and much more.

!!! warning "Whenever you do substantiall changes to the ETL project, make sure that this is reflected in the documentation"
    That is, whenever you are working on a project and you create a pull request, make sure that the documentation still makes sense with your changes. If necessary, adapt it in the same PR.


## Updating the documentation
To preview the documentation on your local machine, run

```bash
mkdocs serve
```

and go to localhost:8000.

Now, you can test this by modifying one of the files in `docs/` and see how this is reflected automatically on the local site.

### Adding a new entry
To add a new entry, simply create a markdown file in the `docs/` directory (or a directory that falls under it). Next, if you want this entry to be listed in the navigation bar, you'l need to add a reference to the file in the `mkdocs.yml` file.

### Pull request
Once you are happy with your documentation tweaks, make sure to create a pull request so that others can review your text.
