# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join("../owid/")))

# -- Project information -----------------------------------------------------

project = "owid-datautils"
copyright = "2022, Our World in Data"
author = "Our World in Data"

# The full version, including alpha/beta/rc tags
release = "0.5.3"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "api/modules.rst"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

html_theme_options = {
    "sidebar_hide_name": True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]


html_logo = "_static/owid.png"
html_favicon = "_static/favicon.ico"

autodoc_default_flags = [
    "members",
    "undoc-members",
    "private-members",
    "special-members",
    "inherited-members",
    "show-inheritance",
]

html_context = {
    "display_github": True,  # Integrate GitHub
    "github_user": "owid",  # Username
    "github_repo": "owid-datautils-py",  # Repo name
    "github_version": "main",  # Version
    "conf_py_path": "docs/",  # Path in the checkout to the docs root
}

## API docs
from sphinx.ext.apidoc import main

# poetry run sphinx-apidoc --help
main(["-f", "-e", "-t", "apidoc-templates", "-P", "-o", "api", "../owid/"])
