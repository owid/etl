!!! warning "This tutorial might be partial or incomplete. Please check with the team if you have questions."

Once a dataset is available on the Grapher admin, you can create charts from its variables. In some cases, the dataset is a newer version from an already existing one. In such cases, we may need to update some charts that are relying on variables from the old dataset.

To this end, we use `poetry run walkthrough charts`. This tool will guide you through the whole process of creating _chart revisions_.

!!! abstract "Chart revision"
    Charts at Our World in Data are based on a config file, which contains several configuration parameters (variable IDs in use, title, subtitle, etc.). The `walkthrough charts` creates a new configuration, based on the new dataset. This new configuration still needs some human revision. That is, needs revision.

Chart revisions are then submitted to the admin site, and need to be reviewed by the user. Again, the `walkthrough` will guide you through.
