!!! warning "This tutorial might be partial or incomplete. Please check with the team if you have questions."

Before ETL came along, datasets were uploaded directly to our Grapher admin site. If you want to leverage these datasets in the ETL, they won't be available out of the box, since they weren't initially imported via ETL. You have two main options to bring them into ETL:

1. Migrate them into the ETL pipeline.
2. Move them to Fast-Track.


Fast-Track is recommended for users who prefer working on Google sheets rather than the ETL environment. However, it's not as flexible as ETL, and it's not recommended for datasets that require frequent updates.


## How to Migrate to ETL

Automatically generate ETL steps for a specific dataset with the following command. Choose the appropriate namespace, version and short name for the migrated dataset.

```
ENV=.env.prod backport-migrate --dataset-id your_dataset_id --namespace your_namespace --version your_version --short-name your_short_name
```

After running this, follow the terminal prompts.


## How to Migrate to Fast-Track

To move a dataset to Fast-Track, generate a Google spreadsheet using:

```
ENV=.env.prod backport-fasttrack --dataset-id xxx --short-name your_short_name
```

Once generated, open the Fast-Track spreadsheet and follow the provided instructions.


!!! warning "Using automatically backported datasets via `backport://backport/owid/latest/dataset_xxx_...` is discouraged."


## Migrating old grapher charts to Data Pages

If you have an old chart that you want to migrate to Data Pages, you can use the following command to pre-generate YAML file in the grapher channel. Generated YAML files contain all metadata information we have about indicator (including everything from garden step). This means we might end up with duplicate information in garden & grapher YAML files. It's then up to you whether you decide to refactor it and move everything to garden channel.

```
STAGING=my-branch etl-metadata-migrate --chart-slug my-chart-slug
```

After running this, follow the terminal prompts.

!!! info
    This is only a band-aid solution. A proper solution would involve converting the entire pipeline to use origins instead of sources and it would
    define all metadata in garden channel.
