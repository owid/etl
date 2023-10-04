!!! warning "This tutorial might be partial or incomplete. Please check with the team if you have questions."

Before ETL came along, datasets were uploaded directly to our Grapher admin site. If you want to leverage these datasets in the ETL, they won't be available out of the box, since they weren't initially imported via ETL. You have two main options to bring them into ETL:

1. Migrate them into the ETL pipeline.
2. Move them to Fast-Track.

Choose Fast-Track if the dataset requires author edits. Otherwise, it's more straightforward to migrate to ETL.


## How to Migrate to ETL

Automatically generate ETL steps for a specific dataset (xxx) with the following command:

```
ENV=.env.prod backport-migrate --dataset-id xxx --namespace your_namespace --version your_version --short-name your_short_name
```

After running this, follow the terminal prompts.


## How to Migrate to Fast-Track

To move a dataset to Fast-Track, generate a Google spreadsheet using:

```
ENV=.env.prod backport-fasttrack --dataset-id xxx --short-name your_short_name
```

Once generated, open the Fast-Track spreadsheet and follow the provided instructions.


!!! warning "Using automatically backported datasets via `backport://backport/owid/latest/dataset_xxx_...` is discouraged."
