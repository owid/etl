# Structure of the ETL

## A compute graph

The ETL is a compute graph, that uses a directed acyclic graph (DAG) to describe the dependencies between datasets. There are currently three types of nodes in the graph: snapshots, datasets and grapher exports.

### Snapshots

Data snapshots represent a copy of an upstream datasource in its original format, at a particular point in time.

New-style snapshots are managed in the ETL using DVC and begin with the prefix `snapshot://`. Old-style snapshots are managed by the [walden](https://github.com/owid/walden) codebase and begin with the prefix `walden://`.

Snapshot URIs use the convention: 

```
snapshot://<provider>/<version>/<filename>
```

Examples:

- `snapshot://aviation_safety_network/2022-10-14/aviation_statistics.csv`
- `walden://irena/2022-10-07/renewable_power_generation_costs`

### Datasets

Datasets are nodes in the compute graph, the main units of work in the ETL. They represent a transformation from one or more ingredients into a better, more useful output.

Dataset URIs use the convention:

```
data://<channel>/<provider>/<version>/<dataset>
```

If the dataset is part of a large topic that involves many providers, the name of the topic can be used instead of a provider name.

The dataset URI is used to identify the code that builds the dataset, and also to identify the output file on disk.

Example:

- URI: `data://garden/faostat/2022-05-17/faostat_fa`
- Code: `etl/steps/data/garden/faostat/2022-05-17/faostat_fa.py`
- Dataset prefix: `s3://owid-catalog/garden/faostat/2022-05-17/faostat_fa`

### Grapher views

The Grapher codebase can only except datasets that are in a particulare shape:

```
(entity, year, variable, value)
```

However, datasets in the ETL are often in a different shape. For example, they may be broken down by gender, disease type, fish stock, or some other dimension.

All data charted on our site is built from simplified grapher views, stored in MySQL. 
Grapher steps are responsible for reshaping a dataset on disk into a grapher view, and then uploading it to MySQL. A single variable may fan out into a large number of grapher views.

Grapher views use the following convention:

```
data://grapher/<provider>/<version>/<dataset>
```

??? How do automatic grapher steps get created from here?

## Features and constraints

Much of how the ETL is designed falls out of its design goals.

### No special hardware

To ensure that members of the public can run and audit our code, we have designed the ETL to be a standalone Python program that operates on flat files and fetches what it needs on demand.

It should not need any special hardware or services, and individual ETL steps may use no more than 16GB memory.

It should be possible to run the ETL on MacOS, Linux and Windows (via WSL).

### Public by default

All our data work is public by default; we only use private data sources when it is overwhelmingly in the public interest, or when the data is early-access and will shortly become publicly available.

### Outputs as pure functions of inputs

To ensure our work is reproducible, we take our own snapshots of any upstream data that we use, meaning that if in future the upstream data provider changes their site, their data or their API, we can still build our datasets from "raw ingredients".

```mermaid
graph LR

upstream --> snapshot --> etl --> catalog[on-disk catalog]
```

We secondly keep record all data dependencies in a directed graph or DAG (see: [`dag/main.yml`](https://github.com/owid/etl/blob/master/dag/main.yml)), and forbid steps from using any data as input that isn't explicitly declared as a dependency. This means that the result of any step is a pure function of its inputs.

### Checksums for safe caching

We keep the ETL efficient to build by using a Merkle tree of MD5 checksums:

- Snapshots have a checksum available in their metadata
- Datasets have a checksum of their inputs available in their metadata (the `source_checksum` field)

When we ask the ETL to build something by running `etl <query>`, it will only build things that are out of date. We can force a rebuild by passing the `--force` flag.

### Ready for data science

Previously, although we could chart data, it was very difficult to work with in Jupyter notebooks.

We have designed the ETL so that data is recorded at different stages of processing. The phase called `meadow` is the version closest to the upstream provider, and the version called `garden` is the best and most useful version of the data. We call data in `garden` "ready for data science".