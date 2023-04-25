
!!! quote "In computing, extract, transform, load (ETL) is a three-phase process where data is extracted, transformed (cleaned, sanitized, scrubbed) and loaded into an output data container ([source](https://en.wikipedia.org/wiki/Extract,_transform,_load))."

We have implemented our own ETL system, which allows us to import data from a wide variety of sources to our catalog.

At a high level, our ETL does the following tasks:


- Captures the data from the source as a snapshot at a particular point in time. This mitigates potential issues trying to access the source site.
- Cleans and brings the data into a common format.
- Describes the data as best as we can for the public using metadata.

In this section, we explore the ETL as a [computational graph](compute-graph), its [different phases](phases.md) and the [data model](common-format.md) that we have built around it. In addition, we discuss some of its key [features and constraints](features-constraints.md).
