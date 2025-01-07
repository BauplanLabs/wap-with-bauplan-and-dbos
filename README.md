# Write-Audit-Publish on the lakehouse with Bauplan and DBOS
A reference implementation of the write-audit-publish pattern with Bauplan and DBOS

## Overview

A common need on S3-backed analytics systems (e.g. a data lakehouse) is safely ingesting new data into tables available to downstream consumers. 

![WAP](img/wap.jpg)

Data engineering best practices suggest the Write-Audit-Publish (WAP) pattern, which consists of three main logical steps:

* Write: ingest data into a ''staging'' / ''temporary'' section of the lakehouse (a [data branch](https://docs.bauplanlabs.com/en/latest/tutorial/02_catalog.html)) - the data is not visible yet to downstream consumers;
* Audit: run quality checks on the data, to verify integrity and quality (avoid the ''garbage in, garbage out'' problem);
* Publish: if the quality checks succeed, proceed to publish the data to the production branch - the data is now visible to downstream consumers; otherwise, raise an error and perform some clean-up operation.

This repository showcases how [DBOS](https://www.dbos.dev) and [Bauplan](https://www.bauplanlabs.com/) can be used to implement WAP in ~150 lines of no-nonsense pure Python code: no knowledge of the JVM, SQL or Iceberg is required.

If you are impatient and want to see the project in action, this is us [running the code from our laptop](https://www.loom.com/share/f8c2e7e3b57d4e2286be02965931bb51?sid=30e576da-9240-4569-b64a-9de5350d82ee).

### What happens under the hood?

While the workflow looks and feels like a simple, no-nonsense Python script, a lot of magic happens behind the scenes _in the cloud_, over _object storage_. In particular, the WAP logic maps exactly to Bauplan operations over the datalake:

* create a data branch, a zero-copy sandbox of the entire data lake in which to perform the ingestion safely;
* create an Iceberg table inside this ingestion branch, loading the files in S3 into it;
* retrieve a selected column from the Iceberg table to make sure there are no nulls (quality check);
* merge the data branch into the production branch (on success), and clean-up the data branch before exiting.

What to the developer looks like a function call (wrapped by DBOS for durable execution) is actually a complex sequence of infrastructure and cloud operations performed by Bauplan for you: you do not need to know anything about Iceberg specs, data branches, columnar querying, but just focus on the business logic.

## Setup

### Bauplan

[Bauplan](https://www.bauplanlabs.com/) is the programmable lakehouse: you can load, transform, query data all from your code (CLI or Python). You can start by reading our [docs](https://docs.bauplanlabs.com/), dive deep into the underlying [architecture](https://arxiv.org/pdf/2410.17465), or explore how the API simplifies [advanced use cases](https://arxiv.org/pdf/2404.13682).

To use Bauplan, you need an API key for our demo environment: you can request one [here](https://www.bauplanlabs.com/#join). Run the 3 minutes [quick start](https://docs.bauplanlabs.com/en/latest/tutorial/01_quick_start.html) to get familiar with the platform first.

Note: the current SDK version is `0.0.3a292` but it is subject to change as the platform evolves - ping us if you need help with any of the APIs used in this project.

### Setup your S3 bucket

To run a Write-Audit-Publish flow you need some files to write first! 

When using the Bauplan demo environment, any parquet or CSV file in a publicly readable bucket will do: just load your (non-sensitive!) file(s) in a S3 bucket and [set the appropriate permissions](https://docs.bauplanlabs.com/en/latest/tutorial/02_catalog.html). 

Note: our example video demo below is based on the [Yellow Trip Dataset](https://data.cityofnewyork.us/Transportation/2021-Yellow-Taxi-Trip-Data/m6nq-qud6/about_data) - adjust the quality check function accordingly if you use a different dataset.

### Setup your Python environment and get started with DBOS

Install the required dependencies in a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Run the [local DBOS setup](https://docs.dbos.dev/quickstart#run-your-app-locally) to get started - i.e. install the CLI tool and setup the database with one of the recommended methods.

Make sure to add the proper environmental variables and runtime configuration to your `dbos-config.yaml` file, e.g.:

```yaml
name: wap_with_bauplan
language: python
runtimeConfig:
  start:
    - "python3 wap_flow.py"
env:
  TABLE_NAME: 'yellow_trips'
  BRANCH_NAME: 'jacopo.dbos_ingestion'
  S3_PATH: 's3://mybucket/yellow_tripdata_2024-01.parquet'
  NAMESPACE: 'dbos'
```

Remember to run _migrate_ on the database when you first set up the project: 

```bash
dbos migrate
```

## Run the workflow

You can run the workflow with DBOS through the CLI: 

```bash
dbos start
```

If you want to see the end result, you can watch this [video demonstration](https://www.loom.com/share/f8c2e7e3b57d4e2286be02965931bb51?sid=30e576da-9240-4569-b64a-9de5350d82ee) of the flow in action, both in case of successful audit and in case of failure.

## License

The code in the project is licensed under the MIT License (DBOS and Bauplan are owned by their respective owners and have their own licenses). 