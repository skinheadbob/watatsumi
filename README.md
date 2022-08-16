# Introduction
Watatsumi is a Data Lake skeleton that runs on top of Delta Lake / (Azure) Databricks. It provides Python APIs to build data pipelines that ingest data from various sources into the Lake. A set of conventions and guidelines is defined (and enforced to certain extent) with regards to pipeline building and maintenance practices. The code base of this project can also be used as training material for data engineers to understand the essential infra of (cloud-based) Data Lake.

The name Watatsumi・ワタツミ・海神・綿津見 refers to the Japanese deity of water (since we are building a data 'lake').

# Infrastructure
<TODO: embed pic (overview) https://drive.google.com/file/d/1Td5PqGp9YpUaolBM0zNR-BE5y6EIOVy0/view?usp=sharing >
* Need a 'cicd' environment to run integration tests
* Need Azure Key Vault to store the credentials
* Each environment gets a dedicated Blob Container for segregation purpose

# Convention
<TODO: embed pic (container) https://drive.google.com/file/d/1Td5PqGp9YpUaolBM0zNR-BE5y6EIOVy0/view?usp=sharing >
Inside each Blob Container, directories are organized following below convention,
```
\
|-landing_zone
  |-[source_name]
    |-[slice_id]
      |-[watermark]
        |- binary files e.g. xxx.csv, yyy.xlsx, etc.
|-bronze_zone
  |-[Delta Table Name] e.g. cu_yourbronzetable
    |- directory _delta_log, partition directories / .snappy.parquet files
|-silve_zone
  |-[Delta Table Name] e.g. ag_yoursilvertable
    ...
|-gold_zone
  |-[Delta Table Name] e.g. au_yourgoldtable
    ...
|-tmp_zone
  ...
|-***_zone
  ...
```

# Slice-based Data Ingestion
When ingesting data into the Lake, we want the ingestion process to be:
  * incremental: suppose the Source holds consumer activities of past 900 days, we want to ingest one day at a time instead of biting off more than we can chew
  * idempotent: when the pipeline job fails, we want to be comfortable to simply re-run the job for transient issues (e.g. network timeout, hardware failure)
  
Ideally the Source should a stream of immutable events but in reality we probably will need to deal with late arriving facts and random in-place data update in the Source. 
The challenge here is that we are chasing after two contradictive goals:
  * accuracy: we don't want to miss the random in-place data update in the Source
  * efficiency: we don't want to re-examinze every existing row in the Source on every incremental ingestion attempt

Watatsumi tries to strike a balance between accuracy and efficiency with this 'slice-based ingestion' approach: 
  * suppose the Source data can be horizontally sliced (e.g. by date),
  * suppose we can also calculate a 'watermark' for each slice to indicate 'when was the slice last updated'
  * then we will be able to figure out which slices are missing in the Lake
  * we will also be able to figure out which slices need to be re-ingested into the Lake because it was updated in the Source after last ingestion

Consider this example: suppose the Source is a Relational DB table where deleted records are marked by column 'deleted_at' and deletions can happen a few days after the record is created.

Using 'slice-based ingestion' we could define slice_id and watermark as the following,
```
SELECT date AS slice_id, MAX( COALESCE(deleted_at, created_at) ) AS watermark
```
By comparing slice_id+watermark, we will know which slices need to be overriden in the Lake.

<TODO: embed pic (slice-based ingestion) https://drive.google.com/file/d/1Td5PqGp9YpUaolBM0zNR-BE5y6EIOVy0/view?usp=sharing >

**Note that slice-based ingestion does not guarantee 100% accuracy**


# Design Decision
The most significant decision is to take full custody of the data in the Lake: always know where to find and how to backup/restore/scrub 'physical data files' (e.g. \*.parquet and _delta_log/*.json)
  * in other words, leverage [unmanaged table](https://docs.databricks.com/lakehouse/data-objects.html#unmanaged-table) instead of [managed table](https://docs.databricks.com/lakehouse/data-objects.html#managed-table)
  
In case that Databricks Workspace is deleted (or Hive is corrupted), we would be able to re-construct the Data Lake without any loss,
  * Delta Lake Tables would stay intact since the physical data files will not be impacted
  * to re-construct the 'catalog', we can walk through 'directories' in each Zone and figure out what Delta Lake Tables are residing inside each directory
  * for permanent Views, the View definition should be in the code base (and be re-created regularly as BAU activity)

# Getting Started

Besides Databricks Workspace, you also need
* **Azure Storage Account (ADLS Gen2)**: create three Containers for env 'dev' 'cicd' and 'prod' and obtain RW SAS key
* **Azure KeyVault**: create your own Databricks Secret Scope

Fill in '002.env_config' _WATATSUMI_YAML (no need to manually mount as Watatsumi will inspect mountpoint every time at startup).

To test whether Watatsumi dev environment is ready, create a Notebook and try this,

<TODO: embed pic (hello watatsumi) https://drive.google.com/file/d/1Td5PqGp9YpUaolBM0zNR-BE5y6EIOVy0/view?usp=sharing >

# Integration Test

Integration test cases can be found at Notebook '000_init/999.run_integration_tests'. 

'run-integration-tests.yml.example' is an example showing how to trigger integration tests from Azure DevOps.

