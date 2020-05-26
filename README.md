### BeamSnowflakeExamples

This repository contains examples of using [Snowflake](https://www.snowflake.com/) with [Apache Beam](https://github.com/apache/beam).
Precisely contains batching, streaming and cross-language usage examples.  

#### Setup of third parties
1. [Create Snowflake Account](https://trial.snowflake.com/?utm_cta=website-homepage-hero-free-trial&_ga=2.199198959.1328097007.1590138521-373661872.1583847959) 
with Google Cloud Platform as a cloud provider.
2. Make sure that your default role for your username is set to ACCOUNTADMIN
    ```
    alter user <USERNAME> set default_role=ACCOUNTADMIN; 
    ```
3. [Create a new Snowflake database](https://docs.snowflake.com/en/sql-reference/sql/create-database.html):
    ```
    create database <DATABASE NAME>;
    ```
4. [Create Google Cloud Platform account](https://cloud.google.com/free).
5. [Create a new GCP project](https://cloud.google.com/resource-manager/docs/creating-managing-projects).
6. [Create GCP bucket](https://cloud.google.com/storage/docs/creating-buckets)
7. Create storage integration object in Snowflake using the following command:
    ```
    CREATE OR REPLACE STORAGE INTEGRATION <INTEGRATION NAME>
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://<BUCKET NAME>');
    ```
   Please note that `gcs` prefix is used here, not `gs`.
8. Authorize Snowflake to operate on your bucket by following [Grant the Service Account Permissions to Access Bucket Objects](https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html#step-3-grant-the-service-account-permissions-to-access-bucket-objects)
9. Setup gcloud on your computer by following [Using the Google Cloud SDK installer](https://cloud.google.com/sdk/docs/downloads-interactive)
10. Run one of the provided examples.

#### Batching example

#### Streaming example

#### Cross-language example
