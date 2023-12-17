# harlequin-bigquery

This is a BigQuery adapter for [Harlequin](https://github.com/tconbeer/harlequin), a SQL IDE for the terminal.

This adapter will use Application Default Credentials to authenticate with BigQuery and run queries.

## Configuration

This adapter supports the following options:

- `project`: The ID of the Google Cloud project to run Harlequin in. Defaults to whatever it can infer from the user's environment, i.e. `gcloud config list project`.
- `location`: The [location](https://cloud.google.com/compute/docs/regions-zones#available) used to run the catalog queries, which [must be region-qualified](https://cloud.google.com/bigquery/docs/information-schema-intro#syntax). Defaults to `US`.

## Required permissions

The user will need the permission to query both [`INFORMATION_SCHEMA.TABLES`](https://cloud.google.com/bigquery/docs/information-schema-tables) and [`INFORMATION_SCHEMA.COLUMNS`](https://cloud.google.com/bigquery/docs/information-schema-columns) to load the data catalog.

To query these views, you need the following Identity and Access Management (IAM) permissions:

- `bigquery.tables.get`
- `bigquery.tables.list`
- `bigquery.routines.get`
- `bigquery.routines.list`

Each of the following predefined IAM roles includes the necessary permissions:

- `roles/bigquery.admin`
- `roles/bigquery.dataViewer`
- `roles/bigquery.metadataViewer`

For more information about BigQuery permissions, see [Access control with IAM](https://cloud.google.com/bigquery/docs/access-control).
