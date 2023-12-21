from __future__ import annotations

import re
from typing import Any

from google.cloud import bigquery
from google.cloud.bigquery.dbapi import Cursor as BigQueryDbApiCursor
from google.cloud.bigquery.enums import StandardSqlTypeNames
from google.cloud.bigquery.table import Row
from harlequin import (
    HarlequinAdapter,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.autocomplete.completion import HarlequinCompletion
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_bigquery.cli_options import BIGQUERY_ADAPTER_OPTIONS
from harlequin_bigquery.functions import BUILTIN_FUNCTIONS
from harlequin_bigquery.keywords import RESERVED_KEYWORDS

# Abbreviations for column types
COLUMN_TYPE_MAPPING = {
    StandardSqlTypeNames.TYPE_KIND_UNSPECIFIED: "?",
    StandardSqlTypeNames.INT64: "#",
    StandardSqlTypeNames.BOOL: "t/f",
    StandardSqlTypeNames.FLOAT64: "#.#",
    StandardSqlTypeNames.STRING: "s",
    StandardSqlTypeNames.BYTES: "0b",
    StandardSqlTypeNames.TIMESTAMP: "ts",
    StandardSqlTypeNames.DATE: "d",
    StandardSqlTypeNames.TIME: "t",
    StandardSqlTypeNames.DATETIME: "dt",
    StandardSqlTypeNames.INTERVAL: "|-|",
    StandardSqlTypeNames.GEOGRAPHY: "geo",
    StandardSqlTypeNames.NUMERIC: "#.#",
    StandardSqlTypeNames.BIGNUMERIC: "#.#",
    StandardSqlTypeNames.JSON: "{j}",
    StandardSqlTypeNames.ARRAY: "[]",
    StandardSqlTypeNames.STRUCT: "{}",
}


class BigQueryCursor(HarlequinCursor):
    def __init__(self, cursor: BigQueryDbApiCursor, *args: Any, **kwargs: Any) -> None:
        self.cursor = cursor
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        if not self.cursor.query_job:
            raise TypeError("Cursor has no query job")

        result_schema = self.cursor.query_job.result().schema
        fields = []
        for field in result_schema:
            standard_sql_field = field.to_standard_sql()
            if not standard_sql_field.type or not standard_sql_field.type.type_kind:
                type_label = "?"  # Type is unspecified
            else:
                type_label = COLUMN_TYPE_MAPPING[standard_sql_field.type.type_kind]
            fields.append((field.name, type_label))

        return fields

    def set_limit(self, limit: int) -> BigQueryCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if not self.cursor.query_job:
                raise RuntimeError("Cursor has no query job")
            if not self.cursor.query_job.destination:
                return []
            if self._limit is None:
                result: list[Row] = self.cursor.fetchall()
            else:
                result = self.cursor.fetchmany(self._limit)
            return [row.values() for row in result]
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e


class BigQueryConnection(HarlequinConnection):
    # Abbreviations for table types
    # From the `table_type` field in the INFORMATION_SCHEMA.TABLES table
    # https://cloud.google.com/bigquery/docs/information-schema-tables#schema

    TABLE_TYPE_MAPPING = {
        "BASE TABLE": "t",
        "CLONE": "tc",
        "SNAPSHOT": "ts",
        "VIEW": "v",
        "MATERIALIZED VIEW": "mv",
        "EXTERNAL": "ext",
    }

    def __init__(
        self,
        project: str | None = None,
        location: str | None = None,
        init_message: str = "",
        **_: Any,
    ) -> None:
        self.location = location or "US"
        self.init_message = init_message
        try:
            self.client = bigquery.Client(project=project, location=location)
            self.conn = bigquery.dbapi.Connection(self.client)
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to BigQuery."
            ) from e

    @property
    def project(self) -> str:
        return self.client.project

    def execute(self, query: str) -> BigQueryCursor:
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e

        return BigQueryCursor(cursor)

    def get_catalog(self) -> Catalog:
        query = f"""
            select
                datasets.schema_name as dataset_id,
                tables.table_name as table_id,
                tables.table_type as table_type,
                columns,column_name,
                columns.data_type as column_type
            from `{self.project}.region-{self.location}.INFORMATION_SCHEMA.SCHEMATA` datasets
            left join `{self.project}.region-{self.location}.INFORMATION_SCHEMA.TABLES` tables
            on datasets.catalog_name = tables.table_catalog
            and datasets.schema_name = tables.table_schema
            left join `{self.project}.region-{self.location}.INFORMATION_SCHEMA.COLUMNS` columns
            using (table_catalog, table_schema, table_name)
            order by dataset_id, table_id, column_name
        """
        cursor = self.execute(query)
        results = cursor.cursor.fetchall()

        current_dataset = current_table = None
        datasets: dict[str, CatalogItem] = {}

        # Iterate in sorted order by dataset, table, then column
        for row in results:
            dataset_id = row.dataset_id
            table_id = row.table_id
            column_name = row.column_name

            if dataset_id != current_dataset:
                current_dataset = dataset_id
                datasets[row.dataset_id] = CatalogItem(
                    qualified_identifier=f"`{self.project}`.`{dataset_id}`",
                    query_name=f"`{dataset_id}`",
                    label=dataset_id,
                    type_label="ds",
                    children=[],
                )

            if table_id and table_id != current_table:
                current_table = table_id
                table_catalog_item = CatalogItem(
                    qualified_identifier=f"`{self.project}`.`{dataset_id}`.`{table_id}`",
                    query_name=f"`{table_id}`",
                    label=table_id,
                    type_label=self.TABLE_TYPE_MAPPING[row.table_type],
                    children=[],
                )
                datasets[dataset_id].children.append(table_catalog_item)

            if column_name:
                # remove anything in <> from the column_type
                column_type_cleaned = re.sub(r"\<.*\>", "", row.column_type)
                column_type_label = COLUMN_TYPE_MAPPING[
                    StandardSqlTypeNames(column_type_cleaned)
                ]
                column_catalog_item = CatalogItem(
                    qualified_identifier=f"`{self.project}`.`{row.dataset_id}`.`{row.table_id}`.`{row.column_name}`",
                    query_name=f"`{row.column_name}`",
                    label=row.column_name,
                    type_label=column_type_label,
                )
                # Relies on order being sorted
                datasets[dataset_id].children[-1].children.append(column_catalog_item)

        return Catalog(items=list(datasets.values()))

    def get_completions(self) -> list[HarlequinCompletion]:
        type_completions = [
            HarlequinCompletion(
                label=str(type_name.value),
                type_label="type",
                value=str(type_name.value),
                priority=1000,
                context=None,
            )
            for type_name in StandardSqlTypeNames
        ]

        keyword_completions = [
            HarlequinCompletion(
                label=keyword,
                type_label="kw",
                value=keyword,
                priority=100,
                context=None,
            )
            for keyword in RESERVED_KEYWORDS
        ]

        # TODO: Get UDFs and routines
        function_completions = [
            HarlequinCompletion(
                label=name,
                type_label="fn",
                value=name,
                priority=1000,
                context=None,
            )
            for name in BUILTIN_FUNCTIONS
        ]

        return [*type_completions, *keyword_completions, *function_completions]


class BigQueryAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = BIGQUERY_ADAPTER_OPTIONS  # type: ignore

    def __init__(
        self, project: str | None = None, location: str | None = None, **_: Any
    ) -> None:
        self.project = project
        self.location = location

    def connect(self) -> BigQueryConnection:
        conn = BigQueryConnection(project=self.project, location=self.location)
        return conn
