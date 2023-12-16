from __future__ import annotations

from typing import Any, Sequence

from google.cloud import bigquery
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.enums import StandardSqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableListItem
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
from harlequin_bigquery.keywords import RESERVED_KEYWORDS
from harlequin_bigquery.functions import BUILTIN_FUNCTIONS


class BigQueryCursor(HarlequinCursor):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.cur = args[0]
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        names = self.cur.column_names
        types = self.cur.column_types
        return list(zip(names, types))

    def set_limit(self, limit: int) -> BigQueryCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType:
        try:
            if self._limit is None:
                return self.cur.fetchall()
            else:
                return self.cur.fetchmany(self._limit)
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e


class BigQueryConnection(HarlequinConnection):
    # Abbreviations for table types
    TABLE_TYPE_MAPPING = {
        "TABLE": "t",
        "VIEW": "v",
        "EXTERNAL": "ext",
    }

    # Abbreviations for column types
    # TODO: Write a test that we have all types mapped
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

    def __init__(self, *args: Any, init_message: str = "", **kwargs: Any) -> None:
        self.init_message = init_message
        try:
            self.client = bigquery.Client()
        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e), title="Harlequin could not connect to BigQuery."
            ) from e

    @property
    def project(self) -> str:
        return self.client.project

    def execute(self, query: str) -> HarlequinCursor | None:
        try:
            cur = self.client.execute(query)  # type: ignore
        except Exception as e:
            raise HarlequinQueryError(
                msg=str(e),
                title="Harlequin encountered an error while executing your query.",
            ) from e
        else:
            if cur is not None:
                return BigQueryCursor(cur)
            else:
                return None

    ## TODO: Ideas for options
    # Include hidden datasets?

    def get_catalog(self) -> Catalog:
        dataset_items: list[CatalogItem] = []
        datasets = self.client.list_datasets()
        dataset: DatasetListItem
        for dataset in datasets:
            tables = self.client.list_tables(dataset.dataset_id)
            table_items: list[CatalogItem] = []
            table: TableListItem
            for table in tables:
                # Get full table object so we can get schema
                table_obj = self.client.get_table(table.reference)

                # Get columns
                schema = table_obj.schema
                field: SchemaField
                field_items: list[CatalogItem] = []
                for field in schema:
                    standard_sql_field = field.to_standard_sql()

                    if (
                        not standard_sql_field.type
                        or not standard_sql_field.type.type_kind
                    ):
                        type_label = "?"  # Type is unspecified
                    else:
                        type_label = self.COLUMN_TYPE_MAPPING[
                            standard_sql_field.type.type_kind
                        ]

                    label = standard_sql_field.name if standard_sql_field.name else "?"

                    field_items.append(
                        CatalogItem(
                            qualified_identifier=f"`{self.project}`.`{dataset.dataset_id}`.`{table.table_id}`.`{field.name}`",
                            query_name=f"`{field.name}`",
                            label=label,
                            type_label=type_label,
                        )
                    )

                table_items.append(
                    CatalogItem(
                        qualified_identifier=f"`{self.project}`.`{dataset.dataset_id}`.`{table.table_id}`",
                        query_name=f"`{self.project}`.`{dataset.dataset_id}`.`{table.table_id}`",
                        label=table.table_id,
                        type_label=self.TABLE_TYPE_MAPPING[table.table_type],
                        children=field_items,
                    )
                )

            dataset_items.append(
                CatalogItem(
                    qualified_identifier=f"`{self.project}`.`{dataset.dataset_id}`",
                    query_name=f"`{self.project}`.`{dataset.dataset_id}`",
                    label=dataset.dataset_id,
                    type_label="ds",
                    children=table_items,
                )
            )

        return Catalog(items=dataset_items)

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
    ADAPTER_OPTIONS = BIGQUERY_ADAPTER_OPTIONS

    def __init__(self, conn_str: Sequence[str], **options: Any) -> None:
        self.conn_str = conn_str
        self.options = options

    def connect(self) -> BigQueryConnection:
        conn = BigQueryConnection(self.conn_str, self.options)
        return conn
