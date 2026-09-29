import sys
from typing import Any, Generator
from unittest.mock import MagicMock

import pytest
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.enums import StandardSqlTypeNames
from harlequin.adapter import HarlequinAdapter, HarlequinConnection, HarlequinCursor
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinQueryError
from harlequin_bigquery.adapter import (
    COLUMN_TYPE_MAPPING,
    BigQueryAdapter,
    BigQueryConnection,
)
from textual_fastdatatable.backend import create_backend

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


@pytest.fixture(scope="session")
def bigquery_client() -> Client:
    return Client()


@pytest.fixture(scope="module")
def test_dataset(bigquery_client: Client) -> Generator[str, None, None]:
    dataset_name = "tmp__harlequin_bigquery_test"
    bigquery_client.create_dataset(dataset_name, exists_ok=True)
    yield dataset_name
    bigquery_client.delete_dataset(dataset_name, delete_contents=True)


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "bigquery-adapter"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == BigQueryAdapter


def test_connect() -> None:
    conn = BigQueryAdapter().connect()
    assert isinstance(conn, HarlequinConnection)


def test_init_extra_kwargs() -> None:
    assert BigQueryAdapter(foo=1, bar="baz").connect()


@pytest.fixture
def connection() -> BigQueryConnection:
    return BigQueryAdapter().connect()


def test_get_catalog(connection: BigQueryConnection) -> None:
    catalog = connection.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)


def test_get_catalog_with_parameterized_types(
    connection: BigQueryConnection, test_dataset: str
) -> None:
    # create a temporary table with a parametrized type
    query = f"CREATE TABLE {test_dataset}.test_table (id NUMERIC(9, 8));"
    cursor = connection.execute(query)
    try:
        catalog = connection.get_catalog()
    finally:
        connection.execute(f"DROP TABLE {test_dataset}.test_table;")
    assert True


def test_execute_select(connection: BigQueryConnection) -> None:
    cur = connection.execute("select 1 as a")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "#")]
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 1


def test_execute_select_no_rows_returned(connection: BigQueryConnection) -> None:
    cur = connection.execute("select a from (select 1 as a) where false")
    assert isinstance(cur, HarlequinCursor)
    assert cur.columns() == [("a", "#")]
    data = cur.fetchall()
    assert not data


def test_execute_select_dupe_cols(connection: BigQueryConnection) -> None:
    cur = connection.execute("select 1 as a, 2 as a, 3 as a")
    assert isinstance(cur, HarlequinCursor)
    assert len(cur.columns()) == 3
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 3
    assert backend.row_count == 1


def test_set_limit(connection: BigQueryConnection) -> None:
    cur = connection.execute("select 1 as a union all select 2 union all select 3")
    assert isinstance(cur, HarlequinCursor)
    cur = cur.set_limit(2)
    assert isinstance(cur, HarlequinCursor)
    data = cur.fetchall()
    backend = create_backend(data)
    assert backend.column_count == 1
    assert backend.row_count == 2


def test_execute_raises_query_error(connection: BigQueryConnection) -> None:
    with pytest.raises(HarlequinQueryError):
        _ = connection.execute("selec;")


def test_all_column_types_are_mapped() -> None:
    assert len(StandardSqlTypeNames) == len(COLUMN_TYPE_MAPPING)
    for value in StandardSqlTypeNames:
        assert value in COLUMN_TYPE_MAPPING


def test_create_and_select_temp_table(connection: BigQueryConnection):
    query = """
    WITH temp_users AS (
        SELECT * FROM UNNEST([
            STRUCT(1 AS user_id, 'Alice' AS user_name, DATE '2021-01-01' AS join_date),
            STRUCT(2, 'Bob', DATE '2021-02-01'),
            STRUCT(3, 'Charlie', DATE '2021-03-01')
        ])
    )
    SELECT * FROM temp_users;
    """
    cursor = connection.execute(query)
    data = cursor.fetchall()
    backend = create_backend(data)
    assert backend.row_count == 3


def test_create_and_drop_temp_table(connection: BigQueryConnection):
    query = """
    CREATE TEMP TABLE test_table (
        id INT64,
        name STRING
    );
    DROP TABLE test_table;
    """
    cursor = connection.execute(query)
    data = cursor.fetchall()
    assert not data


def test_insert_into_temp_table(connection: BigQueryConnection):
    query = """
    CREATE TEMP TABLE test_table (id INT64, name STRING);
    INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob');
    DROP TABLE test_table;
    """
    cursor = connection.execute(query)
    data = cursor.fetchall()
    assert not data


def test_update_temp_table(connection: BigQueryConnection):
    query = """
    CREATE TEMP TABLE test_table (id INT64, name STRING);
    INSERT INTO test_table (id, name) VALUES (1, 'Alice');
    UPDATE test_table SET name = 'Alice Updated' WHERE id = 1;
    DROP TABLE test_table;
    """
    cursor = connection.execute(query)
    data = cursor.fetchall()
    assert not data


def test_delete_from_temp_table(connection: BigQueryConnection):
    query = """
    CREATE TEMP TABLE test_table (id INT64, name STRING);
    INSERT INTO test_table (id, name) VALUES (1, 'Alice');
    DELETE FROM test_table WHERE id = 1;
    DROP TABLE test_table;
    """
    cursor = connection.execute(query)
    data = cursor.fetchall()
    assert not data


def test_get_catalog_resets_current_table_between_datasets():
    connection = BigQueryConnection.__new__(BigQueryConnection)
    connection.location = "US"

    connection.client = MagicMock()
    connection.client.project = "test-project"

    rows = [
        MagicMock(
            dataset_id="dataset_a",
            table_id="shared_table",
            table_type="BASE TABLE",
            column_name="column_a",
            column_type="STRING",
        ),
        MagicMock(
            dataset_id="dataset_b",
            table_id="shared_table",
            table_type="BASE TABLE",
            column_name="column_b",
            column_type="STRING",
        ),
    ]

    cursor = MagicMock()
    cursor.cursor.fetchall.return_value = rows
    connection.execute = MagicMock(return_value=cursor)

    catalog = connection.get_catalog()

    assert len(catalog.items) == 2

    dataset_a = catalog.items[0]
    assert dataset_a.label == "dataset_a"
    assert len(dataset_a.children) == 1
    assert dataset_a.children[0].label == "shared_table"
    assert len(dataset_a.children[0].children) == 1
    assert dataset_a.children[0].children[0].label == "column_a"

    dataset_b = catalog.items[1]
    assert dataset_b.label == "dataset_b"
    assert len(dataset_b.children) == 1
    assert dataset_b.children[0].label == "shared_table"
    assert len(dataset_b.children[0].children) == 1
    assert dataset_b.children[0].children[0].label == "column_b"


def test_get_catalog_handles_range_column_type():
    connection = BigQueryConnection.__new__(BigQueryConnection)
    connection.location = "US"

    connection.client = MagicMock()
    connection.client.project = "test-project"

    rows = [
        MagicMock(
            dataset_id="dataset_a",
            table_id="table_a",
            table_type="BASE TABLE",
            column_name="range_column",
            column_type="RANGE",
        ),
    ]

    cursor = MagicMock()
    cursor.cursor.fetchall.return_value = rows
    connection.execute = MagicMock(return_value=cursor)

    catalog = connection.get_catalog()

    assert len(catalog.items) == 1

    dataset = catalog.items[0]
    assert len(dataset.children) == 1

    table = dataset.children[0]
    assert len(table.children) == 1

    column = table.children[0]
    assert column.label == "range_column"
    assert column.type_label == "rng"
