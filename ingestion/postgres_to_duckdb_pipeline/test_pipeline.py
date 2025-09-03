from unittest.mock import patch, MagicMock

import pipeline



def test_normalize_column_name():
    assert pipeline.normalize_column_name("Order Date") == "order_date"
    assert pipeline.normalize_column_name("Customer ID") == "customer_id"
    assert pipeline.normalize_column_name("Already_clean") == "already_clean"



@patch("pipeline.psycopg2.connect")
def test_inspect_postgres_database(mock_connect, tmp_path):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    def fetchall_side_effect(*args, **kwargs):
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1] if len(mock_cur.execute.call_args[0]) > 1 else None

        if "FROM information_schema.tables" in sql:
            return [("orders",), ("customers",)]
        elif "FROM information_schema.columns" in sql and params == ("orders",):
            return [("order_id", "text", "NO"), ("order_date", "timestamp", "YES")]
        elif "FROM information_schema.columns" in sql and params == ("customers",):
            return [("customer_id", "text", "NO")]
        else:
            return []

    def fetchone_side_effect(*args, **kwargs):
        sql = mock_cur.execute.call_args[0][0]
        params = mock_cur.execute.call_args[0][1] if len(mock_cur.execute.call_args[0]) > 1 else None

        if "COUNT(*)" in sql:
            if params == ("orders",) or "orders" in sql:
                return (2,)
            elif params == ("customers",) or "customers" in sql:
                return (1,)
            else:
                return (0,)
        return (0,)

    mock_cur.fetchall.side_effect = fetchall_side_effect
    mock_cur.fetchone.side_effect = fetchone_side_effect

    with patch("pipeline.toml.load") as mock_toml:
        mock_toml.return_value = {"postgres": {
            "host": "h", "port": 5432, "database": "db", "username": "u", "password": "p"
        }}

        result = pipeline.inspect_postgres_database()

        assert "orders" in result
        assert result["orders"]["row_count"] == 2
        assert result["orders"]["column_count"] == 2
        assert "customers" in result
        assert result["customers"]["row_count"] == 1


@patch("pipeline.psycopg2.connect")
def test_extract_postgres_table_full(mock_connect):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_cur.fetchone.side_effect = [
        (True,),
    ]
    mock_cur.description = [("id",), ("name",)]
    mock_cur.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

    with patch("pipeline.toml.load") as mock_toml:
        mock_toml.return_value = {"postgres": {
            "host": "h", "port": 5432, "database": "db", "username": "u", "password": "p"
        }}

        rows = list(pipeline.extract_postgres_table_incremental("customers"))
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Alice"


@patch("pipeline.psycopg2.connect")
def test_extract_postgres_table_with_cursor(mock_connect):
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cur

    mock_cur.fetchone.side_effect = [
        (True,),
        ("order_date",)
    ]
    mock_cur.description = [("order_date",), ("order_id",)]
    mock_cur.fetchall.return_value = [(1, "o1"), (2, "o2")]

    with patch("pipeline.toml.load") as mock_toml, \
        patch("pipeline.dlt.current") as mock_dlt_state:
        mock_toml.return_value = {"postgres": {
            "host": "h", "port": 5432, "database": "db", "username": "u", "password": "p"
        }}
        mock_dlt_state.source_state.return_value = {}

        rows = list(pipeline.extract_postgres_table_incremental("orders", cursor_column="order_date"))
        assert len(rows) == 2
        assert rows[-1]["order_id"] == "o2"
        assert "orders_order_date_last_cursor" in mock_dlt_state.source_state()

@patch("pipeline.dlt.pipeline")
@patch("pipeline.toml.load")
def test_create_and_run_pipeline(mock_toml, mock_pipeline):
    mock_toml.return_value = {
        "duckdb": {"file_path": ":memory:"},
        "pipeline": {"name": "test_pipeline"},
        "tables": {"source_tables": ["customers"]},
    }

    mock_pipe_instance = MagicMock()
    mock_pipeline.return_value = mock_pipe_instance
    mock_pipe_instance.run.return_value = {"tables": {"customers": {"status": "loaded"}}}

    pipeline.create_and_run_pipeline()

    mock_pipeline.assert_called_once()
    mock_pipe_instance.run.assert_called_once()
