import pytest
from unittest.mock import MagicMock
import sys
import os
import toml
import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pipeline


@pytest.fixture
def mock_dependencies(monkeypatch):
    mock_config = {
        "postgres": {
            "host": "localhost", "port": 5433, "database": "coffee_bean_sales",
            "username": "cbs_username", "password": "cbs_password"
        },
        "duckdb": {"file_path": "../../sim_data_warehouse/sim_data_warehouse.duckdb"},
        "pipeline": {"name": "postgres_to_duckdb", "dataset_name": "raw"},
        "tables": {"source_tables": ["orders", "customers", "products"]},
        "logging": {"level": "INFO", "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"}
    }
    monkeypatch.setattr('toml.load', MagicMock(return_value=mock_config))

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    monkeypatch.setattr('psycopg2.connect', MagicMock(return_value=mock_conn))

    mock_dlt_pipeline = MagicMock()
    mock_dlt_resource = MagicMock()
    monkeypatch.setattr('dlt.pipeline', mock_dlt_pipeline)
    monkeypatch.setattr('dlt.resource', mock_dlt_resource)

    yield {
        "mock_cursor": mock_cursor,
        "mock_conn": mock_conn,
        "mock_dlt_pipeline": mock_dlt_pipeline,
        "mock_dlt_resource": mock_dlt_resource,
    }

def test_extract_postgres_table_yields_data(mock_dependencies):
    mock_cursor = mock_dependencies['mock_cursor']
    mock_conn = mock_dependencies['mock_conn']

    mock_data = [
        (1, 'John Doe', 'john@example.com'),
        (2, 'Jane Smith', 'jane@example.com')
    ]
    mock_cursor.description = [('id', None), ('name', None), ('email', None)]
    mock_cursor.fetchall.return_value = mock_data

    table_name = "test_table"
    result_generator = pipeline.extract_postgres_table(table_name)

    result_list = list(result_generator)

    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name}")
    mock_cursor.fetchall.assert_called_once()

    expected_data = [
        {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'},
        {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com'}
    ]
    assert result_list == expected_data


def test_main_pipeline_run_calls_dlt_correctly(mock_dependencies):
    mock_dlt_pipeline = mock_dependencies['mock_dlt_pipeline']
    mock_dlt_resource = mock_dependencies['mock_dlt_resource']
    
    mock_pipeline_instance = MagicMock()
    mock_dlt_pipeline.return_value = mock_pipeline_instance
    
    mock_dlt_resource.return_value = MagicMock()

    pipeline.create_and_run_pipeline()
    
    mock_dlt_pipeline.assert_called_once()
    assert mock_dlt_pipeline.call_args.kwargs['pipeline_name'] == 'postgres_to_duckdb'
    assert mock_dlt_pipeline.call_args.kwargs['dataset_name'] == 'raw'

    assert mock_dlt_resource.call_count == 3
    
    mock_pipeline_instance.run.assert_called_once()


def test_extract_postgres_table_empty_data(mock_dependencies):
    mock_cursor = mock_dependencies['mock_cursor']
    mock_conn = mock_dependencies['mock_conn']
    
    mock_cursor.description = [('id', None), ('name', None)]
    mock_cursor.fetchall.return_value = []
    
    table_name = "empty_table"
    result_generator = pipeline.extract_postgres_table(table_name)
    
    assert list(result_generator) == []
    
    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name}")
    mock_cursor.fetchall.assert_called_once()
    

def test_config_file_not_found_raises_error(monkeypatch):
    monkeypatch.setattr('toml.load', MagicMock(side_effect=FileNotFoundError("File not found")))

    with pytest.raises(FileNotFoundError):
        pipeline.create_and_run_pipeline()


def test_database_connection_error(monkeypatch):
    monkeypatch.setattr('psycopg2.connect', MagicMock(side_effect=psycopg2.OperationalError("Could not connect to database")))

    with pytest.raises(psycopg2.OperationalError, match="Could not connect to database"):
        list(pipeline.extract_postgres_table("some_table"))
