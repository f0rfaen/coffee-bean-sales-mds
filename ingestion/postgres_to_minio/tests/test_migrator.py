from unittest.mock import patch, MagicMock
from ingestion.postgres_to_minio.pipeline import PostgreSQLToMinIOMigrator
import pytest

@patch("ingestion.postgres_to_minio.pipeline.MinIOManager")
@patch("ingestion.postgres_to_minio.pipeline.dlt.pipeline")
@patch("ingestion.postgres_to_minio.pipeline.sql_database")
def test_run_migration_success(mock_sql, mock_pipeline, mock_minio_manager):
    mock_minio_manager.return_value.test_connection.return_value = True
    mock_minio_manager.return_value.verify_results.return_value = True
    
    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = {"status": "ok"}
    mock_pipeline.return_value = fake_pipeline
    mock_sql.return_value = "fake-source"

    migrator = PostgreSQLToMinIOMigrator("ingestion/postgres_to_minio/config.toml")
    
    result = migrator.run_migration()
    
    assert result["status"] == "ok"
    mock_minio_manager.return_value.test_connection.assert_called_once()
    mock_pipeline.assert_called_once()
    fake_pipeline.run.assert_called_once()
    mock_minio_manager.return_value.verify_results.assert_called_once()

@patch("ingestion.postgres_to_minio.pipeline.MinIOManager")
def test_run_migration_minio_connection_failure(mock_minio_manager):
    mock_minio_manager.return_value.test_connection.return_value = False
    
    migrator = PostgreSQLToMinIOMigrator("ingestion/postgres_to_minio/config.toml")
    
    result = migrator.run_migration()
    
    assert result is None
    mock_minio_manager.return_value.test_connection.assert_called_once()

@patch("ingestion.postgres_to_minio.pipeline.MinIOManager")
@patch("ingestion.postgres_to_minio.pipeline.dlt.pipeline")
@patch("ingestion.postgres_to_minio.pipeline.sql_database")
def test_run_migration_pipeline_failure(mock_sql, mock_pipeline, mock_minio_manager):
    mock_minio_manager.return_value.test_connection.return_value = True
    
    fake_pipeline = MagicMock()
    fake_pipeline.run.side_effect = Exception("Pipeline failed")
    mock_pipeline.return_value = fake_pipeline
    mock_sql.return_value = "fake-source"
    
    migrator = PostgreSQLToMinIOMigrator("ingestion/postgres_to_minio/config.toml")
    
    result = migrator.run_migration()
    
    assert result is None
    fake_pipeline.run.assert_called_once()

@patch("ingestion.postgres_to_minio.pipeline.MinIOManager")
@patch("ingestion.postgres_to_minio.pipeline.dlt.pipeline")
@patch("ingestion.postgres_to_minio.pipeline.sql_database")
def test_run_migration_verification_failure(mock_sql, mock_pipeline, mock_minio_manager):
    mock_minio_manager.return_value.test_connection.return_value = True
    mock_minio_manager.return_value.verify_results.return_value = False

    fake_pipeline = MagicMock()
    fake_pipeline.run.return_value = {"status": "ok"}
    mock_pipeline.return_value = fake_pipeline
    mock_sql.return_value = "fake-source"

    migrator = PostgreSQLToMinIOMigrator("ingestion/postgres_to_minio/config.toml")
    
    result = migrator.run_migration()
    
    assert result["status"] == "ok"
    mock_minio_manager.return_value.test_connection.assert_called_once()
    fake_pipeline.run.assert_called_once()
    mock_minio_manager.return_value.verify_results.assert_called_once()