import pytest
from pathlib import Path
from ingestion.postgres_to_minio.pipeline import Config
import toml

def test_load_config_valid(tmp_path):
    config_file = tmp_path / "config.toml"
    content = """
    [minio]
    endpoint_url = "http://localhost:9000"
    aws_access_key_id = "minio"
    aws_secret_access_key = "minio123"
    bucket_name = "test-bucket"

    [postgres]
    host = "localhost"
    port = 5432
    username = "user"
    password = "pass"
    database = "testdb"

    [tables]
    source_tables = ["table1", "table2"]

    [pipeline]
    name = "test-pipeline"
    layout = "parquet"
    dataset_name = "test_dataset"
    """
    config_file.write_text(content)
    
    cfg = Config(str(config_file))
    
    assert cfg.minio_config['endpoint_url'] == "http://localhost:9000"
    assert cfg.bucket_name == "test-bucket"
    assert cfg.postgres_connection_string == "postgresql://user:pass@localhost:5432/testdb"
    assert cfg.table_names == ["table1", "table2"]
    assert cfg.pipeline_config['name'] == "test-pipeline"
    
def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        Config("non-existent-file.toml")

def test_load_config_invalid_toml(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("this is not toml")
    
    with pytest.raises(toml.TomlDecodeError):
        Config(str(config_file))

def test_load_config_missing_keys(tmp_path):
    config_file = tmp_path / "config.toml"
    config_file.write_text("""
    [minio]
    endpoint_url = "http://localhost:9000"
    """)
    
    cfg = Config(str(config_file))
    
    with pytest.raises(KeyError):
        _ = cfg.bucket_name