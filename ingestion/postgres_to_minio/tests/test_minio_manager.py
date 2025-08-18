# ingestion/postgres_to_minio/tests/test_minio_manager.py
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from ingestion.postgres_to_minio.pipeline import MinIOManager

@patch("ingestion.postgres_to_minio.pipeline.boto3.client")
def test_minio_connection_success(mock_boto):
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3
    
    mock_s3.head_bucket.return_value = {}

    cfg = MagicMock()
    cfg.minio_config = {"endpoint_url": "...", "aws_access_key_id": "...", "aws_secret_access_key": "..."}
    cfg.bucket_name = "mybucket"

    manager = MinIOManager(cfg)
    connection_status = manager.test_connection()

    assert connection_status is True
    mock_s3.create_bucket.assert_not_called()

@patch("ingestion.postgres_to_minio.pipeline.boto3.client")
def test_minio_connection_failure_and_creation(mock_boto):
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3
    
    mock_error_response = {
        'Error': {
            'Code': '404'
        }
    }
    mock_exception = ClientError(mock_error_response, "HeadBucket")
    
    mock_s3.head_bucket.side_effect = [mock_exception, {}]
    
    cfg = MagicMock()
    cfg.minio_config = {"endpoint_url": "...", "aws_access_key_id": "...", "aws_secret_access_key": "..."}
    cfg.bucket_name = "mybucket"
    
    manager = MinIOManager(cfg)
    connection_status = manager.test_connection()
    
    assert connection_status is True
    mock_s3.create_bucket.assert_called_once_with(Bucket="mybucket")