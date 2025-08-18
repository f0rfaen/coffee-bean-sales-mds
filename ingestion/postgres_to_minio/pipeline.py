import dlt
from dlt.sources.sql_database import sql_database
import boto3
from botocore.exceptions import ClientError
import toml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import os
from urllib.parse import quote_plus


class Config:    
    def __init__(self, config_file: str = "config.toml"):
        self.config_file = Path(config_file)
        self.config = self._load_config()
        self._setup_logging()
    
    def _load_config(self) -> Dict[str, Any]:
        if not self.config_file.exists():
            raise FileNotFoundError(f"Configuration file {self.config_file} not found")
        
        with open(self.config_file, 'r') as f:
            return toml.load(f)
    
    def _setup_logging(self):
        log_config = self.config.get('logging', {})
        logging.basicConfig(
            level=getattr(logging, log_config.get('level', 'INFO')),
            format=log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        )
    
    @property
    def minio_config(self) -> Dict[str, str]:
        return {
            "endpoint_url": self.config['minio']['endpoint_url'],
            "aws_access_key_id": self.config['minio']['aws_access_key_id'],
            "aws_secret_access_key": self.config['minio']['aws_secret_access_key']
        }
    
    @property
    def bucket_name(self) -> str:
        return self.config['minio']['bucket_name']
    
    @property
    def postgres_connection_string(self) -> str:
        pg = self.config['postgres']
        # URL encode password in case it contains special characters
        password = quote_plus(pg['password'])
        username = quote_plus(pg['username'])
        
        return f"postgresql://{username}:{password}@{pg['host']}:{pg['port']}/{pg['database']}"
    
    @property
    def table_names(self) -> list:
        return self.config['tables']['source_tables']
    
    @property
    def pipeline_config(self) -> Dict[str, str]:
        return self.config['pipeline']


class MinIOManager:    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_client(self):
        return boto3.client('s3', **self.config.minio_config)
    
    def test_connection(self) -> bool:
        self.logger.info("Testing MinIO connection")
        try:
            s3 = self.get_client()
            s3.list_buckets()
            self.logger.info("MinIO connection successful")

            bucket_name = self.config.bucket_name
            try:
                s3.head_bucket(Bucket=bucket_name)
                self.logger.info(f"Bucket '{bucket_name}' exists")
                return True
            except ClientError as e:
                # Check for the specific 404 error code
                if e.response['Error']['Code'] == '404':
                    self.logger.info(f"Bucket '{bucket_name}' not found. Creating it.")
                    s3.create_bucket(Bucket=bucket_name)
                    self.logger.info(f"Bucket '{bucket_name}' created successfully.")
                    return True
                else:
                    # If it's a different ClientError, raise it to be caught by the outer block
                    raise e
        except Exception as e:
            self.logger.error(f"Failed to connect to MinIO: {e}")
            self.logger.error("Make sure Docker services are running (docker compose up -d)")
            return False
    def verify_results(self) -> bool:
        self.logger.info("Verifying migration results")
        try:
            s3 = self.get_client()
            response = s3.list_objects_v2(Bucket=self.config.bucket_name)
            
            if 'Contents' in response:
                files = [obj for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
                if files:
                    self.logger.info(f"Found {len(files)} Parquet files:")
                    for file in files:
                        size_kb = file['Size'] / 1024
                        self.logger.info(f"  {file['Key']} ({size_kb:.1f} KB)")
                    return True
                else:
                    self.logger.warning("No Parquet files found")
                    return False
            else:
                self.logger.warning("No files found in bucket")
                return False
        except Exception as e:
            self.logger.error(f"Verification failed: {e}")
            return False


class PostgreSQLToMinIOMigrator:
    def __init__(self, config_file: str = "config.toml"):
        self.config = Config(config_file)
        self.minio_manager = MinIOManager(self.config)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_pipeline(self):
        pipeline_config = self.config.pipeline_config
        
        return dlt.pipeline(
            pipeline_name=pipeline_config['name'],
            destination=dlt.destinations.filesystem(
                bucket_url=f"s3://{self.config.bucket_name}",
                credentials=self.config.minio_config,
                layout=pipeline_config['layout']
            ),
            dataset_name=pipeline_config['dataset_name']
        )
    
    def create_source(self):
        return sql_database(
            credentials=self.config.postgres_connection_string,
            table_names=self.config.table_names
        )
    
    def run_migration(self, write_disposition: str = "replace") -> Optional[Any]:
        self.logger.info("Starting PostgreSQL to MinIO migration")
        
        if not self.minio_manager.test_connection():
            return None
        
        source = self.create_source()
        pipeline = self.create_pipeline()
        
        try:
            self.logger.info("Running data pipeline")
            info = pipeline.run(source, write_disposition=write_disposition)
            
            self.logger.info("Pipeline completed successfully")
            self.logger.info(f"Data stored in s3://{self.config.bucket_name}/")
            
            if self.minio_manager.verify_results():
                self.logger.info("Migration verification successful")
            else:
                self.logger.warning("Migration verification had issues")
            
            return info
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            self.logger.error("Check PostgreSQL connection and MinIO setup")
            return None
    
    def get_pipeline_info(self) -> Optional[Dict[str, Any]]:
        try:
            pipeline = self.create_pipeline()
            return pipeline.last_trace
        except Exception as e:
            self.logger.error(f"Failed to get pipeline info: {e}")
            return None


def main():
    print("DLT PostgreSQL to MinIO Migration")
    print("=" * 40)
    
    try:
        migrator = PostgreSQLToMinIOMigrator()
        
        result = migrator.run_migration()
        
        if result:
            print("\nMigration completed successfully!")
            
            if hasattr(result, 'loads_table_name'):
                print(f"Pipeline run ID: {result.loads_table_name}")
            
        else:
            print("\nMigration failed. See logs above.")
            return 1
            
    except FileNotFoundError as e:
        print(f"Configuration error: {e}")
        print("Make sure config.toml exists in the current directory.")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())