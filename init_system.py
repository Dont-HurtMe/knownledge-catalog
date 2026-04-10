import os
import yaml
import boto3
from botocore.client import Config

def load_schema(schema_path="schema.yaml"):
    if not os.path.exists(schema_path):
        print(f"Error: Schema file not found at {schema_path}")
        return None
    with open(schema_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def init_minio(schema):
    storage_cfg = schema.get('storage', {}).get('minio', {})
    bucket_name = storage_cfg.get('bucket_name', 'knowledge-base')
    s3_endpoint = os.environ.get('S3_ENDPOINT_URL', 'http://minio:9000')
    s3_access = os.environ.get('S3_ACCESS_KEY', 'admin')
    s3_secret = os.environ.get('S3_SECRET_KEY', 'qwer1234')
    s3 = boto3.client('s3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access,
        aws_secret_access_key=s3_secret,
        config=Config(signature_version='s3v4')
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"MinIO: Bucket '{bucket_name}' is ready")
    except Exception:
        print(f"MinIO: Bucket not found. Creating '{bucket_name}'...")
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"MinIO: Bucket '{bucket_name}' created successfully")
        except Exception as e:
            print(f"MinIO: Failed to create bucket: {e}")

if __name__ == "__main__":
    print("Starting system initialization...")
    pipeline_schema = load_schema()
    if pipeline_schema:
        init_minio(pipeline_schema)
        print("System initialization complete")
    else:
        print("Initialization failed: Could not read schema")