import os
import yaml
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

def load_schema(schema_path="schema.yaml"):
    if not os.path.exists(schema_path):
        print(f"❌ Error: Schema file not found at {schema_path}")
        return None

    with open(schema_path, "r", encoding="utf-8") as f:
        content = f.read()
    def env_replacer(match):
        var_name = match.group(1)
        return os.environ.get(var_name, "knowledge-base")
        
    expanded_content = re.sub(r'\$\{([^}]+)\}', env_replacer, content)
    return yaml.safe_load(expanded_content)

def init_minio(schema):
    storage_cfg = schema.get('storage', {}).get('minio', {})
    bucket_name = storage_cfg.get('bucket_name', 'knowledge-base')
    
    s3_endpoint = os.environ.get('S3_ENDPOINT_URL')
    s3_access = os.environ.get('S3_ACCESS_KEY')
    s3_secret = os.environ.get('S3_SECRET_KEY')

    if not s3_endpoint or not s3_access:
        print("⏭️ Skip Default Storage Init: No S3 configuration found in environment (BYOS Mode)")
        return

    print(f"🔄 Connecting to Storage at: {s3_endpoint}")
    s3 = boto3.client('s3',
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access,
        aws_secret_access_key=s3_secret,
        config=Config(signature_version='s3v4')
    )
    
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ MinIO: Bucket '{bucket_name}' is ready (Data preserved)")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            print(f"⚠️ MinIO: Bucket not found. Creating '{bucket_name}'...")
            try:
                s3.create_bucket(Bucket=bucket_name)
                print(f"✨ MinIO: Bucket '{bucket_name}' created successfully")
            except ClientError as create_error:
                create_code = create_error.response.get('Error', {}).get('Code', 'Unknown')
                if create_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                    print(f"❌ MinIO Error: Cannot create bucket. Name '{bucket_name}' is already taken!")
                    raise create_error
                else:
                    print(f"❌ MinIO Error: Failed to create bucket: {create_error}")
        else:
            print(f"⚠️ MinIO Warning: Could not access bucket (Error: {error_code})")
    except Exception as e:
        print(f"⚠️ MinIO Connection Warning: {e}")
        print("⚠️ System will continue without Default Storage (Ready for BYOS).")

if __name__ == "__main__":
    print("🚀 Starting system initialization...")
    pipeline_schema = load_schema()
    if pipeline_schema:
        init_minio(pipeline_schema)
        print("🎉 System initialization complete")
    else:
        print("❌ Initialization failed: Could not read schema")