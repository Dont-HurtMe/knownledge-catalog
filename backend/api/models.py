import os
import uuid
import boto3
from botocore.client import Config
from django.db import models
from django.contrib.auth.models import User
from django.db.models.signals import pre_delete
from django.dispatch import receiver

class KnowledgeCatalog(models.Model):
    STATUS_CHOICES = [
        ('PROCESSING', 'Processing'),
        ('PARTIAL_READY', 'Partial Ready'),
        ('WAITING_VLM_APPROVAL', 'Waiting VLM Approval'),
        ('PROCESSING_VLM', 'Processing VLM'),
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    original_filename = models.CharField(max_length=255)
    safe_filename = models.CharField(max_length=255)
    raw_storage_path = models.CharField(max_length=1000)
    parquet_storage_path = models.CharField(max_length=1000, blank=True, null=True)
    
    provider = models.CharField(max_length=100, blank=True, null=True)
    category = models.CharField(max_length=100, blank=True, null=True)
    file_type = models.CharField(max_length=50)
    
    auto_vlm = models.BooleanField(default=False)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='PROCESSING')
    
    total_pages = models.IntegerField(default=0)
    normal_pages = models.IntegerField(default=0)
    vlm_pages = models.IntegerField(default=0)
    pending_vlm_pages = models.IntegerField(default=0)
    error_message = models.TextField(blank=True, null=True)
    
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(blank=True, null=True)

    s3_endpoint = models.CharField(max_length=255, blank=True, null=True)
    s3_access_key = models.CharField(max_length=255, blank=True, null=True)
    s3_secret_key = models.CharField(max_length=255, blank=True, null=True)
    s3_bucket_name = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self):
        return self.original_filename

@receiver(pre_delete, sender=KnowledgeCatalog)
def delete_s3_files_on_model_delete(sender, instance, **kwargs):
    s3_ep = instance.s3_endpoint or os.environ.get('S3_ENDPOINT_URL')
    s3_ak = instance.s3_access_key or os.environ.get('S3_ACCESS_KEY')
    s3_sk = instance.s3_secret_key or os.environ.get('S3_SECRET_KEY')
    s3_bn = instance.s3_bucket_name or os.environ.get('S3_BUCKET_NAME', 'knowledge-base')
    if not (s3_ep and s3_ak and s3_sk):
        print(f"⏭️ Skip deleting files for {instance.id}: No S3 credentials found.")
        return
    try:
        s3 = boto3.client('s3',
            endpoint_url=s3_ep,
            aws_access_key_id=s3_ak,
            aws_secret_access_key=s3_sk,
            config=Config(signature_version='s3v4')
        )
        if instance.raw_storage_path:
            s3.delete_object(Bucket=s3_bn, Key=instance.raw_storage_path)
            print(f"🗑️ Deleted Raw File: {instance.raw_storage_path}")
        if instance.parquet_storage_path:
            prefix = f"{instance.parquet_storage_path}/" if not instance.parquet_storage_path.endswith('/') else instance.parquet_storage_path
            response = s3.list_objects_v2(Bucket=s3_bn, Prefix=prefix)
            if 'Contents' in response:
                delete_keys = [{'Key': obj['Key']} for obj in response['Contents']]
                s3.delete_objects(Bucket=s3_bn, Delete={'Objects': delete_keys})
                print(f"🗑️ Deleted {len(delete_keys)} Processed Files from: {prefix}")

    except Exception as e:
        print(f"⚠️ S3 Deletion Warning: Could not delete files for {instance.id}. Error: {e}")