import uuid
from django.db import models
from django.contrib.auth.models import User

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