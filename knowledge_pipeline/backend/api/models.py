import uuid
from django.db import models

class KnowledgeCatalog(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    original_filename = models.CharField(max_length=500)
    file_type = models.CharField(max_length=50)
    provider = models.CharField(max_length=200)
    producer_raw = models.CharField(max_length=255, null=True, blank=True)
    producer_group = models.CharField(max_length=100, null=True, blank=True)
    file_hash = models.CharField(max_length=64, unique=True)
    total_units = models.IntegerField(default=1)
    status = models.CharField(max_length=50, default='PENDING')
    created_at = models.DateTimeField(auto_now_add=True)

class KnowledgeTransaction(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    catalog = models.ForeignKey(KnowledgeCatalog, on_delete=models.CASCADE)
    unit_reference = models.CharField(max_length=100)
    strategy = models.CharField(max_length=50)
    error_reason = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=50, default='QUEUED')
    content_hash = models.CharField(max_length=64, null=True, blank=True)
    minio_payload_path = models.CharField(max_length=500, null=True, blank=True)
    cluster_id = models.IntegerField(null=True, blank=True)
