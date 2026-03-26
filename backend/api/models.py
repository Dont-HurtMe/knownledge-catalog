import uuid
from django.db import models

class KnowledgeCatalog(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    original_filename = models.CharField(max_length=500)
    safe_filename = models.CharField(max_length=500, null=True, blank=True)
    raw_storage_path = models.CharField(max_length=1000, null=True, blank=True)
    parquet_storage_path = models.CharField(max_length=1000, null=True, blank=True)
    file_type = models.CharField(max_length=50)
    provider = models.CharField(max_length=200)
    category = models.CharField(max_length=200, null=True, blank=True, default=None)
    total_pages = models.IntegerField(default=0)
    status = models.CharField(max_length=50, default='PROCESSING')
    created_at = models.DateTimeField(auto_now_add=True)

class KnowledgeTransaction(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    catalog = models.ForeignKey(KnowledgeCatalog, on_delete=models.CASCADE)
    page_number = models.IntegerField()
    strategy = models.CharField(max_length=50, null=True, blank=True)
    status = models.CharField(max_length=50, default='QUEUED')