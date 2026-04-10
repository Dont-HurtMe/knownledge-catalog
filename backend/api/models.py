
import uuid
from django.db import models
from django.conf import settings

class KnowledgeCatalog(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    original_filename = models.CharField(max_length=500)
    safe_filename = models.CharField(max_length=500, null=True, blank=True)
    raw_storage_path = models.CharField(max_length=1000, null=True, blank=True)
    parquet_storage_path = models.CharField(max_length=1000, null=True, blank=True)
    file_type = models.CharField(max_length=50)
    
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='catalogs')
    provider = models.CharField(max_length=200)
    category = models.CharField(max_length=200, null=True, blank=True, default=None)
    
    auto_vlm = models.BooleanField(default=False)
    pending_vlm_pages = models.IntegerField(default=0)
    
    total_pages = models.IntegerField(default=0)
    normal_pages = models.IntegerField(default=0)
    vlm_pages = models.IntegerField(default=0)
    
    status = models.CharField(max_length=50, default='PROCESSING') 
    error_message = models.TextField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    def __str__(self):
        return self.original_filename