import uuid
from django.db import models
from django.contrib.auth.models import User

class Department(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, unique=True, verbose_name="ชื่อแผนก (Provider)")
    description = models.TextField(blank=True, null=True, verbose_name="รายละเอียด")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name

class Folder(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255, verbose_name="ชื่อโฟลเดอร์")
    parent = models.ForeignKey('self', on_delete=models.CASCADE, null=True, blank=True, related_name='subfolders')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name

class KnowledgeCatalog(models.Model):
    STATUS_CHOICES = [
        ('PENDING', 'Pending'),
        ('PROCESSING', 'Processing'),
        ('PARTIAL_READY', 'Partial Ready'),
        ('WAITING_VLM_APPROVAL', 'Waiting VLM Approval'),
        ('PROCESSING_VLM', 'Processing VLM'), # เพิ่มสถานะตาม Admin action
        ('COMPLETED', 'Completed'),
        ('FAILED', 'Failed'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    original_filename = models.CharField(max_length=500)
    safe_filename = models.CharField(max_length=500)
    file_type = models.CharField(max_length=50)
    
    # เชื่อมโยง Department (Provider) และ Folder
    provider = models.CharField(max_length=255, blank=True, null=True) # เก็บชื่อ string ไว้ทำ Path
    department = models.ForeignKey(Department, on_delete=models.SET_NULL, null=True, blank=True, related_name='documents')
    folder = models.ForeignKey(Folder, on_delete=models.SET_NULL, null=True, blank=True, related_name='documents')
    
    category = models.CharField(max_length=255, blank=True, null=True)
    raw_storage_path = models.TextField(blank=True, null=True)
    parquet_storage_path = models.TextField(blank=True, null=True)
    
    total_pages = models.IntegerField(default=0)
    normal_pages = models.IntegerField(default=0)
    vlm_pages = models.IntegerField(default=0)
    pending_vlm_pages = models.IntegerField(default=0)

    auto_vlm = models.BooleanField(default=False)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='PENDING')
    error_message = models.TextField(blank=True, null=True)
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(blank=True, null=True)

    def __str__(self):
        return self.original_filename