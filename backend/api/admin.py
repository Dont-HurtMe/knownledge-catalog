import httpx
from django.contrib import admin
from django.contrib import messages
from unfold.admin import ModelAdmin
from unfold.decorators import action
from .models import KnowledgeCatalog

@admin.register(KnowledgeCatalog)
class KnowledgeCatalogAdmin(ModelAdmin):
    list_display = ['original_filename', 'provider', 'status', 'auto_vlm', 'pending_vlm_pages', 'created_at']
    list_filter = ['status', 'auto_vlm']
    search_fields = ['original_filename', 'provider']
    
    actions = ['approve_vlm']

    @action(description="Approve VLM Processing")
    def approve_vlm(self, request, queryset):
        waiting_catalogs = queryset.filter(status='WAITING_VLM_APPROVAL')
        approved_count = 0
        
        for catalog in waiting_catalogs:
            catalog.status = 'PROCESSING_VLM'
            catalog.save()
            
            payload = {
                "doc_id": str(catalog.id),
                "resume": True,
                "raw_storage_path": catalog.raw_storage_path,
                "original_filename": catalog.original_filename
            }
            try:
                httpx.post('http://worker:8002/webhook/resume_vlm', json=payload, timeout=5)
                approved_count += 1
            except Exception:
                pass
        
        if approved_count > 0:
            self.message_user(request, f"Approved {approved_count} items", level=messages.SUCCESS)
        else:
            self.message_user(request, "No items waiting for approval", level=messages.WARNING)