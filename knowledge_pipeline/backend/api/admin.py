from django.contrib import admin
from .models import KnowledgeCatalog, KnowledgeTransaction

@admin.register(KnowledgeCatalog)
class KnowledgeCatalogAdmin(admin.ModelAdmin):
    list_display = ('original_filename', 'file_type', 'provider', 'status', 'total_units', 'created_at')
    list_filter = ('status', 'file_type', 'provider')
    search_fields = ('original_filename', 'file_hash')

@admin.register(KnowledgeTransaction)
class KnowledgeTransactionAdmin(admin.ModelAdmin):
    list_display = ('catalog', 'unit_reference', 'strategy', 'status')
    list_filter = ('status', 'strategy')
    search_fields = ('catalog__original_filename', 'unit_reference')
