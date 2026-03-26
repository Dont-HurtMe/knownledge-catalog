from django.contrib import admin
from .models import KnowledgeCatalog, KnowledgeTransaction

@admin.register(KnowledgeCatalog)
class KnowledgeCatalogAdmin(admin.ModelAdmin):
    list_display = ('original_filename', 'provider', 'status', 'total_pages', 'created_at')
    list_filter = ('status', 'provider')
    search_fields = ('original_filename',)

@admin.register(KnowledgeTransaction)
class KnowledgeTransactionAdmin(admin.ModelAdmin):
    list_display = ('catalog', 'page_number', 'strategy', 'status')
    list_filter = ('status', 'strategy')
    search_fields = ('catalog__original_filename',)
