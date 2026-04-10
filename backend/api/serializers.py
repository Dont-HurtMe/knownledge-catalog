from rest_framework import serializers
from .models import KnowledgeCatalog

class KnowledgeCatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = KnowledgeCatalog
        fields = '__all__'

class UploadDocumentSerializer(serializers.Serializer):
    provider = serializers.CharField(max_length=100, required=False, allow_blank=True)
    category = serializers.CharField(max_length=100, required=False, allow_blank=True)
    auto_vlm = serializers.BooleanField(default=False)
    
    s3_endpoint = serializers.CharField(max_length=255, required=False, allow_blank=True)
    s3_access_key = serializers.CharField(max_length=255, required=False, allow_blank=True)
    s3_secret_key = serializers.CharField(max_length=255, required=False, allow_blank=True)
    s3_bucket_name = serializers.CharField(max_length=255, required=False, allow_blank=True)

class PartialReadySerializer(serializers.Serializer):
    normal_pages = serializers.IntegerField(default=0)
    parquet_storage_path = serializers.CharField(required=False, allow_null=True, allow_blank=True)

class PauseVlmSerializer(serializers.Serializer):
    pending_vlm_pages = serializers.IntegerField(required=True)
    normal_pages = serializers.IntegerField(default=0)
    parquet_storage_path = serializers.CharField(required=False, allow_null=True, allow_blank=True)

class CompleteCatalogSerializer(serializers.Serializer):
    total_pages = serializers.IntegerField(default=0)
    normal_pages = serializers.IntegerField(default=0)
    vlm_pages = serializers.IntegerField(default=0)
    parquet_storage_path = serializers.CharField(required=False, allow_null=True, allow_blank=True)

class FailCatalogSerializer(serializers.Serializer):
    error_message = serializers.CharField()