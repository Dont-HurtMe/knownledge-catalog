from rest_framework import serializers
from .models import KnowledgeCatalog, Department, Folder

class DepartmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Department
        fields = '__all__'

class FolderSerializer(serializers.ModelSerializer):
    class Meta:
        model = Folder
        fields = '__all__'

class KnowledgeCatalogSerializer(serializers.ModelSerializer):
    class Meta:
        model = KnowledgeCatalog
        fields = '__all__'

class UploadDocumentSerializer(serializers.Serializer):
    provider = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    category = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    auto_vlm = serializers.BooleanField(default=False)
    s3_endpoint = serializers.CharField(required=False, allow_null=True)
    s3_access_key = serializers.CharField(required=False, allow_null=True)
    s3_secret_key = serializers.CharField(required=False, allow_null=True)
    s3_bucket_name = serializers.CharField(required=False, allow_null=True)

class PartialReadySerializer(serializers.Serializer):
    normal_pages = serializers.IntegerField(default=0)
    parquet_storage_path = serializers.CharField(required=False, allow_blank=True, allow_null=True)

class PauseVlmSerializer(serializers.Serializer):
    pending_vlm_pages = serializers.IntegerField(default=0)
    normal_pages = serializers.IntegerField(default=0)
    parquet_storage_path = serializers.CharField(required=False, allow_blank=True, allow_null=True)

class CompleteCatalogSerializer(serializers.Serializer):
    total_pages = serializers.IntegerField(default=0)
    normal_pages = serializers.IntegerField(default=0)
    vlm_pages = serializers.IntegerField(default=0)
    parquet_storage_path = serializers.CharField(required=False, allow_blank=True, allow_null=True)

class FailCatalogSerializer(serializers.Serializer):
    error_message = serializers.CharField(required=True)

class FileRenameSerializer(serializers.Serializer):
    new_name = serializers.CharField(required=True)

class FileMoveSerializer(serializers.Serializer):
    file_ids = serializers.ListField(child=serializers.UUIDField())
    target_folder_id = serializers.CharField(required=False, allow_null=True)