import os
import re
import uuid
import boto3
import httpx
import yaml
from django.utils import timezone
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.permissions import AllowAny
from botocore.exceptions import ClientError

from .models import KnowledgeCatalog
from .serializers import (
    KnowledgeCatalogSerializer, 
    UploadDocumentSerializer, 
    CompleteCatalogSerializer, 
    FailCatalogSerializer,
    PartialReadySerializer,
    PauseVlmSerializer
)

S3_ENDPOINT = os.environ.get('S3_ENDPOINT_URL', 'http://minio:9000')
S3_ACCESS = os.environ.get('S3_ACCESS_KEY', 'admin')
S3_SECRET = os.environ.get('S3_SECRET_KEY', 'qwer1234')
VERIFY_SSL = os.environ.get('VERIFY_SSL', 'True').lower() in ('true', '1', 't')

with open('/app/schema.yaml', 'r', encoding='utf-8') as f:
    SCHEMA = yaml.safe_load(f)

BUCKET_NAME = SCHEMA['storage']['minio']['bucket_name']
s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET)

def get_safe_filename(original_name: str, doc_id: uuid.UUID) -> str:
    name, ext = os.path.splitext(original_name)
    safe_name = re.sub(r'\s+', '_', name)
    safe_name = re.sub(r'[^\wก-๙.-]', '', safe_name)
    if not safe_name.strip('_'):
        safe_name = "document"
    safe_name = safe_name[:100]
    short_id = str(doc_id)[:8]
    return f"{safe_name}_{short_id}{ext}"

class KnowledgeCatalogViewSet(viewsets.ModelViewSet):
    serializer_class = KnowledgeCatalogSerializer
    parser_classes = [JSONParser, MultiPartParser, FormParser]

    def get_queryset(self):
        queryset = KnowledgeCatalog.objects.all().order_by('-created_at')
        if self.action in ['complete', 'fail', 'partial_ready', 'pause_for_approval']:
            return queryset
        user = self.request.user
        if not user or not user.is_authenticated:
            return queryset.none()
        if user.is_staff or user.is_superuser:
            return queryset
        return queryset.filter(user=user)

    def create(self, request, *args, **kwargs):
        serializer = UploadDocumentSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        file_obj = request.FILES.get('file')
        if not file_obj:
            return Response({"error": "No file provided"}, status=status.HTTP_400_BAD_REQUEST)

        provider = serializer.validated_data.get('provider')
        category = serializer.validated_data.get('category')
        auto_vlm = serializer.validated_data.get('auto_vlm', False)
        real_user = request.user 
        new_doc_id = uuid.uuid4()
        safe_filename = get_safe_filename(file_obj.name, new_doc_id)
        ext = os.path.splitext(safe_filename)[1].lower()
        
        folder_type = "others"
        for group, extensions in SCHEMA['storage']['minio']['file_groups'].items():
            if ext in extensions:
                folder_type = group
                break
                
        raw_zone_template = SCHEMA['storage']['minio']['paths']['raw_zone']
        raw_minio_path = raw_zone_template.format(
            file_group=folder_type, provider=provider, safe_filename=safe_filename
        )
        
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
        except Exception:
            s3.create_bucket(Bucket=BUCKET_NAME)

        catalog = KnowledgeCatalog.objects.create(
            id=new_doc_id,
            original_filename=file_obj.name,
            safe_filename=safe_filename,
            raw_storage_path=raw_minio_path,
            file_type=ext[1:].upper() if ext else 'UNKNOWN',
            provider=provider,
            category=category,
            auto_vlm=auto_vlm,
            user=real_user, 
            status='PROCESSING'
        )
        
        s3.upload_fileobj(file_obj, BUCKET_NAME, raw_minio_path)

        payload = {
            "doc_id": str(catalog.id), 
            "raw_storage_path": raw_minio_path, 
            "provider": provider, 
            "original_filename": file_obj.name,
            "user_id": str(real_user.id),
            "auto_vlm": auto_vlm
        }
        
        try:
            r = httpx.post('http://worker:8002/webhook/ingest', json=payload, timeout=5, verify=VERIFY_SSL)
            r.raise_for_status()
        except Exception as e:
            print(f"WORKER ERROR: {e}")
            
        return Response({"status": "success", "doc_id": str(catalog.id), "saved_as": safe_filename}, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        data = serializer.data
        download_url = None
        if instance.raw_storage_path:
            try:
                download_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': BUCKET_NAME, 'Key': instance.raw_storage_path},
                    ExpiresIn=3600
                )
            except ClientError:
                pass
        data['download_url'] = download_url
        return Response(data)

    @action(detail=True, methods=['post'], permission_classes=[AllowAny])
    def partial_ready(self, request, pk=None):
        instance = self.get_object()
        serializer = PartialReadySerializer(data=request.data)
        if serializer.is_valid():
            instance.status = 'PARTIAL_READY'
            instance.normal_pages = serializer.validated_data.get('normal_pages', 0)
            instance.parquet_storage_path = serializer.validated_data.get('parquet_storage_path')
            instance.save()
            return Response({"status": "partial_ready"})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'], permission_classes=[AllowAny])
    def pause_for_approval(self, request, pk=None):
        instance = self.get_object()
        serializer = PauseVlmSerializer(data=request.data)
        if serializer.is_valid():
            instance.status = 'WAITING_VLM_APPROVAL'
            instance.pending_vlm_pages = serializer.validated_data.get('pending_vlm_pages', 0)
            instance.normal_pages = serializer.validated_data.get('normal_pages', 0)
            instance.parquet_storage_path = serializer.validated_data.get('parquet_storage_path')
            instance.save()
            return Response({"status": "paused_waiting_approval"})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'], permission_classes=[AllowAny])
    def complete(self, request, pk=None):
        instance = self.get_object()
        serializer = CompleteCatalogSerializer(data=request.data)
        if serializer.is_valid():
            instance.status = 'COMPLETED'
            instance.total_pages = serializer.validated_data.get('total_pages', 0)
            if serializer.validated_data.get('normal_pages', 0) > 0:
                instance.normal_pages = serializer.validated_data.get('normal_pages')
            instance.vlm_pages = serializer.validated_data.get('vlm_pages', 0)
            if serializer.validated_data.get('parquet_storage_path'):
                instance.parquet_storage_path = serializer.validated_data.get('parquet_storage_path')
            instance.completed_at = timezone.now()
            instance.save()
            return Response({"status": "completed"})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['post'], permission_classes=[AllowAny])
    def fail(self, request, pk=None):
        instance = self.get_object()
        serializer = FailCatalogSerializer(data=request.data)
        if serializer.is_valid():
            instance.status = 'FAILED'
            instance.error_message = serializer.validated_data.get('error_message')
            instance.completed_at = timezone.now()
            instance.save()
            return Response({"status": "failed_recorded"})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)