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
from botocore.client import Config

from .models import KnowledgeCatalog
from .serializers import (
    KnowledgeCatalogSerializer, 
    UploadDocumentSerializer, 
    CompleteCatalogSerializer, 
    FailCatalogSerializer,
    PartialReadySerializer,
    PauseVlmSerializer
)

VERIFY_SSL = os.environ.get('VERIFY_SSL', 'True').lower() in ('true', '1', 't')

with open('/app/schema.yaml', 'r', encoding='utf-8') as f:
    SCHEMA = yaml.safe_load(f)

def get_safe_filename(original_name: str, doc_id: uuid.UUID) -> str:
    name, ext = os.path.splitext(original_name)
    safe_name = re.sub(r'\s+', '_', name)
    safe_name = re.sub(r'[^\wก-๙.-]', '', safe_name)
    if not safe_name.strip('_'):
        safe_name = "document"
    safe_name = safe_name[:100]
    short_id = str(doc_id)[:8]
    return f"{safe_name}_{short_id}{ext}"

def get_storage_client(endpoint, access, secret):
    return boto3.client('s3',
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(signature_version='s3v4')
    )

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
        
        req_endpoint = serializer.validated_data.get('s3_endpoint')
        req_access = serializer.validated_data.get('s3_access_key')
        req_secret = serializer.validated_data.get('s3_secret_key')
        req_bucket = serializer.validated_data.get('s3_bucket_name')

        is_byos = bool(req_endpoint and req_access and req_secret and req_bucket)

        if is_byos:
            s3_endpoint = req_endpoint
            s3_access = req_access
            s3_secret = req_secret
            s3_bucket = req_bucket
        else:
            s3_endpoint = os.environ.get('S3_ENDPOINT_URL')
            s3_access = os.environ.get('S3_ACCESS_KEY')
            s3_secret = os.environ.get('S3_SECRET_KEY')
            s3_bucket = SCHEMA.get('storage', {}).get('minio', {}).get('bucket_name', 'knowledge-base')

        if not s3_endpoint:
            return Response({"error": "Storage configuration missing. Provide BYOS credentials."}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        real_user = request.user 
        new_doc_id = uuid.uuid4()
        safe_filename = get_safe_filename(file_obj.name, new_doc_id)
        ext = os.path.splitext(safe_filename)[1].lower()
        
        folder_type = "others"
        for group, extensions in SCHEMA.get('storage', {}).get('minio', {}).get('file_groups', {}).items():
            if ext in extensions:
                folder_type = group
                break
                
        raw_zone_template = SCHEMA.get('storage', {}).get('minio', {}).get('paths', {}).get('raw_zone', 'raw/{file_group}/{provider}/{safe_filename}')
        raw_minio_path = raw_zone_template.format(
            file_group=folder_type, provider=provider or 'unknown', safe_filename=safe_filename
        )
        
        s3 = get_storage_client(s3_endpoint, s3_access, s3_secret)
        
        try:
            s3.head_bucket(Bucket=s3_bucket)
        except Exception:
            try:
                s3.create_bucket(Bucket=s3_bucket)
            except ClientError as e:
                return Response({"error": f"Storage Error: Could not access or create bucket '{s3_bucket}'. {e}"}, status=status.HTTP_400_BAD_REQUEST)

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
            status='PROCESSING',
            s3_endpoint=s3_endpoint if is_byos else None,
            s3_access_key=s3_access if is_byos else None,
            s3_secret_key=s3_secret if is_byos else None,
            s3_bucket_name=s3_bucket if is_byos else None
        )
        
        s3.upload_fileobj(file_obj, s3_bucket, raw_minio_path)

        payload = {
            "doc_id": str(catalog.id), 
            "raw_storage_path": raw_minio_path, 
            "provider": provider or "", 
            "original_filename": file_obj.name,
            "user_id": str(real_user.id),
            "auto_vlm": auto_vlm,
            "s3_endpoint": s3_endpoint,
            "s3_access_key": s3_access,
            "s3_secret_key": s3_secret,
            "s3_bucket_name": s3_bucket
        }
        
        try:
            r = httpx.post('http://worker:8002/webhook/ingest', json=payload, timeout=5, verify=VERIFY_SSL)
            r.raise_for_status()
        except Exception as e:
            print(f"🚨 FASTAPI WORKER ERROR: {e}")
            
        return Response({"status": "success", "doc_id": str(catalog.id), "saved_as": safe_filename}, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        data = serializer.data
        download_url = None
        
        if instance.raw_storage_path:
            s3_ep = instance.s3_endpoint or os.environ.get('S3_ENDPOINT_URL')
            s3_ak = instance.s3_access_key or os.environ.get('S3_ACCESS_KEY')
            s3_sk = instance.s3_secret_key or os.environ.get('S3_SECRET_KEY')
            s3_bn = instance.s3_bucket_name or SCHEMA.get('storage', {}).get('minio', {}).get('bucket_name', 'knowledge-base')
            
            if s3_ep and s3_ak:
                s3 = get_storage_client(s3_ep, s3_ak, s3_sk)
                try:
                    download_url = s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': s3_bn, 'Key': instance.raw_storage_path},
                        ExpiresIn=3600
                    )
                except Exception:
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