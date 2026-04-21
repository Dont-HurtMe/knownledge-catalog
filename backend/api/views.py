import os
import re
import uuid
import boto3
import httpx
import yaml
from django.utils import timezone
from django.db.models import Q
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser, JSONParser
from rest_framework.permissions import AllowAny
from rest_framework.pagination import PageNumberPagination
from botocore.exceptions import ClientError
from botocore.client import Config

from .models import KnowledgeCatalog, Department, Folder
from .serializers import (
    KnowledgeCatalogSerializer, 
    UploadDocumentSerializer, 
    CompleteCatalogSerializer, 
    FailCatalogSerializer,
    PartialReadySerializer,
    PauseVlmSerializer,
    DepartmentSerializer,
    FolderSerializer,
    FileRenameSerializer,
    FileMoveSerializer
)

VERIFY_SSL = os.environ.get('VERIFY_SSL', 'True').lower() in ('true', '1', 't')

def load_schema_with_env(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()        
        def env_replacer(match):
            var_name = match.group(1)
            return os.environ.get(var_name, "knowledge-base")
        expanded_content = re.sub(r'\$\{([^}]+)\}', env_replacer, content)
        return yaml.safe_load(expanded_content)
    except Exception:
        return {}

SCHEMA = load_schema_with_env('/app/schema.yaml')

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

class StandardResultsSetPagination(PageNumberPagination):
    page_size = 20
    page_size_query_param = 'page_size'
    max_page_size = 100

class DepartmentViewSet(viewsets.ModelViewSet):
    queryset = Department.objects.all().order_by('-created_at')
    serializer_class = DepartmentSerializer
    authentication_classes = []
    permission_classes = [AllowAny]

class FolderViewSet(viewsets.ModelViewSet):
    queryset = Folder.objects.all().order_by('-created_at')
    serializer_class = FolderSerializer
    authentication_classes = []
    permission_classes = [AllowAny]

class ReportViewSet(viewsets.ViewSet):
    authentication_classes = []
    permission_classes = [AllowAny]
    
    def list(self, request):
        return Response({"status": "success", "data": []})

    def create(self, request):
        return Response({"status": "success", "message": "Report generated"})

class KnowledgeCatalogViewSet(viewsets.ModelViewSet):
    queryset = KnowledgeCatalog.objects.all().order_by('-created_at')
    serializer_class = KnowledgeCatalogSerializer
    parser_classes = [JSONParser, MultiPartParser, FormParser]
    pagination_class = StandardResultsSetPagination
    authentication_classes = []
    permission_classes = [AllowAny]
    
    def get_permissions(self):
        return [AllowAny()]

    def get_queryset(self):
        return KnowledgeCatalog.objects.all().order_by('-created_at')

    def create(self, request, *args, **kwargs):
        serializer = UploadDocumentSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        file_obj = request.FILES.get('file')
        if not file_obj:
            return Response({"error": "No file provided"}, status=status.HTTP_400_BAD_REQUEST)

        provider_name = serializer.validated_data.get('provider', 'unknown')
        auto_vlm = serializer.validated_data.get('auto_vlm', False)
        dept_obj, _ = Department.objects.get_or_create(name=provider_name)
        
        folder_id = request.data.get('folder_id')
        folder_obj = Folder.objects.filter(id=folder_id).first() if folder_id else None
            
        new_doc_id = uuid.uuid4()
        safe_filename = get_safe_filename(file_obj.name, new_doc_id)
        ext = os.path.splitext(safe_filename)[1].lower()
        
        file_group = "others"
        for group, extensions in SCHEMA.get('storage', {}).get('minio', {}).get('file_groups', {}).items():
            if ext in extensions:
                file_group = group
                break
                
        raw_path = f"raw/{file_group}/{provider_name}/{safe_filename}"
        s3_endpoint = os.environ.get('S3_ENDPOINT_URL')
        s3_access = os.environ.get('S3_ACCESS_KEY')
        s3_secret = os.environ.get('S3_SECRET_KEY')
        s3_bucket = os.environ.get('S3_BUCKET_NAME', 'knowledge-base')
        
        s3 = get_storage_client(s3_endpoint, s3_access, s3_secret)
        
        try:
            s3.head_bucket(Bucket=s3_bucket)
        except ClientError:
            try:
                s3.create_bucket(Bucket=s3_bucket)
            except ClientError as e:
                return Response({"error": f"Storage Error: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        s3.upload_fileobj(file_obj, s3_bucket, raw_path)
        
        real_user = request.user if request.user and request.user.is_authenticated else None
        
        catalog = KnowledgeCatalog.objects.create(
            id=new_doc_id,
            original_filename=file_obj.name,
            safe_filename=safe_filename,
            file_type=ext[1:].upper() if ext else 'UNKNOWN',
            provider=provider_name,      
            department=dept_obj,         
            folder=folder_obj,           
            raw_storage_path=raw_path,
            status='PROCESSING',
            auto_vlm=auto_vlm,
            user=real_user
        )
        
        payload = {
            "doc_id": str(catalog.id), 
            "raw_storage_path": raw_path, 
            "provider": provider_name, 
            "original_filename": file_obj.name,                 
            "user_id": str(real_user.id) if real_user else "0", 
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
            print(f"FASTAPI WORKER ERROR: {e}")
            
        return Response({"status": "success", "doc_id": str(catalog.id), "saved_as": safe_filename}, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = self.get_serializer(instance)
        data = serializer.data
        download_url = None
        
        if instance.raw_storage_path:
            s3_ep = getattr(instance, 's3_endpoint', None) or os.environ.get('S3_ENDPOINT_URL')
            s3_ak = getattr(instance, 's3_access_key', None) or os.environ.get('S3_ACCESS_KEY')
            s3_sk = getattr(instance, 's3_secret_key', None) or os.environ.get('S3_SECRET_KEY')
            s3_bn = getattr(instance, 's3_bucket_name', None) or SCHEMA.get('storage', {}).get('minio', {}).get('bucket_name', 'knowledge-base')
            
            if s3_ep and s3_ak:
                external_ep = s3_ep.replace("minio:9000", "localhost:9000") if "minio:9000" in s3_ep else s3_ep
                s3_external = get_storage_client(external_ep, s3_ak, s3_sk)
                try:
                    download_url = s3_external.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': s3_bn, 
                            'Key': instance.raw_storage_path,
                            'ResponseContentType': 'application/pdf',
                            'ResponseContentDisposition': 'inline'
                        },
                        ExpiresIn=3600
                    )
                    if download_url and "minio:9000" in download_url:
                        download_url = download_url.replace("minio:9000", "localhost:9000")
                except Exception:
                    pass
        data['download_url'] = download_url
        return Response(data)

    @action(detail=False, methods=['get'], permission_classes=[AllowAny])
    def cite(self, request):
        file_name = request.query_params.get('file_name')
        if not file_name:
            return Response({"error": "Missing file_name parameter"}, status=status.HTTP_400_BAD_REQUEST)
        
        doc = KnowledgeCatalog.objects.filter(
            Q(safe_filename=file_name) | Q(original_filename=file_name)
        ).first()
        
        if not doc:
            return Response({"error": "Document not found"}, status=status.HTTP_404_NOT_FOUND)
            
        serializer = self.get_serializer(doc)
        data = serializer.data
        download_url = None
        
        raw_storage_path = getattr(doc, 'raw_storage_path', None)
        
        if raw_storage_path:
            s3_ep = getattr(doc, 's3_endpoint', None) or os.environ.get('S3_ENDPOINT_URL')
            s3_ak = getattr(doc, 's3_access_key', None) or os.environ.get('S3_ACCESS_KEY')
            s3_sk = getattr(doc, 's3_secret_key', None) or os.environ.get('S3_SECRET_KEY')
            s3_bn = getattr(doc, 's3_bucket_name', None) or SCHEMA.get('storage', {}).get('minio', {}).get('bucket_name', 'knowledge-base')
            
            if s3_ep and s3_ak:
                external_ep = s3_ep.replace("minio:9000", "localhost:9000") if "minio:9000" in s3_ep else s3_ep
                s3_external = get_storage_client(external_ep, s3_ak, s3_sk)
                try:
                    download_url = s3_external.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': s3_bn, 
                            'Key': raw_storage_path,
                            'ResponseContentType': 'application/pdf',          
                            'ResponseContentDisposition': 'inline'
                        },
                        ExpiresIn=3600
                    )
                    if download_url and "minio:9000" in download_url:
                        download_url = download_url.replace("minio:9000", "localhost:9000")
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

    @action(detail=True, methods=['patch'])
    def rename(self, request, pk=None):
        file_obj = self.get_object()
        serializer = FileRenameSerializer(data=request.data)
        if serializer.is_valid():
            file_obj.original_filename = serializer.validated_data['new_name']
            file_obj.save()
            return Response({"status": "renamed", "new_name": file_obj.original_filename})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=False, methods=['post'])
    def move(self, request):
        serializer = FileMoveSerializer(data=request.data)
        if serializer.is_valid():
            file_ids = serializer.validated_data['file_ids']
            target_folder_id = serializer.validated_data.get('target_folder_id')
            
            target_folder = None
            if target_folder_id:
                try:
                    target_folder = Folder.objects.get(id=target_folder_id)
                except Folder.DoesNotExist:
                    return Response({"error": "Target folder not found"}, status=status.HTTP_404_NOT_FOUND)

            KnowledgeCatalog.objects.filter(id__in=file_ids).update(folder=target_folder)
            return Response({"status": "moved", "count": len(file_ids), "target_folder_id": target_folder_id})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)