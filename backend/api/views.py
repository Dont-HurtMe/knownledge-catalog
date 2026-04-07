import os
import re
import uuid
import boto3
import httpx
import json
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import KnowledgeCatalog, KnowledgeTransaction
from botocore.exceptions import ClientError

# ==========================================
# 1. Environment & Initialization

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

# ==========================================
# 2. API Endpoints

@csrf_exempt
def upload_document(request):
    if request.method == 'POST' and request.FILES.get('file'):
        file_obj = request.FILES['file']
        provider = request.POST.get('provider', 'DITP')
        category = request.POST.get('category', None)
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
            status='PROCESSING'
        )
        
        s3.upload_fileobj(file_obj, BUCKET_NAME, raw_minio_path)
        
        payload = {
            "doc_id": str(catalog.id), 
            "raw_storage_path": raw_minio_path, 
            "provider": provider, 
            "original_filename": file_obj.name
        }
        
        try:
            httpx.post('http://worker:8002/webhook/ingest', json=payload, timeout=5, verify=VERIFY_SSL)
        except Exception as e:
            print(f"⚠️ Trigger ingest worker failed: {e}")
            
        return JsonResponse({"status": "success", "doc_id": str(catalog.id), "saved_as": safe_filename})
    return JsonResponse({"error": "Invalid request"}, status=400)

@csrf_exempt
def init_pages(request, doc_id):
    if request.method == 'POST':
        data = json.loads(request.body)
        total_pages = data.get('total_pages', 0)
        catalog = KnowledgeCatalog.objects.get(id=doc_id)
        catalog.total_pages = total_pages
        catalog.save()
        transactions = [KnowledgeTransaction(catalog=catalog, page_number=i+1) for i in range(total_pages)]
        KnowledgeTransaction.objects.bulk_create(transactions)
        return JsonResponse({"status": "initialized"})

@csrf_exempt
def update_transaction(request, doc_id):
    if request.method == 'POST':
        data = json.loads(request.body)
        page_number = data.get('page_number')
        status = data.get('status')
        strategy = data.get('strategy')
        
        catalog = KnowledgeCatalog.objects.get(id=doc_id)
        KnowledgeTransaction.objects.filter(catalog=catalog, page_number=page_number).update(status=status, strategy=strategy)
        
        success_count = KnowledgeTransaction.objects.filter(catalog=catalog, status='SUCCESS').count()
        failed_count = KnowledgeTransaction.objects.filter(catalog=catalog, status='FAILED').count()
        
        if (success_count + failed_count) == catalog.total_pages and catalog.total_pages > 0 and catalog.status != 'AGGREGATING':
            extracted_zone_template = SCHEMA['storage']['minio']['paths']['extracted_zone']
            parquet_minio_path = extracted_zone_template.format(provider=catalog.provider, doc_id=str(catalog.id))
            
            catalog.status = 'AGGREGATING'
            catalog.parquet_storage_path = parquet_minio_path
            catalog.save()
            
            payload = {
                "doc_id": str(catalog.id), 
                "provider": catalog.provider, 
                "original_filename": catalog.original_filename,
                "category": catalog.category 
            }
            try:
                httpx.post('http://worker:8002/webhook/aggregate', json=payload, timeout=10, verify=VERIFY_SSL)
            except Exception as e:
                print(f"⚠️ Trigger aggregate worker failed: {e}")

        return JsonResponse({"status": "updated"})
@csrf_exempt
def complete_catalog(request, doc_id):
    if request.method == 'POST':
        KnowledgeCatalog.objects.filter(id=doc_id).update(status='COMPLETED')
        return JsonResponse({"status": "completed"})

@csrf_exempt
def get_document(request, doc_id):
    if request.method == 'GET':
        try:
            catalog = KnowledgeCatalog.objects.get(id=doc_id)
            download_url = None
            if catalog.raw_storage_path:
                try:
                    download_url = s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': BUCKET_NAME, 'Key': catalog.raw_storage_path},
                        ExpiresIn=3600
                    )
                except ClientError:
                    pass

            return JsonResponse({
                "doc_id": str(catalog.id),
                "original_filename": catalog.original_filename,
                "safe_filename": catalog.safe_filename,
                "provider": catalog.provider,
                "category": catalog.category,
                "status": catalog.status,
                "total_pages": catalog.total_pages,
                "raw_storage_path": catalog.raw_storage_path,
                "parquet_storage_path": catalog.parquet_storage_path,
                "download_url": download_url
            })
        except KnowledgeCatalog.DoesNotExist:
            return JsonResponse({"error": "Document not found"}, status=404)
    return JsonResponse({"error": "Method not allowed"}, status=405)