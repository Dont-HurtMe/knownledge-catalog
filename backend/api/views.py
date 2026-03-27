import os
import re
import uuid
import boto3
import httpx
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import KnowledgeCatalog, KnowledgeTransaction
from botocore.exceptions import ClientError

S3_ENDPOINT = os.environ.get('S3_ENDPOINT_URL', 'http://minio:9000')
S3_ACCESS = os.environ.get('S3_ACCESS_KEY', 'admin')
S3_SECRET = os.environ.get('S3_SECRET_KEY', 'qwer1234')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'knowledge-base')
VERIFY_SSL = os.environ.get('VERIFY_SSL', 'True').lower() in ('true', '1', 't')

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

@csrf_exempt
def upload_document(request):
    if request.method == 'POST' and request.FILES.get('file'):
        file_obj = request.FILES['file']
        provider = request.POST.get('provider', 'DITP')
        category = request.POST.get('category', None)
        new_doc_id = uuid.uuid4()
        
        safe_filename = get_safe_filename(file_obj.name, new_doc_id)
        ext = os.path.splitext(safe_filename)[1].lower()
        
        if ext == ".pdf":
            folder_type = "pdf"
        elif ext in [".txt", ".md"]:
            folder_type = "text"
        elif ext in [".png", ".jpg", ".jpeg"]:
            folder_type = "image"
        else:
            folder_type = "others"
            
        raw_minio_path = f"raw/{folder_type}/{provider}/{safe_filename}"
        
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
        except Exception:
            pass
            
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
            filename_no_ext = os.path.splitext(catalog.safe_filename)[0]
            parquet_minio_path = f"extracted/{catalog.provider}/{filename_no_ext}/extracted_content.parquet"
            
            catalog.status = 'AGGREGATING'
            catalog.parquet_storage_path = parquet_minio_path
            catalog.save()
            
            payload = {
                "doc_id": str(catalog.id), 
                "provider": catalog.provider, 
                "original_filename": catalog.original_filename,
                "parquet_storage_path": parquet_minio_path
            }
            try:
                httpx.post('http://worker:8002/webhook/aggregate', json=payload, timeout=5, verify=VERIFY_SSL)
            except Exception:
                pass

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