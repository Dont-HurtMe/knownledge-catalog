import os, boto3, httpx
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import KnowledgeCatalog

S3_ENDPOINT = os.environ.get('S3_ENDPOINT_URL', 'http://minio:9000')
S3_ACCESS = os.environ.get('S3_ACCESS_KEY', 'admin')
S3_SECRET = os.environ.get('S3_SECRET_KEY', 'qwer1234')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'knowledge-base')
s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET)

@csrf_exempt
def upload_document(request):
    if request.method == 'POST' and request.FILES.get('file'):
        file_obj = request.FILES['file']
        provider = request.POST.get('provider', 'DITP')
        try:
            s3.head_bucket(Bucket=BUCKET_NAME)
        except Exception:
            s3.create_bucket(Bucket=BUCKET_NAME)

        catalog = KnowledgeCatalog.objects.create(
            original_filename=file_obj.name, file_type='PDF',
            provider=provider, file_hash=file_obj.name, status='PROCESSING'
        )
        minio_path = f"raw/pdf/{catalog.id}.pdf"
        s3.upload_fileobj(file_obj, BUCKET_NAME, minio_path)
        print(f"📦 Uploaded: {minio_path}")
        
        try:
            httpx.post('http://worker:8002/webhook/ingest', json={"doc_id": str(catalog.id), "minio_path": minio_path}, timeout=5)
        except Exception as e:
            print(f"⚠️ Failed to trigger worker: {e}")
            
        return JsonResponse({"status": "success", "doc_id": str(catalog.id)})
    return JsonResponse({"error": "Invalid request"}, status=400)
