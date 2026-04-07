import os, json, fitz, boto3, httpx, base64, re
import pandas as pd
import multiprocessing
import asyncio
import yaml
from typing import Optional 
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

fitz.TOOLS.mupdf_display_errors(False)

# ==========================================
# 1. Environment & Configuration
S3_ENDPOINT = os.environ.get('S3_ENDPOINT_URL', 'http://minio:9000')
S3_ACCESS = os.environ.get('S3_ACCESS_KEY', 'admin')
S3_SECRET = os.environ.get('S3_SECRET_KEY', 'qwer1234')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'knowledge-base')
DJANGO_API = os.environ.get('DJANGO_API_URL', 'http://backend:8001/api')
VERIFY_SSL = os.environ.get('VERIFY_SSL', 'True').lower() in ('true', '1', 't')

with open('/app/schema.yaml', 'r') as f:
    SCHEMA = yaml.safe_load(f)

# ==========================================
# 2. Custom Exceptions & Validator
class PDFValidationError(Exception): pass
class LowTextDensityError(PDFValidationError): pass
class BrokenFontError(PDFValidationError): pass
class BrokenThaiTextError(PDFValidationError): pass
class HighImageCoverageError(PDFValidationError): pass
class ComplexVectorTableError(PDFValidationError): pass

class PDFValidator:
    @staticmethod
    def check_image_coverage(page: fitz.Page):
        page_area = page.rect.get_area()
        if page_area <= 0: return
        images = page.get_image_info()
        if images:
            total_image_area = sum((img["bbox"][2] - img["bbox"][0]) * (img["bbox"][3] - img["bbox"][1]) 
                                   for img in images if "bbox" in img)
            if (total_image_area / page_area) > 0.60:
                raise HighImageCoverageError("High image coverage")

    @staticmethod
    def check_complex_tables(page: fitz.Page):
        if len(page.get_drawings()) > 100: 
            raise ComplexVectorTableError("Complex vector tables")

    @staticmethod
    def check_text_density(text: str):
        if not text or len(text.strip()) < 30: 
            raise LowTextDensityError("Low text density")

    @staticmethod
    def check_broken_font(text: str):
        pua_count = len(re.findall(r'[\uf700-\uf7ff]', text))
        ufffd_count = text.count("\ufffd")
        mojibake_count = len(re.findall(r'[\u00C0-\u024F]', text))
        ctrl_count = len(re.findall(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', text))
        if "(cid:" in text or ufffd_count > 1 or pua_count > 3 or mojibake_count > 2 or ctrl_count > 5:
            raise BrokenFontError("Broken Encoding")

    @staticmethod
    def check_broken_thai(text: str):
        broken_pattern = r'(?:\s[\u0e30-\u0e3a\u0e45\u0e47-\u0e4e])|(?:[\u0e32\u0e33\u0e40-\u0e44][\u0e31\u0e34-\u0e3a\u0e47-\u0e4e])'
        if len(re.findall(broken_pattern, text)) >= 1:
            raise BrokenThaiTextError("Broken Thai")

    @classmethod
    def validate_all(cls, page: fitz.Page, text: str):
        cls.check_image_coverage(page)
        cls.check_complex_tables(page)
        cls.check_text_density(text)
        cls.check_broken_font(text)
        cls.check_broken_thai(text)

# ==========================================
# 3. Core Functions
def get_s3_client():
    return boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET)

def save_temp_json(doc_id, page_num, data):
    s3 = get_s3_client()
    temp_zone_template = SCHEMA['storage']['minio']['paths']['temp_zone']
    minio_path = temp_zone_template.format(doc_id=doc_id, page_number=page_num)
    s3.put_object(Bucket=BUCKET_NAME, Key=minio_path, Body=json.dumps(data, ensure_ascii=False).encode('utf-8'))

# ==========================================
# 4. Prefect Tasks
@task
def vlm_page_task(doc_id: str, local_file_path: str, page_num: int, provider: str, original_filename: str, fallback_reason: str = "VLM_FALLBACK"):
    try:
        doc = fitz.open(local_file_path)
        page = doc[page_num]
        pix = page.get_pixmap()
        base64_image = base64.b64encode(pix.tobytes("jpeg")).decode('utf-8')
        
        response = httpx.post("http://vlm:8003/extract", json={"image_base64": base64_image}, timeout=60.0, verify=VERIFY_SSL)
        text_content = response.json().get("text", "")
        
        data = {
            "doc_id": doc_id, "original_filename": original_filename, 
            "provider": provider, "page_number": page_num + 1, 
            "strategy": fallback_reason, "text": text_content
        }
        save_temp_json(doc_id, page_num + 1, data)
        httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": page_num+1, "status": "SUCCESS", "strategy": fallback_reason}, verify=VERIFY_SSL)
    except Exception:
        httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": page_num+1, "status": "FAILED", "strategy": fallback_reason}, verify=VERIFY_SSL)

@task
def extract_page_task(doc_id: str, local_file_path: str, page_num: int, provider: str, original_filename: str):
    try:
        doc = fitz.open(local_file_path)
        page = doc[page_num]
        raw_text = page.get_text()
        
        try:
            PDFValidator.validate_all(page, raw_text)
            data = {"doc_id": doc_id, "original_filename": original_filename, "provider": provider, "page_number": page_num + 1, "strategy": "FITZ_RAW", "text": raw_text}
            save_temp_json(doc_id, page_num + 1, data)
            httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": page_num+1, "status": "SUCCESS", "strategy": "FITZ_RAW"}, verify=VERIFY_SSL)
            
        except PDFValidationError as e:
            error_reason = f"VLM_{e.__class__.__name__}".upper()
            ext = os.path.splitext(original_filename)[1].lower()
            file_groups = SCHEMA['storage']['minio']['file_groups']
            if ext in file_groups.get('pdf', []) or ext in file_groups.get('image', []):
                httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": page_num+1, "status": "PENDING_VLM", "strategy": "QUEUED_VLM"}, verify=VERIFY_SSL)
                vlm_page_task.submit(doc_id, local_file_path, page_num, provider, original_filename, error_reason)
            else:
                httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": page_num+1, "status": "FAILED", "strategy": "VLM_NOT_ALLOWED"}, verify=VERIFY_SSL)
                
    except Exception:
        httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": page_num+1, "status": "FAILED", "strategy": "ERROR"}, verify=VERIFY_SSL)

@task
def extract_text_file_task(doc_id: str, local_file_path: str, provider: str, original_filename: str):
    try:
        with open(local_file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        data = {"doc_id": doc_id, "original_filename": original_filename, "provider": provider, "page_number": 1, "strategy": "PLAIN_TEXT", "text": text}
        save_temp_json(doc_id, 1, data)
        httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": 1, "status": "SUCCESS", "strategy": "PLAIN_TEXT"}, verify=VERIFY_SSL)
    except Exception:
        httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": 1, "status": "FAILED", "strategy": "ERROR"}, verify=VERIFY_SSL)

# ==========================================
# 5. Prefect Flows

@flow(name="Document_Router_Flow", task_runner=ConcurrentTaskRunner())
def ingest_document_flow(doc_id: str, raw_storage_path: str, provider: str, original_filename: str):
    s3 = get_s3_client()
    ext = os.path.splitext(original_filename)[1].lower()
    local_path = f"/tmp/{doc_id}{ext}"
    s3.download_file(BUCKET_NAME, raw_storage_path, local_path)
    
    file_groups = SCHEMA['storage']['minio']['file_groups']
    
    if ext in file_groups.get('pdf', []) or ext in file_groups.get('image', []):
        doc = fitz.open(local_path)
        total_pages = len(doc)
        httpx.post(f"{DJANGO_API}/catalog/{doc_id}/init/", json={"total_pages": total_pages}, verify=VERIFY_SSL)
        for p in range(total_pages):
            extract_page_task.submit(doc_id, local_path, p, provider, original_filename)
            
    elif ext in file_groups.get('text', []):
        httpx.post(f"{DJANGO_API}/catalog/{doc_id}/init/", json={"total_pages": 1}, verify=VERIFY_SSL)
        extract_text_file_task.submit(doc_id, local_path, provider, original_filename)
        
    else:
        httpx.post(f"{DJANGO_API}/catalog/{doc_id}/init/", json={"total_pages": 1}, verify=VERIFY_SSL)
        httpx.post(f"{DJANGO_API}/transaction/{doc_id}/update/", json={"page_number": 1, "status": "FAILED", "strategy": "UNSUPPORTED_FORMAT"}, verify=VERIFY_SSL)

@flow(name="Parquet_Aggregator_Flow")
def aggregate_flow(doc_id: str, provider: str, original_filename: str, category: Optional[str] = None):
    try:
        s3 = get_s3_client()
        temp_zone_template = SCHEMA['storage']['minio']['paths']['temp_zone']
        temp_prefix = temp_zone_template.split('{page_number}')[0].format(doc_id=doc_id)
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=temp_prefix)
        data_list = []
        for obj in objects.get('Contents', []):
            res = s3.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
            data_list.append(json.loads(res['Body'].read().decode('utf-8')))
        if not data_list:
            print(f"⚠️ No temp data found for {doc_id}")
            return
        df = pd.DataFrame(data_list)
        if not df.empty and 'page_number' in df.columns:
            df = df.sort_values('page_number')
        
        extracted_zone_template = SCHEMA['storage']['minio']['paths']['extracted_zone']
        parquet_storage_path = extracted_zone_template.format(provider=provider, doc_id=doc_id)
        
        local_parquet_path = f"/tmp/{doc_id}.parquet"
        df.to_parquet(local_parquet_path, index=False)
        s3.upload_file(local_parquet_path, BUCKET_NAME, parquet_storage_path)
        
        if os.path.exists(local_parquet_path):
            os.remove(local_parquet_path)
        
        base_extract_path = os.path.dirname(parquet_storage_path)
        metadata = {
            "doc_id": doc_id, 
            "original_filename": original_filename, 
            "provider": provider,
            "category": category, 
            "parquet_storage_path": parquet_storage_path,
            "total_pages": len(df), 
            "vlm_pages": len(df[df['strategy'].str.contains('VLM', na=False)]) if 'strategy' in df.columns else 0
        }
        s3.put_object(Bucket=BUCKET_NAME, Key=f"{base_extract_path}/metadata.json", Body=json.dumps(metadata, ensure_ascii=False).encode('utf-8'))
        delete_keys = [{'Key': obj['Key']} for obj in objects.get('Contents', [])]
        if delete_keys:
            s3.delete_objects(Bucket=BUCKET_NAME, Delete={'Objects': delete_keys})
            
        httpx.post(f"{DJANGO_API}/catalog/{doc_id}/complete/", verify=VERIFY_SSL)
    except Exception as e:
        print(f"❌ Aggregate Flow Failed for {doc_id}: {e}")
        raise e

# ==========================================
# 6. FastAPI Webhooks

app = FastAPI(title="Event-Driven Worker")

class IngestPayload(BaseModel):
    doc_id: str
    raw_storage_path: Optional[str] = None
    minio_path: Optional[str] = None 
    provider: str
    original_filename: str

class AggregatePayload(BaseModel):
    doc_id: str
    provider: str
    original_filename: str
    category: Optional[str] = None 

def run_flow_in_thread(flow_fn, *args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        flow_fn(*args)
    finally:
        loop.close()

@app.post("/webhook/ingest")
def trigger_ingestion(payload: IngestPayload):
    target_path = payload.raw_storage_path if payload.raw_storage_path else payload.minio_path
    p = multiprocessing.Process(
        target=run_flow_in_thread, 
        args=(ingest_document_flow, payload.doc_id, target_path, payload.provider, payload.original_filename)
    )
    p.start()
    return {"status": "Router Started"}

@app.post("/webhook/aggregate")
def trigger_aggregate(payload: AggregatePayload):
    p = multiprocessing.Process(
        target=run_flow_in_thread, 
        args=(aggregate_flow, payload.doc_id, payload.provider, payload.original_filename, payload.category)
    )
    p.start()
    return {"status": "Aggregator Started"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)