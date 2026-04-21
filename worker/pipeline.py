import os
import re
import base64
import boto3
import httpx
import fitz
import pandas as pd
from io import BytesIO
from prefect import flow, task, serve
from botocore.client import Config

DJANGO_API = os.environ.get("DJANGO_API_URL", "http://backend:8001/api/catalog")
VLM_API_URL = os.environ.get("VLM_API_URL", "http://vlm:8003/extract")

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

def get_s3_client(creds: dict):
    return boto3.client('s3',
        endpoint_url=creds['s3_endpoint'],
        aws_access_key_id=creds['s3_access_key'],
        aws_secret_access_key=creds['s3_secret_key'],
        config=Config(signature_version='s3v4')
    )

@task
def download_file(raw_storage_path: str, creds: dict) -> bytes:
    s3 = get_s3_client(creds)
    obj = s3.get_object(Bucket=creds['s3_bucket_name'], Key=raw_storage_path)
    return obj["Body"].read()

@task
def extract_and_split(file_bytes: bytes, filename: str):
    text_data = []
    vlm_pages = []
    ext = filename.split('.')[-1].lower() if '.' in filename else ''
    
    if ext == 'txt':
        text = file_bytes.decode('utf-8', errors='ignore')
        text_data.append({"page_number": 1, "text": text})
        return text_data, vlm_pages, 1
        
    elif ext in ['png', 'jpg', 'jpeg']:
        vlm_pages.append(1)
        return text_data, vlm_pages, 1
        
    else:
        try:
            doc = fitz.open(stream=file_bytes, filetype="pdf")
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                text = page.get_text("text").strip()
                try:
                    PDFValidator.validate_all(page, text)
                    text_data.append({"page_number": page_num + 1, "text": text})
                except PDFValidationError:
                    vlm_pages.append(page_num + 1)
            return text_data, vlm_pages, len(doc)
        except Exception:
            return [], [], 0

@task
def save_parquet(data: list, doc_id: str, part_name: str, creds: dict) -> str:
    if not data: return ""
    df = pd.DataFrame(data)
    folder_path = f"processed/{doc_id}"
    file_path = f"{folder_path}/{part_name}.parquet"
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    s3 = get_s3_client(creds)
    s3.upload_fileobj(parquet_buffer, creds['s3_bucket_name'], file_path)
    return folder_path

@task
def notify_django(doc_id: str, action: str, payload: dict):
    url = f"{DJANGO_API}/{doc_id}/{action}/"
    resp = httpx.post(url, json=payload, timeout=20)
    if resp.status_code >= 400:
        print(f"❌ Django Validation Error [{resp.status_code}]: {resp.text}")
    resp.raise_for_status()

@task
def process_vlm_extraction(file_bytes: bytes, vlm_pages: list, filename: str) -> list:
    data = []
    if not vlm_pages:
        return data

    ext = filename.split('.')[-1].lower() if '.' in filename else ''
    
    if ext in ['png', 'jpg', 'jpeg']:
        try:
            img_base64 = base64.b64encode(file_bytes).decode('utf-8')
            resp = httpx.post(VLM_API_URL, json={"image_base64": img_base64}, timeout=120.0) 
            resp.raise_for_status()
            
            resp_data = resp.json()
            extracted_text = resp_data.get("text")
            
            # --- ดักจับกรณี VLM รีเทิร์น text เป็น None ---
            if extracted_text is not None:
                final_text = extracted_text.strip()
            else:
                detail = resp_data.get("detail", "Unknown VLM Error")
                final_text = f"[VLM Error: {detail}]"
                
            data.append({"page_number": 1, "text": final_text})
        except Exception as e:
            data.append({"page_number": 1, "text": f"[Error connecting to VLM: {e}]"})
        return data
        
    try:
        doc = fitz.open(stream=file_bytes, filetype="pdf")
    except Exception:
        return []

    for p in vlm_pages:
        try:
            page = doc.load_page(p - 1)
            pix = page.get_pixmap(dpi=150)
            img_bytes = pix.tobytes("jpeg")
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            resp = httpx.post(VLM_API_URL, json={"image_base64": img_base64}, timeout=120.0)
            resp.raise_for_status()
            
            resp_data = resp.json()
            extracted_text = resp_data.get("text")
            
            # --- ดักจับกรณี VLM รีเทิร์น text เป็น None สำหรับไฟล์ PDF ---
            if extracted_text is not None:
                final_text = extracted_text.strip()
            else:
                detail = resp_data.get("detail", "Unknown VLM Error")
                final_text = f"[VLM Error on page {p}: {detail}]"
                
            data.append({"page_number": p, "text": final_text})
        except Exception as e:
            data.append({"page_number": p, "text": f"[Error connecting to VLM on page {p}: {e}]"})
            
    return data

@flow(name="process-document")
def ingest_flow(payload: dict):
    doc_id = payload["doc_id"]
    raw_storage_path = payload["raw_storage_path"]
    auto_vlm = payload.get("auto_vlm", False)
    original_filename = payload.get("original_filename", "doc.pdf")
    
    creds = {
        "s3_endpoint": payload["s3_endpoint"],
        "s3_access_key": payload["s3_access_key"],
        "s3_secret_key": payload["s3_secret_key"],
        "s3_bucket_name": payload["s3_bucket_name"]
    }
    
    file_bytes = download_file(raw_storage_path, creds)
    text_data, vlm_pages, total_pages = extract_and_split(file_bytes, original_filename)
    
    folder_path = save_parquet(text_data, doc_id, "part_1_text", creds)
    pending_vlm_count = len(vlm_pages)
    
    if pending_vlm_count == 0:
        notify_django(doc_id, "complete", {
            "total_pages": total_pages,
            "normal_pages": len(text_data),
            "vlm_pages": 0,
            "parquet_storage_path": folder_path
        })
        return

    if not auto_vlm:
        notify_django(doc_id, "pause_for_approval", {
            "pending_vlm_pages": pending_vlm_count,
            "normal_pages": len(text_data),
            "parquet_storage_path": folder_path
        })
        return

    vlm_data = process_vlm_extraction(file_bytes, vlm_pages, original_filename)
    if vlm_data:
        save_parquet(vlm_data, doc_id, "part_2_vlm", creds)
        
    notify_django(doc_id, "complete", {
        "total_pages": total_pages,
        "normal_pages": len(text_data),
        "vlm_pages": pending_vlm_count,
        "parquet_storage_path": folder_path
    })

@flow(name="process-vlm")
def resume_vlm_flow(payload: dict):
    doc_id = payload["doc_id"]
    resp = httpx.get(f"{DJANGO_API}/{doc_id}/", timeout=20)
    if resp.status_code != 200: 
        print(f"❌ Worker Error: Could not fetch data from Django. Status: {resp.status_code}, Response: {resp.text}")
        return
    cat = resp.json()
    creds = {
        "s3_endpoint": cat.get("s3_endpoint") or os.environ.get("S3_ENDPOINT_URL"),
        "s3_access_key": cat.get("s3_access_key") or os.environ.get("S3_ACCESS_KEY"),
        "s3_secret_key": cat.get("s3_secret_key") or os.environ.get("S3_SECRET_KEY"),
        "s3_bucket_name": cat.get("s3_bucket_name") or os.environ.get("S3_BUCKET_NAME", "knowledge-base")
    }
    
    raw_storage_path = cat["raw_storage_path"]
    original_filename = cat.get("original_filename", "doc.pdf")
    
    file_bytes = download_file(raw_storage_path, creds)
    _, vlm_pages, total_pages = extract_and_split(file_bytes, original_filename)
    
    vlm_data = process_vlm_extraction(file_bytes, vlm_pages, original_filename)
    if vlm_data:
        save_parquet(vlm_data, doc_id, "part_2_vlm", creds)
        
    notify_django(doc_id, "complete", {
        "total_pages": total_pages,
        "normal_pages": cat.get("normal_pages", 0),
        "vlm_pages": len(vlm_pages),
        "parquet_storage_path": f"processed/{doc_id}"
    })

if __name__ == "__main__":
    import time
    ingest_deploy = ingest_flow.to_deployment(name="ingest-deployment")
    resume_deploy = resume_vlm_flow.to_deployment(name="resume-deployment")
    
    for i in range(10):
        try:
            serve(ingest_deploy, resume_deploy)
            break
        except Exception:
            time.sleep(5)