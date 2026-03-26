import os, json, fitz, boto3
from prefect import flow, task
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import uvicorn

S3_ENDPOINT = os.environ.get('S3_ENDPOINT_URL', 'http://minio:9000')
S3_ACCESS = os.environ.get('S3_ACCESS_KEY', 'admin')
S3_SECRET = os.environ.get('S3_SECRET_KEY', 'qwer1234')
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'knowledge-base')

s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS, aws_secret_access_key=S3_SECRET)

def validate_page(page, text):
    if not text or len(text.strip()) < 30: raise Exception("LowTextDensity")
    if "(cid:" in text or text.count("\ufffd") > 1: raise Exception("BrokenFont")

@task
def process_page_task(doc_id: str, local_pdf_path: str, page_num: int):
    status, strategy = "QUEUED", "fitz_raw"
    text_content = ""
    try:
        doc = fitz.open(local_pdf_path)
        page = doc[page_num]
        raw_text = page.get_text()
        try:
            validate_page(page, raw_text)
            text_content = raw_text
            status = "SUCCESS"
        except Exception as e:
            strategy = "vlm_fallback"
            status = "SKIPPED" 

        if status == "SUCCESS":
            payload = {"doc_id": doc_id, "unit_ref": f"PAGE_{page_num+1}", "strategy": strategy, "text": text_content}
            minio_path = f"extracted/{doc_id}/page_{page_num+1}.json"
            s3.put_object(Bucket=BUCKET_NAME, Key=minio_path, Body=json.dumps(payload, ensure_ascii=False).encode('utf-8'))
            print(f"✅ Saved -> {minio_path}")

    except Exception as e:
        print(f"❌ Error page {page_num}: {e}")

@flow(name="PDF_Ingestion_Pipeline")
def ingest_pdf_flow(doc_id: str, minio_path: str):
    print(f"⚙️ เริ่ม Ingest Document: {doc_id}")
    local_pdf_path = f"/tmp/{doc_id}.pdf"
    s3.download_file(BUCKET_NAME, minio_path, local_pdf_path)
    doc = fitz.open(local_pdf_path)
    process_page_task.map(doc_id=doc_id, local_pdf_path=local_pdf_path, page_num=list(range(len(doc))))
    os.remove(local_pdf_path)
    print("🎉 Pipeline Done!")

app = FastAPI(title="Ingest Worker")
class IngestPayload(BaseModel):
    doc_id: str
    minio_path: str

@app.post("/webhook/ingest")
def trigger_ingestion(payload: IngestPayload, bg: BackgroundTasks):
    bg.add_task(ingest_pdf_flow, payload.doc_id, payload.minio_path)
    return {"status": "Processing Started"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
