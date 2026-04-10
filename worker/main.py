from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prefect.deployments import run_deployment

app = FastAPI()

class IngestPayload(BaseModel):
    doc_id: str
    raw_storage_path: str
    provider: str
    original_filename: str
    user_id: str
    auto_vlm: bool = False
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket_name: str

class ResumePayload(BaseModel):
    doc_id: str
    resume: bool = True
    raw_storage_path: str
    original_filename: str

@app.post("/webhook/ingest")
async def ingest_webhook(payload: IngestPayload):
    try:
        await run_deployment(
            name="process-document/ingest-deployment",
            parameters={"payload": payload.model_dump()},
            timeout=0
        )
        return {"status": "queued"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/webhook/resume_vlm")
async def resume_webhook(payload: ResumePayload):
    try:
        await run_deployment(
            name="process-vlm/resume-deployment",
            parameters={"payload": payload.model_dump()},
            timeout=0
        )
        return {"status": "queued"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))