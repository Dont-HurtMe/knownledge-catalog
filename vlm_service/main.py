import os
import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
app = FastAPI(title="VLM Plug & Play Service")
VERIFY_SSL = os.environ.get('VERIFY_SSL', 'False').lower() in ('true', '1', 't')
http_client = httpx.Client(verify=VERIFY_SSL, timeout=120.0)

client = OpenAI(
    base_url=os.getenv("VLM_API_BASE"),
    api_key=os.getenv("VLM_API_KEY"),
    http_client=http_client,
    timeout=120.0 
)
MODEL_NAME = os.getenv("VLM_MODEL")

class VLMRequest(BaseModel):
    image_base64: str
    prompt: str = (
        "Extract all text from this image exactly as it appears. "
        "If diagrams or tables are found, provide a detailed description of the sequences, flow, or data contained within them instead of attempting to format them as tables. "
        "For text in articles, abstracts, or scanned documents, perform a full raw text extraction. "
        "Do not summarize the content, do not include markdown headers, and do not include any conversational or filler text."
    )

@app.post("/extract")
def extract_text(req: VLMRequest):
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": req.prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{req.image_base64}"}}
                    ],
                }
            ],
            temperature=0.2, 
            max_tokens=8192, 
            extra_body={
                "options": {
                    "num_ctx": 16384,  
                    "num_gpu": 99      
                }
            }
        )
        return {"status": "success", "text": response.choices[0].message.content}
    except Exception as e:
        return {"status": "error", "text": None, "detail": str(e)}