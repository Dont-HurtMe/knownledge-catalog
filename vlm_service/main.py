import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from openai import OpenAI

app = FastAPI(title="VLM Plug & Play Service")

client = OpenAI(
    base_url=os.getenv("VLM_API_BASE", "https://api.openai.com/v1"),
    api_key=os.getenv("VLM_API_KEY", "dummy-key")
)
MODEL_NAME = os.getenv("VLM_MODEL", "gpt-4o-mini")

class VLMRequest(BaseModel):
    image_base64: str
    prompt: str = "Extract all text from this image exactly as it appears. Do not add markdown formatting, headers, or any conversational text."

@app.post("/extract")
def extract_text(req: VLMRequest):
    if not os.getenv("VLM_API_KEY") or os.getenv("VLM_API_KEY") == "your-openai-api-key-here":
        return {"text": ""}

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
            max_tokens=1500,
        )
        return {"text": response.choices[0].message.content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
