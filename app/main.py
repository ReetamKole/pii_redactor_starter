import os
from datetime import datetime
from pathlib import Path
import json 
import pandas as pd
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .storage import upload_bytes
from .utils import redact_text, is_valid_phone
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

RAW_BUCKET = os.getenv("GCS_RAW_BUCKET", "your-raw-data-bucket")
PROCESSED_BUCKET = os.getenv("GCS_PROCESSED_BUCKET", "your-processed-data-bucket")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile = File(...),
    name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(...),
):
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    raw_key = f"raw/{ts}-{file.filename}"
    content = await file.read()
    upload_bytes(bucket=RAW_BUCKET, blob_name=raw_key, data=content, content_type=file.content_type or "application/octet-stream")

    metadata = {
        "name": name,
        "email": email,
        "phone": phone,
        "filename": file.filename,
        "uploaded_utc": ts,
        "phone_valid": is_valid_phone(phone),
    }
    meta_key = f"raw/{ts}-{Path(file.filename).stem}.json"
    upload_bytes(bucket=RAW_BUCKET, blob_name=meta_key, data=json.dumps(metadata).encode("utf-8"), content_type="application/json")

    processed_key = f"processed/{ts}-redacted-{file.filename}"
    try:
        if file.filename.lower().endswith(".csv"):
            from io import StringIO
            df = pd.read_csv(StringIO(content.decode("utf-8", errors="ignore")))
            for col in df.columns:
                df[col] = df[col].astype(str).map(redact_text)
            processed_bytes = df.to_csv(index=False).encode("utf-8")
            upload_bytes(bucket=PROCESSED_BUCKET, blob_name=processed_key, data=processed_bytes, content_type="text/csv")
        else:
            redacted = redact_text(content.decode("utf-8", errors="ignore"))
            upload_bytes(bucket=PROCESSED_BUCKET, blob_name=processed_key, data=redacted.encode("utf-8"), content_type="text/plain")
    except Exception as e:
        print("Processing error:", e)

    return templates.TemplateResponse("success.html", {
        "request": request,
        "raw_key": raw_key,
        "meta_key": meta_key,
        "processed_key": processed_key,
        "raw_bucket": RAW_BUCKET,
        "processed_bucket": PROCESSED_BUCKET
    })
