import os
from datetime import datetime
from pathlib import Path
import json 
import pandas as pd
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import HTTPException
import uuid
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

load_dotenv()

from .storage import upload_bytes, init_database, save_metadata_to_db, get_all_uploads_with_anomalies, get_anomaly_statistics
from .utils import redact_text, is_valid_phone, is_valid_email, detect_anomalies

logger = logging.getLogger("app")
app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    init_database()

RAW_BUCKET = os.getenv("GCS_RAW_BUCKET", "your-raw-data-bucket")
PROCESSED_BUCKET = os.getenv("GCS_PROCESSED_BUCKET", "your-processed-data-bucket")
logger.info("=" * 60)
logger.info("Initializing application")
logger.info(f"RAW_BUCKET: {RAW_BUCKET}")
logger.info(f"PROCESSED_BUCKET: {PROCESSED_BUCKET}")
logger.info("=" * 60)

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
    upload_id = uuid.uuid4().hex
    logger.info(f"Upload started: id={upload_id}, file={file.filename}")
    raw_key = f"raw/{upload_id}/{ts}-{file.filename}"
    content = await file.read()
    logger.info(f"File read successfully: size={len(content)} bytes")
    upload_bytes(bucket=RAW_BUCKET, blob_name=raw_key, data=content, content_type=file.content_type or "application/octet-stream")
    logger.info(f"Uploaded raw file to {RAW_BUCKET}/{raw_key}")
    
    safe_name = Path(file.filename).name
    processed_key = f"processed/{ts}-{upload_id}-redacted-{safe_name}"
    
    # Detect anomalies in user input
    anomaly_check = detect_anomalies(name, email, phone)
    
    metadata = {
        "upload_id": upload_id,
        "name": name,
        "email": email,
        "phone": phone,
        "filename": safe_name,
        "filesize_bytes": len(content),
        "filetype": file.content_type or "application/octet-stream",
        "uploaded_utc": ts,
        "phone_valid": is_valid_phone(phone),
        "email_valid": is_valid_email(email),
        "anomaly": anomaly_check["has_anomaly"],
        "anomaly_details": anomaly_check["anomaly_details"],
        "raw_key": raw_key,
        "processed_key": processed_key,
    }
    
    # Log anomalies if detected
    if anomaly_check["has_anomaly"]:
        logger.warning(f"Anomalies detected for upload {upload_id}: {anomaly_check['anomaly_details']}")
    
    # Save metadata to database
    save_metadata_to_db(metadata)
    
    meta_key = f"raw/{upload_id}/{ts}-{Path(safe_name).stem}.json"
    upload_bytes(bucket=RAW_BUCKET, blob_name=meta_key, data=json.dumps(metadata).encode("utf-8"), content_type="application/json")
    try:
        if file.filename.lower().endswith(".csv"):
            from io import StringIO
            df = pd.read_csv(StringIO(content.decode("utf-8", errors="ignore")))
            for col in df.columns:
                df[col] = df[col].astype(str).map(redact_text)
            processed_bytes = df.to_csv(index=False).encode("utf-8")
            upload_bytes(bucket=PROCESSED_BUCKET, blob_name=processed_key, data=processed_bytes, content_type="text/csv")
            logger.info(f"Processed CSV uploaded to {PROCESSED_BUCKET}/{processed_key}")
        else:
            redacted = redact_text(content.decode("utf-8", errors="ignore"))
            upload_bytes(bucket=PROCESSED_BUCKET, blob_name=processed_key, data=redacted.encode("utf-8"), content_type="text/plain")
            logger.info(f"Processed text file uploaded to {PROCESSED_BUCKET}/{processed_key}")
    except Exception as e:
        logger.exception(f"Error while processing file {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")
        
    logger.info(f"Upload completed successfully: id={upload_id}")
    return templates.TemplateResponse("success.html", {
        "request": request,
        "raw_key": raw_key,
        "meta_key": meta_key,
        "processed_key": processed_key,
        "raw_bucket": RAW_BUCKET,
        "processed_bucket": PROCESSED_BUCKET
    })

@app.get("/report", response_class=HTMLResponse)
async def anomaly_report(request: Request):
    """Anomaly Detection Dashboard"""
    uploads = get_all_uploads_with_anomalies(limit=100)
    stats = get_anomaly_statistics()
    
    return templates.TemplateResponse("report.html", {
        "request": request,
        "uploads": uploads,
        "stats": stats
    })

@app.get("/report/json")
async def anomaly_report_json():
    """API endpoint for anomaly data"""
    uploads = get_all_uploads_with_anomalies(limit=100)
    stats = get_anomaly_statistics()
    
    return {
        "uploads": uploads,
        "statistics": stats
    }
