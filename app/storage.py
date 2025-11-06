
import os
from pathlib import Path
from google.cloud import storage
from google.auth.exceptions import DefaultCredentialsError

def gcs_client():
    # Only used when USE_LOCAL_STORAGE is not true
    return storage.Client(project=os.getenv("GCP_PROJECT_ID"))

def upload_bytes(bucket: str, blob_name: str, data: bytes, content_type: str = "application/octet-stream"):
    """
    If USE_LOCAL_STORAGE=true,
    Otherwise, upload to GCS.
    """
    use_local = os.getenv("USE_LOCAL_STORAGE", "").lower() == "true"
    if use_local:
        base = Path("local_uploads") / bucket
        dest = base / blob_name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return f"local://{dest.as_posix()}"
    else:
        try:
            client = gcs_client()
            b = client.bucket(bucket)
            blob = b.blob(blob_name)
            blob.upload_from_string(data, content_type=content_type)
            return f"gs://{bucket}/{blob_name}"
        except DefaultCredentialsError as e:
            # Make the error clear if user forgot creds
            raise RuntimeError(
                "GCP credentials missing. Set GOOGLE_APPLICATION_CREDENTIALS, "
                "GCP_PROJECT_ID, GCS_RAW_BUCKET, GCS_PROCESSED_BUCKET or set USE_LOCAL_STORAGE=true."
            ) from e
