import requests
import os
import datetime
from google.cloud import storage

# === Config ===
URL = "https://www.fao.org/fishery/services/statistics/api/data/download/query/aquaculture_quantity/en"
DOWNLOAD_FOLDER = os.path.join("data_input", "data_fao")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

GCS_BUCKET = os.getenv("GCS_BUCKET", "etl-lake")
UPLOAD_TO_GCS = os.getenv("UPLOAD_TO_GCS", "false").lower() == "true"

years = [str(y) for y in range(2023, 1949, -1)]

payload = {
    "aggregationType": "sum",
    "disableSymbol": "false",
    "includeNullValues": "true",
    "grouped": True,
    "rows": [
        {"field": "country_un_code", "group": "COUNTRY", "groupField": "name_en", "order": "asc"}
    ],
    "columns": [
        {
            "field": "year",
            "order": "desc",
            "values": years,
            "condition": "In",
            "sort": "desc"
        }
    ],
    "filters": []
}

headers = {"Content-Type": "application/json"}

# === Download function ===
def download_fao_data():
    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_name = f"aquaculture_data_{timestamp}.csv"
    file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
    try:
        print("[INFO] Sending request to FAO API...")
        res = requests.post(URL, json=payload, headers=headers, timeout=120)
        res.raise_for_status()
        with open(file_path, "wb") as f:
            f.write(res.content)
        print(f"[INFO] Data saved to {file_path}")
        return file_path
    except Exception as e:
        print(f"[ERROR] Failed to download FAO data: {e}")
        return None

# === Optional upload to GCS ===
def upload_to_gcs(local_path):
    try:
        client = storage.Client()
        bucket = client.bucket(GCS_BUCKET)
        blob_name = f"bronze/fao/{os.path.basename(local_path)}"
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(local_path)
        print(f"[INFO] Uploaded to GCS: gs://{GCS_BUCKET}/{blob_name}")
    except Exception as e:
        print(f"[ERROR] GCS upload failed: {e}")

if __name__ == "__main__":
    path = download_fao_data()
    if path and UPLOAD_TO_GCS:
        upload_to_gcs(path)
