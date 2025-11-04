from google.cloud import bigquery
import os

def load_to_bigquery(table_name: str, source_folder: str, dataset="fao_dataset"):
    """Load tất cả file CSV trong thư mục vào BigQuery"""
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_id = f"{client.project}.{dataset}.{table_name}"

    for f in os.listdir(source_folder):
        if not f.endswith(".csv"):
            continue
        file_path = os.path.join(source_folder, f)
        print(f"[INFO] Loading {file_path} → {table_id}")

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE"
        )

        with open(file_path, "rb") as file:
            job = client.load_table_from_file(file, table_id, job_config=job_config)
        job.result()

    print(f"✅ Loaded data into BigQuery table: {table_id}")
