import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


def _load_env() -> None:
    primary_env_path = os.getenv("DAILY_REPORT_ENV_PATH", "/opt/airflow/env/daily_report.env")
    fallback_env_path = "env/daily_report.env"
    if os.path.exists(primary_env_path):
        load_dotenv(primary_env_path)
        return
    load_dotenv(fallback_env_path)


_load_env()

BASE_URL = os.getenv("REDASH_BASE_URL")
QUERY_ID_RAW = os.getenv("REDASH_QUERY_ID")
API_KEY = os.getenv("REDASH_API_KEY")

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE", "redash_new_contracts")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-southeast1")
BQ_CREDENTIALS_PATH = os.getenv("BQ_CREDENTIALS_PATH")
BQ_MAX_BYTES_BILLED = int(os.getenv("BQ_MAX_BYTES_BILLED", "0"))
REDASH_LOOKBACK_DAYS = int(os.getenv("REDASH_LOOKBACK_DAYS", "2"))

if BQ_CREDENTIALS_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = BQ_CREDENTIALS_PATH

if not BASE_URL or not QUERY_ID_RAW or not API_KEY:
    raise ValueError("Missing required Redash environment variables")

if not BQ_PROJECT_ID or not BQ_DATASET or not BQ_TABLE:
    raise ValueError("Missing required BigQuery environment variables: BQ_PROJECT_ID, BQ_DATASET, BQ_TABLE")

if BQ_CREDENTIALS_PATH and not os.path.exists(BQ_CREDENTIALS_PATH):
    raise ValueError(f"BQ_CREDENTIALS_PATH not found: {BQ_CREDENTIALS_PATH}")

if REDASH_LOOKBACK_DAYS < 1:
    raise ValueError("REDASH_LOOKBACK_DAYS must be >= 1")

QUERY_ID = int(QUERY_ID_RAW)

TARGET_SCHEMA = [
    bigquery.SchemaField("contract_no", "STRING"),
    bigquery.SchemaField("contract_type", "STRING"),
    bigquery.SchemaField("item_type", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("model", "STRING"),
    bigquery.SchemaField("size", "STRING"),
    bigquery.SchemaField("color", "STRING"),
    bigquery.SchemaField("hardware", "STRING"),
    bigquery.SchemaField("material", "STRING"),
    bigquery.SchemaField("year", "STRING"),
    bigquery.SchemaField("holodc", "STRING"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("approved_amount", "FLOAT"),
    bigquery.SchemaField("transfer_date", "DATE"),
    bigquery.SchemaField("promo_code", "STRING"),
    bigquery.SchemaField("details", "STRING"),
    bigquery.SchemaField("image_url", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
]

UPSERT_COLUMNS = [field.name for field in TARGET_SCHEMA]


def get_today_range():
    end_date = date.today()
    start_date = end_date - timedelta(days=REDASH_LOOKBACK_DAYS - 1)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def execute_query(start_date, end_date):
    payload = {
        "parameters": {
            "date": {
                "start": start_date,
                "end": end_date,
            }
        }
    }

    res = requests.post(
        f"{BASE_URL}/api/queries/{QUERY_ID}/results",
        headers={"Authorization": f"Key {API_KEY}"},
        json=payload,
        timeout=60,
    )
    res.raise_for_status()
    data = res.json()

    if "query_result" in data:
        return data["query_result"]["data"]["rows"]

    if "job" in data:
        job_id = data["job"]["id"]
        return wait_for_job(job_id)

    raise RuntimeError(f"Unexpected Redash response: {data}")


def wait_for_job(job_id, max_wait=300):
    waited = 0

    while waited < max_wait:
        res = requests.get(
            f"{BASE_URL}/api/jobs/{job_id}",
            headers={"Authorization": f"Key {API_KEY}"},
            timeout=30,
        )
        res.raise_for_status()
        job = res.json()["job"]

        if job["status"] == 3:
            result_id = job["query_result_id"]
            result = requests.get(
                f"{BASE_URL}/api/query_results/{result_id}.json",
                headers={"Authorization": f"Key {API_KEY}"},
                timeout=60,
            )
            result.raise_for_status()
            return result.json()["query_result"]["data"]["rows"]

        if job["status"] == 4:
            raise RuntimeError("Redash query failed")

        time.sleep(2)
        waited += 2

    raise TimeoutError("Redash job timeout")


def _to_str(value):
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _parse_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_transfer_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _map_row(row):
    contract_no = _to_str(row.get("เลขที่สัญญา"))
    if not contract_no:
        return None

    return {
        "contract_no": contract_no,
        "contract_type": _to_str(row.get("ประเภทสัญญา")),
        "item_type": _to_str(row.get("ประเภท")),
        "brand": _to_str(row.get("ยี่ห้อ")),
        "model": _to_str(row.get("รุ่น")),
        "size": _to_str(row.get("ขนาด")),
        "color": _to_str(row.get("สี")),
        "hardware": _to_str(row.get("ฮาร์ดแวร์")),
        "material": _to_str(row.get("วัสดุ")),
        "year": _to_str(row.get("ปี")),
        "holodc": _to_str(row.get("holodc")),
        "first_name": _to_str(row.get("ชื่อ")),
        "last_name": _to_str(row.get("นามสกุล")),
        "approved_amount": _parse_float(row.get("วงเงินอนุมัติ")),
        "transfer_date": _parse_transfer_date(row.get("วันที่โอน")),
        "promo_code": _to_str(row.get("โปรโมชั่นโค้ด")),
        "details": _to_str(row.get("รายละเอียด")),
        "image_url": _to_str(row.get("รูปภาพ")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def _bigquery_client():
    return bigquery.Client(project=BQ_PROJECT_ID, location=BQ_LOCATION)


def _ensure_target_table(client):
    dataset_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"Created dataset: {dataset_id}")

    target_table_id = f"{dataset_id}.{BQ_TABLE}"
    try:
        table = client.get_table(target_table_id)
        if table.time_partitioning is None or not table.clustering_fields:
            print(
                "Warning: existing table is not partitioned/clustering configured. "
                "Cost can be higher than needed."
            )
    except NotFound:
        table = bigquery.Table(target_table_id, schema=TARGET_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="transfer_date",
        )
        table.clustering_fields = ["contract_no"]
        client.create_table(table)
        print(f"Created table: {target_table_id}")

    return target_table_id


def _build_merge_sql(target_table_id, temp_table_id):
    update_clause = ",\n        ".join(
        f"`{column}` = S.`{column}`" for column in UPSERT_COLUMNS if column != "contract_no"
    )
    insert_columns = ", ".join(f"`{column}`" for column in UPSERT_COLUMNS)
    insert_values = ", ".join(f"S.`{column}`" for column in UPSERT_COLUMNS)

    return f"""
    MERGE `{target_table_id}` T
    USING `{temp_table_id}` S
      ON T.contract_no = S.contract_no
    WHEN MATCHED THEN
      UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """


def upsert_into_bigquery(rows):
    if not rows:
        print("No data returned from Redash")
        return

    transformed_by_contract = {}
    skipped_rows = 0
    for row in rows:
        mapped_row = _map_row(row)
        if not mapped_row:
            skipped_rows += 1
            continue
        transformed_by_contract[mapped_row["contract_no"]] = mapped_row

    transformed_rows = list(transformed_by_contract.values())
    duplicate_rows = len(rows) - skipped_rows - len(transformed_rows)

    if not transformed_rows:
        print("No valid rows for BigQuery after transformation")
        return

    client = _bigquery_client()
    target_table_id = _ensure_target_table(client)
    temp_table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.tmp_redash_contracts_{uuid.uuid4().hex}"

    load_job = client.load_table_from_json(
        transformed_rows,
        temp_table_id,
        job_config=bigquery.LoadJobConfig(
            schema=TARGET_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    load_job.result()

    merge_sql = _build_merge_sql(target_table_id, temp_table_id)
    query_job_config = None
    if BQ_MAX_BYTES_BILLED > 0:
        query_job_config = bigquery.QueryJobConfig(maximum_bytes_billed=BQ_MAX_BYTES_BILLED)
    merge_job = client.query(merge_sql, job_config=query_job_config)
    merge_job.result()

    client.delete_table(temp_table_id, not_found_ok=True)

    affected = merge_job.num_dml_affected_rows or 0
    print(f"Upsert completed in BigQuery table {target_table_id}")
    print(
        f"Total source rows: {len(rows)} | Upserted rows: {affected} | "
        f"Skipped rows: {skipped_rows} | Duplicate keys merged: {duplicate_rows}"
    )


if __name__ == "__main__":
    print("Starting ingestion job")
    start_date, end_date = get_today_range()
    print(f"Using date range: {start_date} -> {end_date}")

    rows = execute_query(start_date, end_date)
    print(f"Retrieved rows from Redash: {len(rows)}")

    upsert_into_bigquery(rows)
    print("Done")
