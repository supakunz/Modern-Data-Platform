import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

# ======================================================
# LOAD ENV
# ======================================================
load_dotenv("/opt/airflow/env/synthetic_pipeline.env")

# ======================================================
# SOURCE DB CONNECTION
# ======================================================
def make_src_engine():
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('SRC_DB_USER')}:"
        f"{os.getenv('SRC_DB_PASSWORD')}@"
        f"{os.getenv('SRC_DB_HOST')}:"
        f"{os.getenv('SRC_DB_PORT')}/"
        f"{os.getenv('SRC_DB_NAME')}"
    )


def make_bq_client():
    project_id = os.getenv("BQ_PROJECT_ID")
    if not project_id:
        raise ValueError("Missing BQ_PROJECT_ID for PRD BigQuery load")

    # Guard against compose/env injecting empty GOOGLE_APPLICATION_CREDENTIALS.
    explicit_adc = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if explicit_adc is not None and not explicit_adc.strip():
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)

    credentials_path = os.getenv("BQ_CREDENTIALS_PATH")
    if credentials_path:
        if not os.path.exists(credentials_path):
            raise ValueError(f"BQ_CREDENTIALS_PATH not found: {credentials_path}")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    location = os.getenv("BQ_LOCATION", "asia-southeast1")
    try:
        return bigquery.Client(project=project_id, location=location)
    except DefaultCredentialsError as exc:
        raise RuntimeError(
            "BigQuery ADC not found. Run on host machine:\n"
            "  gcloud auth application-default login\n"
            f"  gcloud config set project {project_id}"
        ) from exc


# ======================================================
# EXTRACT
# ======================================================
print("Loading source data...")

src_engine = make_src_engine()
df = pd.read_sql(
    """
    SELECT
        contract_num,
        transaction_date,
        form_id,
        brand,
        model,
        sub_model,
        size,
        color,
        hardware,
        material,
        picture_url,
        condition,
        year_stamp_holo_dc,
        product_year,
        estimate_amount,
        actual_price
    FROM source_contracts
    WHERE status = 1
      AND actualprice_status = 1
      AND actual_price > 0
    """,
    src_engine,
)

row_count = len(df)
print(f"Extracted rows: {row_count}")

if row_count == 0:
    raise Exception("ALERT: Source returned 0 rows")

null_form = df["form_id"].isna().sum()
if null_form > 0:
    raise Exception(f"ALERT: form_id contains {null_form} NULL rows")

# Force types before load.
df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")
df["form_id"] = df["form_id"].astype("int64")
df["estimate_amount"] = df["estimate_amount"].astype("int64")
df["actual_price"] = df["actual_price"].astype("int64")

# load_table_from_json handles plain Python values safely.
df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
records = df.where(pd.notnull(df), None).to_dict(orient="records")

# ======================================================
# LOAD TO BIGQUERY BRONZE
# ======================================================
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "bronze")
BQ_TABLE = os.getenv("BQ_TABLE", "contract_records")
BQ_LOCATION = os.getenv("BQ_LOCATION", "asia-southeast1")

client = make_bq_client()
dataset_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}"
table_id = f"{dataset_id}.{BQ_TABLE}"

print(f"Ensuring dataset exists: {dataset_id} ({BQ_LOCATION})")
try:
    client.get_dataset(dataset_id)
except NotFound:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BQ_LOCATION
    client.create_dataset(dataset)

schema = [
    bigquery.SchemaField("contract_num", "STRING"),
    bigquery.SchemaField("transaction_date", "TIMESTAMP"),
    bigquery.SchemaField("form_id", "INT64"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("model", "STRING"),
    bigquery.SchemaField("sub_model", "STRING"),
    bigquery.SchemaField("size", "STRING"),
    bigquery.SchemaField("color", "STRING"),
    bigquery.SchemaField("hardware", "STRING"),
    bigquery.SchemaField("material", "STRING"),
    bigquery.SchemaField("picture_url", "STRING"),
    bigquery.SchemaField("condition", "STRING"),
    bigquery.SchemaField("year_stamp_holo_dc", "STRING"),
    bigquery.SchemaField("product_year", "STRING"),
    bigquery.SchemaField("estimate_amount", "INT64"),
    bigquery.SchemaField("actual_price", "INT64"),
]

print(f"Loading {row_count} rows to {table_id}")
load_job = client.load_table_from_json(
    records,
    table_id,
    job_config=bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    ),
)
load_job.result()

loaded_rows = client.get_table(table_id).num_rows
if loaded_rows != row_count:
    raise Exception(f"ALERT: Loaded rows mismatch source={row_count} target={loaded_rows}")

print("Load to BigQuery bronze completed successfully")
