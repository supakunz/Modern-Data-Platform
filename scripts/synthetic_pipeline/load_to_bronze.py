import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ======================================================
# LOAD ENV
# ======================================================
load_dotenv("/opt/airflow/env/synthetic_pipeline.env")

# ======================================================
# DB CONNECTIONS
# ======================================================
def make_engine(prefix: str):
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv(prefix + '_DB_USER')}:"
        f"{os.getenv(prefix + '_DB_PASSWORD')}@"
        f"{os.getenv(prefix + '_DB_HOST')}:"
        f"{os.getenv(prefix + '_DB_PORT')}/"
        f"{os.getenv(prefix + '_DB_NAME')}"
    )

src_engine = make_engine("SRC")
wh_engine = make_engine("WH")

# ======================================================
# EXTRACT
# ======================================================
print("🔄 Loading source data...")

df = pd.read_sql("""
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
""", src_engine)

row_count = len(df)

print(f"✅ Extracted rows: {row_count}")

# ---------------- ALERT 1 ----------------
if row_count == 0:
    raise Exception("❌ ALERT: Source returned 0 rows")

# ---------------- ALERT 2 ----------------
null_form = df["form_id"].isna().sum()
if null_form > 0:
    raise Exception(
        f"❌ ALERT: form_id contains {null_form} NULL rows"
    )

# ======================================================
# LOAD TO BRONZE
# ======================================================
TABLE_NAME = "contract_records"
SCHEMA_NAME = "bronze"

print("🔄 Loading to bronze...")

with wh_engine.begin() as conn:

    exists = conn.execute(text(f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = '{SCHEMA_NAME}'
              AND table_name = '{TABLE_NAME}'
        )
    """)).scalar()

    if exists:
        conn.execute(text(
            f"TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME}"
        ))

# LOAD
df.to_sql(
    TABLE_NAME,
    wh_engine,
    schema=SCHEMA_NAME,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=5000
)

# ---------------- ALERT 3 ----------------
with wh_engine.begin() as conn:
    loaded_rows = conn.execute(text(f"""
        SELECT count(*) FROM {SCHEMA_NAME}.{TABLE_NAME}
    """)).scalar()

if loaded_rows != row_count:
    raise Exception(
        f"❌ ALERT: Loaded rows mismatch source={row_count} target={loaded_rows}"
    )

print("✅ Load to bronze completed successfully")
