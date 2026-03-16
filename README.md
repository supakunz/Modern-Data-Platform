# Data Pipeline Platform (Synthetic Pricing Pipeline)

แพลตฟอร์ม Data Pipeline สำหรับข้อมูลสังเคราะห์ (mock data) เพื่อสร้างชุดข้อมูลคุณภาพ (bronze -> silver -> gold) และทำ data transformation ด้วย dbt โดยมี Airflow เป็น orchestration หลัก

## ไฮไลต์

- Airflow orchestrates งาน ETL และ dbt
- โหลดข้อมูล mock จาก source table ไปยัง Bronze layer
- dbt แปลงข้อมูลผ่าน Staging / Intermediate ไปยัง Mart (Gold)
- รองรับการรันทั้ง PostgreSQL (dev) และ BigQuery (prd)
- Docker Compose สำหรับ dev/prod
- Jupyter Notebook สำหรับวิเคราะห์ข้อมูล

## Architecture

![Planned Pipeline Architecture](arch/DesingArchPipeline.png)

## Pipeline Flow (สรุป)

1. Airflow DAG `synthetic_pricing_pipeline` รันทุกวันเวลา 07:00 (Asia/Bangkok)
2. Task `load_to_bronze`
   - dev: รัน `load_to_bronze.py` แล้วโหลดเข้า `bronze.contract_records` (PostgreSQL)
   - prd: รัน `load_to_bronze_bigquery.py` แล้วโหลดเข้า `${BQ_PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}` (BigQuery)
3. Task `dbt_build` รัน `dbt build --target <dev|prd> --select +mart_contract_pricing`

## Tech Stack

- Airflow 2.8.1 (Python 3.10)
- dbt-postgres 1.8.0 และ dbt-bigquery 1.8.0
- PostgreSQL 15
- BigQuery (สำหรับ prd target)
- pandas + SQLAlchemy
- Docker & Docker Compose
- Jupyter Notebook (Python 3.11)

## Project Structure

```text
.
├── airflow/                      # Airflow image + supporting code
├── dags/                         # Airflow DAGs หลัก
├── dbt/                          # dbt project และ profiles
├── scripts/                      # ETL scripts
├── data/                         # mock + mapping files
├── database/                     # init SQL และตัวอย่าง data artifacts
├── jupyter/                      # Jupyter Dockerfile + requirements
├── docker-compose.dev.yml
├── docker-compose.prd.yml
└── env/synthetic_pipeline.env
```

## การตั้งค่า (Configuration)

ไฟล์หลักที่ต้องแก้:

- `env/synthetic_pipeline.env` สำหรับ connection/source และค่า BigQuery
- `dbt/profiles.yml` สำหรับ dbt target (`dev` / `prd`)

ตัวอย่าง `env/synthetic_pipeline.env` (ใส่ค่าจริงเอง):

```env
# ===== SOURCE DB =====
SRC_DB_HOST=postgres
SRC_DB_PORT=5432
SRC_DB_NAME=airflow
SRC_DB_USER=airflow
SRC_DB_PASSWORD=airflow

# ===== WAREHOUSE DB (dev) =====
WH_DB_HOST=postgres
WH_DB_PORT=5432
WH_DB_NAME=airflow
WH_DB_USER=airflow
WH_DB_PASSWORD=airflow

# ===== BIGQUERY (prd) =====
BQ_PROJECT_ID=<gcp_project_id>
BQ_DATASET=bronze
BQ_TABLE=contract_records
BQ_LOCATION=asia-southeast1
```

ตัวแปรที่เกี่ยวข้องเพิ่มเติม:

- `SYNC_CALLBACK_URL` สำหรับ callback หลัง DAG สำเร็จ (optional)
- `JUPYTER_PORT` และ `JUPYTER_TOKEN` สำหรับ Jupyter

## BigQuery Authentication (ไม่ใช้ JSON key)

แนะนำใช้ ADC บนเครื่อง:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <gcp_project_id>
```

หมายเหตุ: compose มีการ mount `~/.config/gcloud` เข้า Airflow/dbt อยู่แล้ว

## การรันระบบด้วย Docker

### Dev

```bash
make dev-build
make dev-ps
```

Airflow UI: `http://localhost:8080` (ค่าเริ่มต้น `admin/admin`)

### Prod

```bash
make prod-up
make prod-ps
```

Airflow UI: `http://localhost:8090`

## คำสั่งที่ใช้บ่อย

```bash
# ตรวจสอบ container
make dev-ps

# ดู logs
make dev-logs
make prod-logs

# รัน dbt (dev target)
make dev-dbt-run

# รัน dbt test
make dev-dbt-test
```

## Data Layers (dbt)

- Bronze: ข้อมูลดิบจาก ingest (`contract_records`)
- Silver: staging + intermediate สำหรับ clean/mapping/feature
- Gold: `gold.mart_contract_pricing` สำหรับใช้งาน downstream

## ตัวอย่างผลลัพธ์ใน BigQuery

ใส่รูป screenshot จาก BigQuery หลังรัน pipeline สำเร็จ (ใช้ข้อมูล synthetic เท่านั้น)

### Bronze Silver Gold Layer (`gold.mart_contract_pricing`)

<img width="1465" height="825" alt="Image" src="https://github.com/user-attachments/assets/9ed59b4c-68b8-4a02-88a6-aa68dab8b0ff" />

หมายเหตุ:
- ควรเบลอ/ตัดข้อมูลที่เป็น project id, email, token, หรือ URL สำคัญก่อนอัปโหลด

## Data Lineage (dbt)

<img width="1403" height="429" alt="Image" src="https://github.com/user-attachments/assets/15c7470c-0faf-481f-87b6-81c912becb5e" />

โครงสร้างโดยรวม:
- `stg_contract_records` -> `int_*` -> `mart_contract_pricing`
- ใช้ mapping ด้านสี/ปี/condition ในชั้น intermediate ก่อนส่งไป mart

ถ้าต้องการดู lineage จริง สามารถใช้ dbt docs:

```bash
docker exec -it dbt dbt docs generate
docker exec -it dbt dbt docs serve --port 8081
```

แล้วเปิด `http://localhost:8081`

## การติดตั้ง dbt (Docker เท่านั้น)

โปรเจกต์นี้ใช้ dbt ผ่าน container `dbt`

1. เริ่ม container dbt (ข้ามได้ถ้ารัน compose ไปแล้ว)

```bash
docker compose -f docker-compose.dev.yml up -d dbt
docker compose --env-file env/synthetic_pipeline.env -f docker-compose.prd.yml up -d dbt
```

2. ตรวจสอบว่า dbt ใช้งานได้

```bash
docker exec -it dbt dbt --version
docker exec -it -w /usr/app/synthetic_pricing_pipeline dbt dbt debug
```

3. ติดตั้ง packages (ถ้ามี)

```bash
docker exec -it -w /usr/app/synthetic_pricing_pipeline dbt dbt deps
```

## หมายเหตุด้านความปลอดภัย

- โปรเจกต์นี้ใช้ข้อมูล mock/synthetic เพื่อเลี่ยงข้อมูลบริษัทจริง
- อย่า commit credentials/token
- ใช้ environment variables หรือ secret manager เมื่อ deploy จริง
