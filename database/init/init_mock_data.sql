-- Create schemas used by dbt layers
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Cleanup old legacy names to avoid confusion.
DROP VIEW IF EXISTS silver.stg_dbt_jjm_customer_loan CASCADE;
DROP TABLE IF EXISTS gold.mart_dbt_jjm_customer_loan CASCADE;
DROP TABLE IF EXISTS bronze.dbt_jjm_customer_loan CASCADE;
DROP TABLE IF EXISTS public.jjm_customer_loan CASCADE;

-- Source table used by scripts/synthetic_pipeline/load_to_bronze.py
CREATE TABLE IF NOT EXISTS public.source_contracts (
    contract_num TEXT,
    transaction_date TIMESTAMP,
    form_id BIGINT,
    brand TEXT,
    model TEXT,
    sub_model TEXT,
    size TEXT,
    color TEXT,
    hardware TEXT,
    material TEXT,
    picture_url TEXT,
    condition TEXT,
    year_stamp_holo_dc TEXT,
    product_year TEXT,
    estimate_amount INTEGER,
    actual_price INTEGER,
    status INTEGER,
    actualprice_status INTEGER
);

TRUNCATE TABLE public.source_contracts;

COPY public.source_contracts (
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
    actual_price,
    status,
    actualprice_status
)
FROM '/mock/synthetic_contracts.csv'
WITH (
    FORMAT csv,
    HEADER true
);

-- Keep bronze source ready so dbt can run immediately after docker compose up.
CREATE TABLE IF NOT EXISTS bronze.contract_records (
    contract_num TEXT,
    transaction_date TIMESTAMP,
    form_id BIGINT,
    brand TEXT,
    model TEXT,
    sub_model TEXT,
    size TEXT,
    color TEXT,
    hardware TEXT,
    material TEXT,
    picture_url TEXT,
    condition TEXT,
    year_stamp_holo_dc TEXT,
    product_year TEXT,
    estimate_amount INTEGER,
    actual_price INTEGER
);

TRUNCATE TABLE bronze.contract_records;

INSERT INTO bronze.contract_records (
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
)
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
FROM public.source_contracts
WHERE status = 1
  AND actualprice_status = 1
  AND actual_price > 0;
