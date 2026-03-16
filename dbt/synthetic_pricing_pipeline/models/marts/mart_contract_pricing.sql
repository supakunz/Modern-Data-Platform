{{ config(
    materialized='table'
) }}

select
    *
from {{ ref('int_feature_engineering') }}
