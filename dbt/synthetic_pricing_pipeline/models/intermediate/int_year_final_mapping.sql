{% if target.type == 'bigquery' %}
  {% set extract_year_from_stamp = "regexp_extract(stamp_clean, r'((?:19|20)[0-9]{2})')" %}
{% else %}
  {% set extract_year_from_stamp = "(regexp_match(stamp_clean, '((?:19|20)[0-9]{2})'))[1]" %}
{% endif %}

with base as (

    select
        *,
        nullif(trim(cast(product_year as {{ dbt.type_string() }})), '') as product_year_clean,
        upper(trim(cast(year_stamp_holo_dc as {{ dbt.type_string() }}))) as stamp_clean
    from {{ ref('int_year_stamp_mapping') }}

),

derived as (

    select
        *,
        case
            when product_year_clean is not null
                then product_year_clean
            when stamp_clean is null or stamp_clean = ''
                then 'Unknown'
            when {{ extract_year_from_stamp }} is not null
                then {{ extract_year_from_stamp }}
            else 'Unknown'
        end as final_product_year,

        case
            when product_year_clean is not null
                then product_year_clean
            when stamp_clean in ('A', 'B', 'C', 'D', 'E')
                then stamp_clean
            when {{ extract_year_from_stamp }} is not null
                then {{ extract_year_from_stamp }}
            else 'Unknown'
        end as final_year_letter
    from base

)

select
    contract_num,
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
    transaction_date,
    final_product_year as product_year,
    estimate_amount,
    color_segment,
    final_year_letter as year_letter
from derived
