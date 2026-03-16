{% if target.type == 'bigquery' %}
  {% set extract_year_expr = "regexp_extract(year_stamp_holo_dc, r'((?:19|20)[0-9]{2})')" %}
  {% set has_alpha_expr = "regexp_contains(year_stamp_holo_dc, r'[A-Za-z]')" %}
  {% set has_digit_expr = "regexp_contains(year_stamp_holo_dc, r'[0-9]')" %}
{% else %}
  {% set extract_year_expr = "(regexp_match(year_stamp_holo_dc, '((?:19|20)[0-9]{2})'))[1]" %}
  {% set has_alpha_expr = "year_stamp_holo_dc ~ '[A-Za-z]'" %}
  {% set has_digit_expr = "year_stamp_holo_dc ~ '[0-9]'" %}
{% endif %}

with base as (

    select *
    from {{ ref('int_color_mapping') }}

),

normalized as (

    select
        *,
        nullif(trim(cast(product_year as {{ dbt.type_string() }})), '') as product_year_clean,
        nullif(trim(cast(year_stamp_holo_dc as {{ dbt.type_string() }})), '') as stamp_clean
    from base

),

mapped as (

    select
        *,
        case
            when product_year_clean is not null
                then product_year_clean

            when stamp_clean is null
                then 'Unknown'

            when lower(stamp_clean) in ('n/a', 'na')
                then 'Unknown'

            when stamp_clean in ('ไม่ระบุ', 'จำไม่ได้', 'ไม่ทราบ', 'ไม่แน่ใจ')
                then 'Unknown'

            when lower(stamp_clean) like '%metal plate%'
                then 'legacy code'

            when lower(stamp_clean) like '%holo%'
                then 'legacy code'

            when lower(stamp_clean) like '%microchip%'
                then 'legacy code'

            when {{ extract_year_expr }} is not null
                then {{ extract_year_expr }}

            when {{ has_alpha_expr }} and {{ has_digit_expr }}
                then 'legacy code'

            when {{ has_alpha_expr }}
                then 'legacy code'

            else 'Unknown'
        end as product_year_derived
    from normalized

),

final as (

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
        product_year_derived as product_year,
        estimate_amount,
        color_segment
    from mapped

)

select *
from final
