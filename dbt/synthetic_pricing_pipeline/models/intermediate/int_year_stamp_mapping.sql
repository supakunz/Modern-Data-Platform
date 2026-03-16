{% if target.type == 'bigquery' %}
  {% set extract_year_expr = "regexp_extract(stamp_clean_up, r'((?:19|20)[0-9]{2})')" %}
  {% set extract_letter_expr = "regexp_extract(stamp_clean_up, r'([A-Z])')" %}
  {% set has_digit_expr = "regexp_contains(stamp_clean_up, r'[0-9]')" %}
{% else %}
  {% set extract_year_expr = "(regexp_match(stamp_clean_up, '((?:19|20)[0-9]{2})'))[1]" %}
  {% set extract_letter_expr = "substring(stamp_clean_up from '([A-Z])')" %}
  {% set has_digit_expr = "stamp_clean_up ~ '[0-9]'" %}
{% endif %}

with base as (

    select *
    from {{ ref('int_color_mapping') }}

),

normalized as (

    select
        *,
        nullif(trim(cast(product_year as {{ dbt.type_string() }})), '') as product_year_clean,
        upper(trim(cast(year_stamp_holo_dc as {{ dbt.type_string() }}))) as stamp_clean_up
    from base

),

classified as (

    select
        *,
        {{ extract_year_expr }} as explicit_year,
        {{ extract_letter_expr }} as stamp_letter,
        case
            when stamp_clean_up like '%SQUARE%' then 'square_shape'
            when stamp_clean_up like '%CIRCLE%' then 'circle_shape'
            when {{ has_digit_expr }} and {{ extract_letter_expr }} is not null then 'without_shape_new'
            when {{ extract_letter_expr }} is not null then 'without_shape_old'
            else null
        end as shape_type_guess
    from normalized

),

joined as (

    select
        c.*,
        m.year as mapped_year
    from classified c
    left join {{ ref('synthetic_year_code_mapping') }} m
      on c.shape_type_guess = m.shape_type
     and c.stamp_letter = m.letter

),

mapped as (

    select
        *,
        case
            when product_year_clean is not null
                then product_year_clean

            when explicit_year is not null
                then explicit_year

            when mapped_year is not null
                then cast(mapped_year as {{ dbt.type_string() }})

            when stamp_clean_up is null or stamp_clean_up = ''
                then 'Unknown'

            when lower(stamp_clean_up) in ('n/a', 'na')
                then 'Unknown'

            when stamp_clean_up in ('ไม่ระบุ', 'จำไม่ได้', 'ไม่ทราบ', 'ไม่แน่ใจ')
                then 'Unknown'

            when lower(stamp_clean_up) like '%metal plate%'
                then 'legacy code'

            when lower(stamp_clean_up) like '%holo%'
                then 'legacy code'

            when lower(stamp_clean_up) like '%microchip%'
                then 'legacy code'

            when stamp_letter is not null
                then 'legacy code'

            else 'Unknown'
        end as product_year_derived
    from joined

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
