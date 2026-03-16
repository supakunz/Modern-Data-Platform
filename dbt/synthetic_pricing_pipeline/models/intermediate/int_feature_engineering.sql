{% if target.type == 'bigquery' %}
  {% set product_year_int_expr = "safe_cast(product_year as int64)" %}
{% else %}
  {% set product_year_int_expr = "case when product_year ~ '^[0-9]{4}$' then product_year::int else null end" %}
{% endif %}

with base as (

    select
        *
    from {{ ref('int_year_hermes_mapping') }}
    where estimate_amount is not null
      and estimate_amount > 0

),

prepared as (

    select
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
        coalesce(product_year, 'Unknown') as product_year,
        coalesce(year_letter, 'Unknown') as year_letter,
        estimate_amount,
        color_segment
    from base

),

feature_product_year as (

    select
        *,
        case
            when product_year is null
                 or trim(product_year) = ''
                 or lower(product_year) = 'unknown'
                then 'unknown'
            when lower(product_year) in ('metal plate', 'legacy code')
                then 'legacy_code'
            when {{ product_year_int_expr }} >= 2023
                then 'recent'
            when {{ product_year_int_expr }} >= 2019
                then 'mid'
            when {{ product_year_int_expr }} is not null
                then 'legacy'
            else 'encoded'
        end as product_year_segment
    from prepared

),

mapped_condition as (

    select
        *,
        case
            when condition = '100% (New) สินค้าใหม่ ไม่เคยผ่านการใช้งาน และออกจากช๊อปไม่เกิน 12 เดือน' then 100
            when condition = '99% ไม่ผ่านการใช้งาน (Kept Unused) ไม่ผ่านการใช้งาน แต่อุปกรณ์ไม่ครบเหมือนออกจากช๊อป' then 99
            when condition = '95-98% เหมือนใหม่ (Like New) มีร่องรอยเพียงเล็กน้อยจากการจัดเก็บหรือทดลองใช้งาน' then 97
            when condition = '94-90% (Good Condition) ผ่านการใช้งาน มีตำหนิทั่วไป เช่น มุมถลอกหรือรอยขีดเล็กน้อย' then 92
            when condition = '89-85% (Fair Condition) มีตำหนิชัดเจน เช่น สีซีด มุมถลอก หรือหนังมีรอยชัดเจน' then 87
            when condition = '84-80% (Poor Condition) ทรงเริ่มเปลี่ยน สีซีดจาง หนังเริ่มเสียทรงหรือมีสีเฟดบางส่วน' then 82
            when condition = '79-70% (Damaged) ทรงเปลี่ยนชัดเจน สีเฟดชัดเจน มีรอยถลอกหรือขาดชัดเจน' then 75
            when condition = '69-60% (Breakage) สภาพทรุดโทรม มีตำหนิหลายจุด ผ่านการซ่อม/เปลี่ยนทรง/เปลี่ยนอะไหล่ หรือซ่อมสี' then 65
            when condition = 'ต่ำกว่า 59% (Reconsideration) สินค้าชำรุด หรือเปลี่ยนสีทั้งใบ/ทำสี ต้องได้รับการพิจารณาโดยเจ้าหน้าที่ผู้มีอำนาจ' then 59
            else null
        end as condition_score_raw
    from feature_product_year

),

median_calc as (
{% if target.type == 'bigquery' %}
    select
        coalesce(max(median_condition_score), 92) as median_condition_score
    from (
        select
            percentile_cont(condition_score_raw, 0.5) over() as median_condition_score
        from mapped_condition
        where condition_score_raw is not null
    ) s
{% else %}
    select
        coalesce(
            percentile_cont(0.5) within group (order by condition_score_raw),
            92
        ) as median_condition_score
    from mapped_condition
    where condition_score_raw is not null
{% endif %}
)

select
    m.contract_num,
    cast(m.transaction_date as date) as transaction_date,
    m.form_id,
    m.brand,
    m.model,
    m.sub_model,
    m.size,
    m.color,
    m.hardware,
    m.material,
    m.picture_url,
    m.condition,
    m.product_year,
    m.estimate_amount,
    m.color_segment,
    m.year_letter,
    m.product_year_segment,

    coalesce(m.condition_score_raw, mc.median_condition_score) as condition_score,

    case
        when coalesce(m.condition_score_raw, mc.median_condition_score) is null
            then 'Unknown'
        when coalesce(m.condition_score_raw, mc.median_condition_score) < 82.54416876904165
            then 'Low Condition'
        when coalesce(m.condition_score_raw, mc.median_condition_score) < 94.21111748929775
            then 'Medium Condition'
        else 'High Condition'
    end as condition_segment

from mapped_condition m
cross join median_calc mc
order by m.form_id
