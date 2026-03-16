with src as (

    select
        *,
        lower(coalesce(condition, '')) as condition_lc
    from {{ ref('stg_contract_records') }}

)

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

    case
        when condition is null then null

        when condition_lc like '%100%%'
             or condition_lc = 'new'
            then '100% (New) สินค้าใหม่ ไม่เคยผ่านการใช้งาน และออกจากช๊อปไม่เกิน 12 เดือน'

        when condition_lc like '%99%%'
             or condition_lc like '%unused%'
            then '99% ไม่ผ่านการใช้งาน (Kept Unused) ไม่ผ่านการใช้งาน แต่อุปกรณ์ไม่ครบเหมือนออกจากช๊อป'

        when condition_lc like '%95-98%'
             or condition_lc like '%premium%'
             or condition_lc like '%like new%'
            then '95-98% เหมือนใหม่ (Like New) มีร่องรอยเพียงเล็กน้อยจากการจัดเก็บหรือทดลองใช้งาน'

        when condition_lc like '%94-90%'
             or condition_lc like '%standard%'
             or condition_lc like '%good%'
            then '94-90% (Good Condition) ผ่านการใช้งาน มีตำหนิทั่วไป เช่น มุมถลอกหรือรอยขีดเล็กน้อย'

        when condition_lc like '%89-85%'
             or condition_lc like '%fair%'
            then '89-85% (Fair Condition) มีตำหนิชัดเจน เช่น สีซีด มุมถลอก หรือหนังมีรอยชัดเจน'

        when condition_lc like '%84-80%'
             or condition_lc like '%aged%'
             or condition_lc like '%poor%'
            then '84-80% (Poor Condition) ทรงเริ่มเปลี่ยน สีซีดจาง หนังเริ่มเสียทรงหรือมีสีเฟดบางส่วน'

        when condition_lc like '%79-70%'
             or condition_lc like '%worn%'
             or condition_lc like '%damaged%'
            then '79-70% (Damaged) ทรงเปลี่ยนชัดเจน สีเฟดชัดเจน มีรอยถลอกหรือขาดชัดเจน'

        when condition_lc like '%69-60%'
             or condition_lc like '%restore%'
             or condition_lc like '%breakage%'
            then '69-60% (Breakage) สภาพทรุดโทรม มีตำหนิหลายจุด ผ่านการซ่อม/เปลี่ยนทรง/เปลี่ยนอะไหล่ หรือซ่อมสี'

        when condition_lc like '%reconsideration%'
             or condition_lc like '%59%%'
            then 'ต่ำกว่า 59% (Reconsideration) สินค้าชำรุด หรือเปลี่ยนสีทั้งใบ/ทำสี ต้องได้รับการพิจารณาโดยเจ้าหน้าที่ผู้มีอำนาจ'

        else 'Unknown'
    end as condition,

    year_stamp_holo_dc,
    product_year,
    coalesce(actual_price, estimate_amount) as estimate_amount

from src
