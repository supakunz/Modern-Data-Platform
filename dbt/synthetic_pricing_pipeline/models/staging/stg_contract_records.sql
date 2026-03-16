select
    
    cast(contract_num as {{ dbt.type_string() }})  as contract_num,

    cast(transaction_date as {{ dbt.type_timestamp() }}) as transaction_date,
    
    cast(form_id as {{ dbt.type_int() }})          as form_id,

    cast(brand as {{ dbt.type_string() }})         as brand,
    
    cast(model as {{ dbt.type_string() }})         as model,
    
    cast(sub_model as {{ dbt.type_string() }})     as sub_model,
    
    cast(size as {{ dbt.type_string() }})          as size,
    
    cast(color as {{ dbt.type_string() }})         as color,
    
    cast(hardware as {{ dbt.type_string() }})      as hardware,
    
    cast(material as {{ dbt.type_string() }})      as material,
    
    cast(picture_url as {{ dbt.type_string() }})   as picture_url,

    cast(condition as {{ dbt.type_string() }})     as condition,

    cast(year_stamp_holo_dc as {{ dbt.type_string() }}) as year_stamp_holo_dc,
    
    cast(product_year as {{ dbt.type_string() }})   as product_year,

    cast(estimate_amount as {{ dbt.type_int() }})   as estimate_amount,
    
    cast(actual_price as {{ dbt.type_int() }})      as actual_price

from {{ source('bronze', 'contract_records') }}
