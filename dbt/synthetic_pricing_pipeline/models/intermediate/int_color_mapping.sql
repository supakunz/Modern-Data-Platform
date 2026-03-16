select
    l.*,
    coalesce(c.color_segment, 'Unknown') as color_segment
from {{ ref('int_condition_mapping') }} l
left join {{ ref('synthetic_color_mapping') }} c
  on upper(l.brand) = upper(c.brand)
 and upper(l.color) = upper(c.color)
