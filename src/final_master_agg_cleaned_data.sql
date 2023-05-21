select 
  a.timestamp as timestamp_stockagg, 
  a.product_id as product_id_stockagg, 
  a.estimated_stock_pct, 
  b.timestamp as timestamp_salesagg, 
  b.product_id as product_id_salesagg, 
  b.quantity as quantity_salesagg, 
  c.timestamp as timestamp_stocktemp, 
  c.temperature as temp__sensor 
from 
  cleaned_data.stock_clean_level_data as a 
  left join cleaned_data.sales_clean_data as b on a.product_id = b.product_id 
			and a.timestamp = b.timestamp 
  left join cleaned_data.stock_clean_temp_data as c on a.timestamp = c.timestamp
