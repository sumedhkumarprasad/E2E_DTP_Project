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
  processed_data.stock_level_agg as a 
  left join processed_data.sales_agg as b on a.product_id = b.product_id 
			and a.timestamp = b.timestamp 
  left join processed_data.df_stock_temp_agg as c on a.timestamp = c.timestamp
