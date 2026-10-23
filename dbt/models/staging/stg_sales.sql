select cast(sale_id as varchar) as sale_id, cast(sku as varchar) as sku,
  cast(quantity_sold as integer) as quantity_sold, cast(sale_date as date) as sale_date,
  cast(unit_price as double) as unit_price, _file_hash, _imported_at
from {{ source('raw','raw_sales') }}
