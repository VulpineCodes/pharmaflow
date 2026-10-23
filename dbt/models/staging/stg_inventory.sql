-- Staging: 1:1 cleaned view over raw inventory imports. No business logic beyond typing/trimming.
select cast(sku as varchar) as sku, trim(product_name) as product_name,
  cast(quantity_on_hand as integer) as quantity_on_hand, cast(unit_cost as double) as unit_cost,
  cast(expiry_date as date) as expiry_date, cast(reorder_point as integer) as reorder_point,
  supplier, batch, _file_hash, _imported_at
from {{ source('raw','raw_inventory') }}
