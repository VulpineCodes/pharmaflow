-- Mart: current inventory per SKU (latest import wins in the Python core; here: latest _imported_at).
select sku, product_name, quantity_on_hand, reorder_point, unit_cost, expiry_date,
  quantity_on_hand <= reorder_point as is_low_stock,
  expiry_date <= current_date + interval 30 day and expiry_date >= current_date as is_expiring_soon
from {{ ref('stg_inventory') }}
qualify row_number() over (partition by sku order by _imported_at desc) = 1
