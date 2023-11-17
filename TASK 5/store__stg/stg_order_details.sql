with details as (
    select
    orders.order_date
    , details.quantity
    , details.price
    , products.name as product_name
    , brands.name as brand_name
    , orders.customer_phone
    , {{ normalize_phone_number('customer_phone') }} AS normalized_phone
from {{source('store', 'orders')}} as orders
join {{source('store', 'order_details')}} as details
    on orders.order_id = details.order_id 
join {{source('store', 'products')}} as products
    on details.product_id = products.product_id
join {{source('store', 'brands')}} as brands
    on products.brand_id = brands.brand_id
)

select *
    , case 
        when normalized_phone like '62%' then 'Indonesia'
        when normalized_phone like '91%' then 'India'
    end as country
from details 