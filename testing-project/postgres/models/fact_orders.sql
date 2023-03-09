select
    order_id
    ,order_country
    ,order_total
    ,had_discount
    ,customer_id
    ,to_date(order_date,'MM/DD/YYYY') as order_date
    ,round(order_total - (order_total/2)) as discount_total
from {{ref('fact_orders_source')}}
