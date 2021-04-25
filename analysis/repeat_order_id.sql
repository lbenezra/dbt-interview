-- this query finds the duplicate order IDs

with facts as (

    select * from {{ ref('fct_payments_and_orders') }}
)

select 
    order_id, 
    count(order_id) as order_id_count,

from facts
group by order_id
having count(order_id) > 1 
    