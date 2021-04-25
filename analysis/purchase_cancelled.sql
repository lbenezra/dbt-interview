-- this query finds all the cancelled orders and associated dollar amount 

with facts as (

    select * from {{ ref('fct_payments_and_orders') }}
)

select 
    order_id, 
    order_status,
    case when order_status = 'cancelled' then total_amount end as dollar_amount_cancelled

from facts 
order by order_status

