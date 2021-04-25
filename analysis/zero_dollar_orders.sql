-- this query categorizes the orders that were completed with a zero dollar total amount

with facts as (

    select * from {{ ref('fct_payments_and_orders') }} 
)

select 
    order_id, 
    case when total_amount = 0 and order_status_category != 'cancelled' 
         then 'zero dollar total' 
         when total_amount != 0 and order_status_category = 'cancelled'
         then 'cancelled order'
         else
         'no error' 
    end as zero_dollar_status,
    order_status_category,
    total_amount
    
from facts

order by zero_dollar_status desc