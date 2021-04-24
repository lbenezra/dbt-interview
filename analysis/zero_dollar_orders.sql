with facts as (

    select * from {{ ref('fct_payments_and_orders') }} where total_amount = 0
)

select 
    count(case when order_status = 'paid' then total_amount end) as zero_dollar_orders
from facts