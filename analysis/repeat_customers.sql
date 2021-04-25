-- this query finds the number of repeat customers and new customers since the first order date

with dim as (

    select * from {{ ref('dim_user_and_first_order') }}
),

facts as (

    select order_id, created_at from {{ ref('fct_payments_and_orders')}}
),

repeat_customers as (
    
    select
        min(created_at) as first_order_date,
        count(case when user_type = 'repeat' 
            then user_type end
            ) as repeat_customers,
        count( case when user_type = 'new' 
            then user_type end
            ) as new_customers

    from dim
    left join 
    facts on 
    dim.first_order_id = facts.order_id
    
   
)
select * from repeat_customers