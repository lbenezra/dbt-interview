with dim as (

    select * from {{ ref('dim_user_and_first_order') }}
),

repeat_customers as (
    
    select 
        count(case when user_type = 'repeat' 
            then user_type end
            ) as repeat_customers,
        count( case when user_type = 'new' 
            then user_type end
            ) as new_customers

    from dim
   
)
select * from repeat_customers