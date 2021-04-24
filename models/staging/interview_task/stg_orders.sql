with source as (

    select * from {{ source('interview_task', 'orders') }}

),

staging_orders as (

    select 
        order_id, 
        user_id,
        created_at,
        updated_at, 
        shipped_at,
        currency, 
        status as order_status,
        case when status in (
            'paid',
            'completed',
            'shipped'
            ) 
        then 'completed'
        else status
        end as order_status_category,
        shipping_method,
        amount_total_cents
    from source

)

select * from staging_orders 