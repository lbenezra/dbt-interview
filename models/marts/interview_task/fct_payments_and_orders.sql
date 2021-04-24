with payments as (

    select * from {{ ref('stg_payments') }}
),

orders as (

    select * from {{ ref('stg_orders') }}
),

devices as (

    select * from {{ ref('int_devices') }}
),

joined as (
    
    select 
        orders.order_id,
        orders.user_id,
        orders.created_at,
        orders.updated_at,
        orders.shipped_at,
        orders.currency,
        orders.order_status,
        orders.order_status_category,
        orders.shipping_method,
        payments.gross_tax_amount_cents / 100 as gross_tax_amount,
        payments.gross_amount_cents / 100 as gross_amount,
        payments.gross_amount_shipping_cents / 100 as gross_amount_shipping,
        payments.gross_total_amount_cents / 100 as gross_total_amount,
       case when orders.currency = 'USD' then orders.amount_total_cents / 100 
            else payments.gross_total_amount_cents / 100
        end as total_amount,
        devices.device,
        devices.purchase_device_type
    from orders
    left join payments
    on orders.order_id = payments.order_id
    left join devices
    on payments.order_id = devices.order_id

)

select * from joined 