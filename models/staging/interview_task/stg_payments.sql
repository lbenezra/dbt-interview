with source as (

    select * from {{ source('interview_task', 'payments') }}

),

staging_payments as (
   
    select
    order_id,
    payment_id,
    payment_type,
    created_at,
    status,
    {% set cost_types = ["tax_amount_cents", "amount_cents", "amount_shipping_cents"]%} 
    {% for cost_type in cost_types %}
    sum(
        case when status = 'completed'
                then {{cost_type}}
             else 0
        end
    ) as gross_{{cost_type}},
    {% endfor %}

    sum(
        case when status = 'completed'
             then tax_amount_cents + amount_cents + amount_shipping_cents
        else 0
        end
    ) as gross_total_amount_cents

    from source
    group by order_id, payment_id, created_at, status, payment_type
)

select * from staging_payments

