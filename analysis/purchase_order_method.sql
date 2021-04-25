-- this query counts orders by each order method

with facts as (

    select * from {{ ref('fct_payments_and_orders') }}
),

purchase_method as (
    
    select 

        {% set methods = ['desktop', 'mobile_app', 'mobile_web', 'unknown', 'error'] %} 
        {% for method in methods %}
        count(
            case when purchase_device_type  = '{{method}}'
                then purchase_device_type
            end
            ) as sold_by_{{method}}

            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}

    from facts 
)
select * from purchase_method