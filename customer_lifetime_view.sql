with customer_data as (
    select
        id,
        first_name,
        last_name
    from {{ source('PC_HEVODATA_DB', 'pgsql_raw_customers') }}
),

order_data as (
    select
        customer_id,
        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(*) as number_of_orders
    from {{ source('PC_HEVODATA_DB', 'pgsql_raw_orders') }}
    group by customer_id
),

payment_data as (
    select
        customer_id,
        sum(payment_amount) as customer_lifetime_value
    from {{ source('PC_HEVODATA_DB', 'pgsql_raw_payments') }}
    group by customer_id
)

select
    c.id,
    c.first_name,
    c.last_name,
    o.first_order,
    o.most_recent_order,
    o.number_of_orders,
    p.customer_lifetime_value
from customer_data c
left join order_data o on c.id = o.customer_id
left join payment_data p on c.id = p.customer_id
