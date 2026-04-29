{{ config(
    materialized='table',
    schema='marts'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_payments') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        customers.first_name,
        customers.last_name,
        orders.order_date,
        orders.status,
        orders.total_amount
    FROM orders
    LEFT JOIN customers
        ON orders.customer_id = customers.customer_id
)

SELECT * FROM final
