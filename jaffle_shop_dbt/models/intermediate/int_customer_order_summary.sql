{{ config(
    materialized='view',
    schema='intermediate'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_orders_with_payments') }}
),

customer_metrics AS (
    SELECT
        customer_id,
        MIN(order_date)    AS first_order_date,
        MAX(order_date)    AS most_recent_order_date,
        COUNT(order_id)    AS number_of_orders,
        SUM(total_amount)  AS lifetime_value
    FROM orders
    GROUP BY customer_id
)

SELECT * FROM customer_metrics
