{{ config(
    materialized='table',
    schema='marts'
) }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_metrics AS (
    SELECT * FROM {{ ref('int_customer_order_summary') }}
),

final AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_metrics.first_order_date,
        customer_metrics.most_recent_order_date,
        customer_metrics.number_of_orders,
        customer_metrics.lifetime_value
    FROM customers
    LEFT JOIN customer_metrics
        ON customers.customer_id = customer_metrics.customer_id
    -- LEFT JOIN : garder TOUS les clients même sans commandes
)

SELECT * FROM final
