-- Calculate the RFM features related to user orders on a daily basis.
-- An order is considered 'Completed' if its status is 'Completed' in the latest snapshot and not being 'Completed' in the previous snapshot.
{{ config(
     materialized='incremental',
     unique_key=['user_id', 'activity_date'],
     incremental_strategy='delete+insert'
) }}

 WITH previous_date_data AS (
     SELECT 
          *
     FROM {{ snapshot('orders_snapshot') }}
     WHERE dbt_valid_from <= current_date - interval '1 day'
     AND (dbt_valid_to > current_date - interval '1 day' OR dbt_valid_to IS NULL)
),
new_order_price AS (
     SELECT 
          user_id, 
          order_id,
          SUM(sale_price) AS order_amount
     FROM {{ source('thelook', 'order_items') }}
     {% if is_incremental() %}
          AND updated_at >= {{ var('start_date') }}
          AND updated_at < {{ var('end_date') }}
     {% endif %}
     GROUP BY user_id, order_id
)
SELECT 
     o.user_id,
     date(o.updated_at) AS activity_date,
     MAX(o.created_at) AS last_order_time,
     SUM(CASE WHEN o.status = 'Completed' AND (prev.status != 'Completed' OR prev.status IS NULL) THEN 1 ELSE 0 END) AS completed_order_count,
     SUM(CASE WHEN o.status = 'Cancelled' AND (prev.status != 'Cancelled' OR prev.status IS NULL) THEN 1 ELSE 0 END) AS cancelled_order_count,
     SUM(CASE WHEN o.status = 'Completed' AND (prev.status != 'Completed' OR prev.status IS NULL) THEN nop.order_amount ELSE 0 END) AS total_completed_order_amount,
     SUM(CASE WHEN o.status = 'Cancelled' AND (prev.status != 'Cancelled' OR prev.status IS NULL) THEN nop.order_amount ELSE 0 END) AS total_cancelled_order_amount
FROM {{ source('thelook', 'orders') }} AS o
LEFT JOIN previous_date_data AS prev ON prev.order_id = o.order_id
LEFT JOIN new_order_price AS nop ON nop.order_id = o.order_id
WHERE 1=1
{% if is_incremental() %}
     AND o.updated_at >= {{ var('start_date') }}
     AND o.updated_at < {{ var('end_date') }}
{% endif %}
GROUP BY o.user_id, activity_date;
