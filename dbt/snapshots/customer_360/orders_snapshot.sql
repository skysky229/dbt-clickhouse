{% snapshot orders_snapshot %}
{{
     config(
          target_schema='thelook',
          unique_key='order_id',
          strategy='check',
          check_cols=['status', 'updated_at']
     )
}}

SELECT *, updated_at FROM {{ source('thelook', 'orders') }}

{% endsnapshot %}