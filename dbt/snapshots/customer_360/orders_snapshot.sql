{% snapshot orders_snapshot %}
{{
     config(
          target_schema='thelook',
          target_database='thelook',
          unique_key='order_id',
          strategy='check',
          check_cols=['status', 'updated_at']
     )
}}

SELECT * FROM {{ source('thelook', 'orders') }}

{% endsnapshot %}