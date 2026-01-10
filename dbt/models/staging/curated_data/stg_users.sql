SELECT
  *
FROM {{ source('thelook', 'users') }} LIMIT 10  