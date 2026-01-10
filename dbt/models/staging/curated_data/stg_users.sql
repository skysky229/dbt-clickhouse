SELECT
  *
FROM {{ source('thelook', 'users') }}