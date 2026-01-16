-- Calculate daily features including event counts, last event details, session counts, and total session durations 
-- for each user_id, activity_date, browser, and traffic_source combination.

{{ config(
     materialized='incremental',
     unique_key=['user_id', 'activity_date', 'browser', 'traffic_source'],
     incremental_strategy='delete+insert'
) }}

WITH session_durations AS (
     SELECT 
          user_id,
          date(created_at) AS activity_date,
          browser,
          traffic_source,
          session_id,
          MAX(created_at) - MIN(created_at) AS session_duration
     FROM {{ source('thelook', 'events') }}
     WHERE 1=1
     {% if is_incremental() %}
          AND created_at >= {{ var('start_date') }}
          AND created_at < {{ var('end_date') }}
     {% endif %}
     GROUP BY user_id, activity_date, browser, traffic_source, session_id
)
SELECT 
     user_id,
     date(created_at) AS activity_date,
     browser,
     traffic_source,
     count(*) AS event_count,
     MAX(created_at) AS last_event_time,
     argMax(event_type, created_at) AS last_event_type,
     argMax(uri, created_at) AS last_event_uri,
     COUNT(DISTINCT session_id) AS session_count,
     SUM(session_duration) AS total_session_duration
FROM {{ source('thelook', 'events') }}
LEFT JOIN session_durations USING (user_id, activity_date, browser, traffic_source)
WHERE 1=1
{% if is_incremental() %}
     AND created_at >= {{ var('start_date') }}
     AND created_at < {{ var('end_date') }}
{% endif %}
GROUP BY user_id, activity_date, browser, traffic_source