{{ config(
     materialized='table'
) }}

-- 1. Base Events with period windows
WITH events_base AS (
     SELECT 
          user_id,
          activity_date,
          traffic_source,
          event_count,
          session_count,
          total_session_duration,
          last_event_time
     FROM {{ ref('int_daily_user_events') }}
     WHERE activity_date <= {{ var('job_date') }}
),

-- 2. Calculate Rankings (Sum sessions over period then rank)
traffic_source_stats AS (
     SELECT 
          user_id,
          traffic_source,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN session_count ELSE 0 END) as sessions_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN session_count ELSE 0 END) as sessions_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN session_count ELSE 0 END) as sessions_30d,
          SUM(session_count) as sessions_total
     FROM events_base
     GROUP BY user_id, traffic_source
),

rankings AS (
     SELECT 
          user_id,
          traffic_source,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY sessions_1d DESC, sessions_total DESC) as rank_1d,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY sessions_7d DESC, sessions_total DESC) as rank_7d,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY sessions_30d DESC, sessions_total DESC) as rank_30d,
          ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY sessions_total DESC) as rank_total
     FROM traffic_source_stats
),

most_popular_sources AS (
     SELECT
          user_id,
          MAX(CASE WHEN rank_1d = 1 THEN traffic_source END) as most_popular_traffic_source_1d,
          MAX(CASE WHEN rank_7d = 1 THEN traffic_source END) as most_popular_traffic_source_7d,
          MAX(CASE WHEN rank_30d = 1 THEN traffic_source END) as most_popular_traffic_source_30d,
          MAX(CASE WHEN rank_total = 1 THEN traffic_source END) as most_popular_traffic_source_total
     FROM rankings
     GROUP BY user_id
),

-- 3. Aggregate Event Metrics
user_event_agg AS (
     SELECT 
          user_id,
          MAX(last_event_time) AS last_event_time,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN event_count ELSE 0 END) AS events_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN event_count ELSE 0 END) AS events_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN event_count ELSE 0 END) AS events_30d,
          SUM(event_count) AS events_total,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN total_session_duration ELSE 0 END) AS total_session_duration_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN total_session_duration ELSE 0 END) AS total_session_duration_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN total_session_duration ELSE 0 END) AS total_session_duration_30d,
          SUM(total_session_duration) AS total_session_duration_total
     FROM events_base
     GROUP BY user_id
),

order_base AS (
     SELECT 
          user_id,
          activity_date,
          last_order_time,
          completed_order_count,
          cancelled_order_count,
          total_completed_order_amount,
          total_cancelled_order_amount
     FROM {{ ref('int_daily_user_orders') }}
     WHERE activity_date <= {{ var('job_date') }}
),
user_order_agg AS (
     SELECT 
          user_id,
          MAX(last_order_time) AS last_order_time,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN completed_order_count ELSE 0 END) AS completed_orders_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN completed_order_count ELSE 0 END) AS completed_orders_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN completed_order_count ELSE 0 END) AS completed_orders_30d,
          SUM(completed_order_count) AS completed_orders_total,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN cancelled_order_count ELSE 0 END) AS cancelled_orders_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN cancelled_order_count ELSE 0 END) AS cancelled_orders_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN cancelled_order_count ELSE 0 END) AS cancelled_orders_30d,
          SUM(cancelled_order_count) AS cancelled_orders_total,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN total_completed_order_amount ELSE 0 END) AS total_completed_amount_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN total_completed_order_amount ELSE 0 END) AS total_completed_amount_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN total_completed_order_amount ELSE 0 END) AS total_completed_amount_30d,
          SUM(total_completed_order_amount) AS total_completed_amount_total,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) = 0 THEN total_cancelled_order_amount ELSE 0 END) AS total_cancelled_amount_1d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 7 THEN total_cancelled_order_amount ELSE 0 END) AS total_cancelled_amount_7d,
          SUM(CASE WHEN dateDiff('day', activity_date, toDate({{ var('job_date') }})) < 30 THEN total_cancelled_order_amount ELSE 0 END) AS total_cancelled_amount_30d,
          SUM(total_cancelled_order_amount) AS total_cancelled_amount_total
     FROM order_base
     GROUP BY user_id
)

SELECT 
     e.user_id as user_id,
     e.last_event_time,
     dateDiff('day', e.last_event_time, toDate({{ var('job_date') }})) AS days_since_last_event,
     e.events_1d,
     e.events_7d,
     e.events_30d,
     e.events_total,
     e.total_session_duration_1d,
     e.total_session_duration_7d,
     e.total_session_duration_30d,
     e.total_session_duration_total,
     p.most_popular_traffic_source_7d,
     p.most_popular_traffic_source_30d,
     p.most_popular_traffic_source_total,

     o.last_order_time,
     dateDiff('day', o.last_order_time, toDate({{ var('job_date') }})) AS days_since_last_order,

     o.completed_orders_1d,
     o.completed_orders_7d,
     o.completed_orders_30d,
     o.completed_orders_total,

     o.cancelled_orders_1d,
     o.cancelled_orders_7d,
     o.cancelled_orders_30d,
     o.cancelled_orders_total,

     o.total_completed_amount_1d,
     o.total_completed_amount_7d,
     o.total_completed_amount_30d,
     o.total_completed_amount_total,

     o.total_cancelled_amount_1d,
     o.total_cancelled_amount_7d,
     o.total_cancelled_amount_30d,
     o.total_cancelled_amount_total,

     o.total_completed_amount_1d / o.completed_orders_1d AS avg_completed_order_amount_1d,
     o.total_completed_amount_7d / o.completed_orders_7d AS avg_completed_order_amount_7d,
     o.total_completed_amount_30d / o.completed_orders_30d AS avg_completed_order_amount_30d,
     o.total_completed_amount_total / o.completed_orders_total AS avg_completed_order_amount_total,

     o.total_cancelled_amount_1d / o.cancelled_orders_1d AS avg_cancelled_order_amount_1d,
     o.total_cancelled_amount_7d / o.cancelled_orders_7d AS avg_cancelled_order_amount_7d,
     o.total_cancelled_amount_30d / o.cancelled_orders_30d AS avg_cancelled_order_amount_30d,
     o.total_cancelled_amount_total / o.cancelled_orders_total AS avg_cancelled_order_amount_total,

     CASE WHEN dateDiff('day', e.last_event_time, toDate({{ var('job_date') }})) > 90 THEN TRUE ELSE FALSE END AS is_churned
FROM user_event_agg e
LEFT JOIN most_popular_sources p ON e.user_id = p.user_id
FULL OUTER JOIN user_order_agg o ON e.user_id = o.user_id;