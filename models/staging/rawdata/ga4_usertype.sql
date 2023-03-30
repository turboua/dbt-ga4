SELECT event_date_dt, user_pseudo_id, CASE
    WHEN DENSE_RANK() OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp, event_name) = 1 
    THEN 'new' ELSE 'returning'
END
  AS customer_type FROM {{ ref("base_ga4__events") }} where event_name = 'session_start'