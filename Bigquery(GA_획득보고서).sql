SELECT
    CONCAT(trafficsource.source," / ",trafficsource.medium) as source_medium,
    COUNT(distinct fullvisitorId) AS users,
    COUNT(totals.newVisits) AS new_users, 
    COUNT(DISTINCT CONCAT(fullvisitorId, CAST(visitId AS string), visitstarttime)) AS sessions,
    ROUND(SUM(totals.bounces)/COUNT(DISTINCT CONCAT(fullvisitorId, CAST(visitId AS string), visitstarttime))*100,2) AS bounce_rate,
    ROUND(SUM(totals.pageviews)/COUNT(DISTINCT CONCAT(fullvisitorId, CAST(visitId AS string), visitstarttime)),2) AS pageviews_per_session,
    ROUND(SUM(totals.timeOnSite)/COUNT(DISTINCT CONCAT(fullvisitorId, CAST(visitId AS string), visitstarttime)),0) AS average_session_duration,

  FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_*` WHERE _table_suffix BETWEEN '20211001' AND '20211231' AND totals.visits = 1
  GROUP BY 1
  ORDER BY 2 DESC
