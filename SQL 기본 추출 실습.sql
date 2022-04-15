 SELECT fullVisitorId, 
        COUNT(visitid) AS Visit_Count,
        COUNTIF(device.deviceCategory = 'mobile') AS mobile,
        COUNTIF(device.deviceCategory = 'desktop') AS desktop,
        COUNTIF(device.deviceCategory = 'tablet') AS other
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` WHERE _table_suffix BETWEEN '20210701' AND '20210731'
GROUP BY fullVisitorId
ORDER BY Visit_Count DESC