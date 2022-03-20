 -- GA 잠재고객 보고서
 SELECT
   COUNT(DISTINCT(fullVisitorId)) AS USER,  -- 1. 사용자
   COUNTIF(totals.newVisits = 1) AS NEW_VISITOR,  -- 2. 신규 방문자
   SUM(totals.visits) AS SESSION,   -- 3. 세션 -- COUNT(DISTINCT(CONCAT(fullVisitorId, visitId, visitStartTime))) AS SESSION2, -- AND totals.visits = 1 조건 시 동일 
   ROUND(SUM(totals.visits) / COUNT(DISTINCT(fullVisitorId)), 2) AS SESSION_PER_USER,  -- 4. 사용자당 세션 수
   SUM(totals.pageviews) as PAGEVIEW,  -- 5. 페이지뷰 수
   ROUND(SUM(totals.pageviews) / SUM(totals.visits), 2) AS PAGEVIEW_PER_SESSION,  -- 6. 세션당 페이지 수
   ROUND(SUM(totals.timeonsite)/ SUM(totals.visits)) AS AVG_SESSIOIN_DURATION,  -- 7. 평균 세션 시간
   ROUND(SUM(totals.bounces) / SUM(totals.visits) * 100, 2) AS BOUNCE_RATE  -- 8. 이탈률
FROM `bhjeong-1.171781649.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20220101' AND '20220131'




-- GA 이벤트 개요 보고서
SELECT
  hits.eventInfo.eventCategory as c,
  COUNT(*) events,
  COUNT(DISTINCT CONCAT(fullvisitorid,visitId, visitstartTime, COALESCE(hits.eventinfo.eventCategory,''), 
                                                 COALESCE(hits.eventinfo.eventaction,''), COALESCE(hits.eventinfo.eventlabel, ''))) uniqueEvents
FROM
  `bhjeong-1.171781649.ga_sessions_*` t, unnest(hits) as hits
WHERE
  _TABLE_SUFFIX BETWEEN '20210101' AND '20211231' AND hits.type='EVENT' AND hits.eventInfo.eventCategory is not null
GROUP BY 1
ORDER BY 2 DESC