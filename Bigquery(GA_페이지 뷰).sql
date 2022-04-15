- 최종 쿼리(페이지뷰). 평균 페이지 시간은 따로 해서 inner join 해야함 
- 왜냐하면 페이지 시간은 type page 처리가 필요하고 이외의 것들은 필요가 없.

  WITH avg_time AS (
SELECT 
  page, 
  SUM(TIMEOnPage) AS TimeOnPage, 
  SUM(Exits) AS exits, 
  SUM(Pageviews) AS pageviews,
  SAFE_DIVIDE(SUM(TIMEOnPage),(SUM(pageviews)-SUM(exits))) AS avg_time_on_page -- 페이지에 총 머문 시간/(총 페이지뷰 - 총 이탈), '0으로 나누기' 오류를 처리 safe_divide
  
 FROM(
   SELECT
     Sessions, 
     Page, 
     PageViews,
     CASE WHEN exit = TRUE THEN LastInteraction - hitTime ELSE LEAD(hitTime) OVER (PARTITION BY Sessions ORDER BY hitNum) - hitTime END AS TimeOnPage, -- lead : 다음 행의 값을 반환하는 함수
    Exits 
    
  FROM (
    SELECT
       CASE WHEN totals.visits=1 THEN CONCAT(fullvisitorid, CAST(visitNumber AS STRING),CAST(visitStartTime AS STRING)) END AS Sessions,
       CASE WHEN Type="PAGE" AND totals.visits=1 THEN 1 ELSE 0 END AS PageViews,
       hits.Page.pagePath AS Page,
       hits.IsExit AS exit,
       CASE WHEN hits.Isexit =TRUE THEN 1 ELSE 0 END AS Exits,
       hits.hitNUmber AS hitNum, --         순서대로 배열된 조회수입니다. 각 세션의 첫 번째 조회에 대해서는 1로 설정
       hits.Type AS hitType, 
       hits.time/1000 AS hitTime, -- visitStartTime 이후 경과한 시간(단위: 밀리초). 첫 번째 조회의 hits.time은 0
       MAX(IF(hits.isInteraction =TRUE, hits.time/1000,0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS LastInteraction -- partition by 파티션_컬럼
       
 
 FROM
 `bigquery-public-data.google_analytics_sample.ga_sessions_*` , unnest(hits) AS hits  WHERE _table_suffix BETWEEN '20210101' AND '20211231'
    )
  WHERE hitType = 'PAGE'  
    )
  GROUP BY 1 
)



SELECT
  pagepath,
  pageviews,
  unique_pageviews,
  avg_time_on_page,
  entraces,
  ROUND(CASE
    WHEN sessions = 0 THEN 0
    ELSE bounces / sessions
  END * 100,2) AS bounce_rate,
  exit_rate
 
FROM (
  SELECT
  pagePath,
  COUNT(CASE WHEN hitType ='PAGE' THEN 1  ELSE NULL END) AS pageviews,
  COUNT(DISTINCT CASE WHEN hitType ='PAGE' THEN CONCAT(fullvisitorid, visitid, visitstarttime, pagepath, pagetitle) ELSE NULL END) AS unique_pageviews,
  SUM(bounces) AS bounces,
  SUM(sessions) AS sessions,
  SUM(CASE WHEN entrance IS NOT NULL THEN 1 ELSE 0 END) AS entraces,
  ROUND(SUM(CASE WHEN isExit = true AND hitType ='PAGE' THEN 1 ELSE 0 END)/SUM(CASE WHEN hitType ='PAGE' THEN 1 ELSE NULL END)*100,2) AS exit_rate,
 ROUND(AVG(avg_time_on_page),0) AS avg_time_on_page,
      
    FROM (
      SELECT
        hitType,
        fullVisitorId,
        visitStartTime,
        visitid,
        pagepath,
        pagetitle,
        entrance,
        isExit,
        hitNumber,
         CASE
          WHEN hitNumber = first_interaction THEN bounces
          ELSE 0
        END AS bounces,
        CASE
          WHEN hitNumber = first_hit THEN visits
          ELSE 0
        END AS sessions,
        avg_time_on_page
                      
        FROM (
          SELECT
            hits.type AS hitType,
            fullVisitorId,
            visitStartTime,
            visitid,
            ifnull(hits.page.pagetitle,'') AS pagetitle,
            ifnull(hits.page.pagepath,'') AS pagepath,
            hits.isentrance AS entrance,
            hits.isExit AS isExit,
            totals.bounces,
            totals.visits,
            hits.hitNumber,
            MIN(IF(hits.isInteraction IS NOT NULL,
                hits.hitNumber,
                0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction,
            MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_hit,
            avg_time_on_page

            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` , UNNEST(hits) AS hits
            LEFT JOIN avg_time ON hits.page.pagepath = avg_time.page
  WHERE _table_suffix BETWEEN '20210101' AND '20211231'))
  
  GROUP BY
    pagePath)
ORDER BY
  pageviews DESC
