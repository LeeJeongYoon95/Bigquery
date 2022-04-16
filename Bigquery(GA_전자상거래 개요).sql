SELECT 
  CAST(ROUND(SUM(totals.totalTransactionRevenue)/1000000,0) AS INT64) AS revenue,
  ROUND(SUM(totals.transactions)/COUNT(totals.visits) *100,2) AS conversion_rate, --총 거래/총 방문자
  SUM(totals.transactions) AS transactions, -- 총 거래 
  CAST(ROUND(ROUND(SUM(totals.totalTransactionRevenue),0)/SUM(totals.transactions),0)/1000000 AS INT64) AS avg_order_value
  
FROM 
  `{{프로젝트 ID}}.{{GA 속성 번호}}.ga_sessions_{{Date}}` 
WHERE
   _TABLE_SUFFIX BETWEEN '20200101' AND '20211231' 