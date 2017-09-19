#standardSQL
SELECT
data.*
FROM(
SELECT 
ARRAY(
  SELECT AS STRUCT
    STRUCT(1 as schema_version, STRUCT(STRUCT(NULL) AS location, "" AS gender) as user,
      STRUCT(STRUCT("11" AS value, "bid" AS type) AS bid, STRUCT("_392" AS value, "customer_user_id" AS type) AS customer_user_id, STRUCT(fullvisitorid AS value, "djUCID" as type) AS djUCID) AS identifiers,
      STRUCT(device.browser AS client, device.operatingSystem AS os, device.deviceCategory AS origin) AS device,
      STRUCT("fish" AS tracker, page.pagePath AS url, referer AS url_referrer) AS source,
      UNIX_MILLIS(CURRENT_TIMESTAMP()) AS created_at,
      UNIX_MILLIS(CURRENT_TIMESTAMP()) AS local_timestamp,
      "search" AS type,
      STRUCT(REGEXP_EXTRACT(page.pagePath, r'/\?q=(.*)') AS query, "keyword" AS query_type) AS details) event,
    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS created_at 
  FROM UNNEST(hits) WHERE REGEXP_CONTAINS(page.pagePath, r'/\?q=')) data
FROM `{{dataset}}.ga_sessions_*`
WHERE True
AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE REGEXP_CONTAINS(page.pagePath, r'/\?q='))
AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{days_interval}} DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{days_interval_end}} DAY))
),
UNNEST(data) data
