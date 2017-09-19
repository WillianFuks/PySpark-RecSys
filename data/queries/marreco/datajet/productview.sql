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
      "productview" AS type,
      STRUCT(STRUCT((SELECT v2ProductName FROM UNNEST(product)) AS title, STRUCT((SELECT productBrand FROM UNNEST(product)) AS name) AS brand, STRUCT((SELECT productPrice / 10e6 FROM UNNEST(product)) AS `current`, (SELECT productPrice / 10e6 FROM UNNEST(product)) AS `previous`) AS price, (SELECT productSKU FROM UNNEST(product)) AS group_id, ARRAY(SELECT productSKU FROM UNNEST(product)) AS skus, ARRAY(SELECT AS STRUCT v AS name, REGEXP_REPLACE(v, ' ', '-') AS slug FROM UNNEST(SPLIT((SELECT v2productCategory FROM UNNEST(product)), ',')) v) AS categories, ARRAY(SELECT AS STRUCT v AS name, REGEXP_REPLACE(v, ' ', '-') AS slug FROM UNNEST(SPLIT((SELECT v2productCategory FROM UNNEST(product)), ',')) v) AS main_category_path, page.pagePath AS url, ARRAY(SELECT page.pagePath FROM UNNEST(hits) LIMIT 1) AS images) AS product) AS details) event,
    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS created_at 
  FROM UNNEST(hits) WHERE ecommerceaction.action_type = '2') data
FROM `{{dataset}}.ga_sessions_*`
WHERE True
AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE ecommerceaction.action_type = '2')
AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{days_interval}} DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{days_interval_end}} DAY))
),
UNNEST(data) data
