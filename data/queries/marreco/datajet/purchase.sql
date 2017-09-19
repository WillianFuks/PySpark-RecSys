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
      "orderconfirmation" AS type,
      STRUCT(transaction.transactionId AS order_id, ARRAY(SELECT STRUCT(STRUCT(productBrand AS name) AS brand, STRUCT(productPrice / 10e6 AS `current`, productPrice / 10e6 AS previous) AS price, REGEXP_EXTRACT(productSKU, r'(.*)-\d+') AS group_id, [COALESCE(REGEXP_EXTRACT(productSKU, r'(.*)-\d+'), productSKU), productSKU] AS skus, ARRAY(SELECT AS STRUCT v AS name, REGEXP_REPLACE(v, ' ', '-') AS slug FROM UNNEST(SPLIT( v2productCategory, '|')) v) AS main_category_path) FROM UNNEST(product)) AS products, ARRAY(SELECT productQuantity FROM UNNEST(product)) AS quantities) AS details) event,
    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS created_at
  FROM UNNEST(hits) WHERE ecommerceaction.action_type = '6') data
FROM `{{dataset}}.ga_sessions_*`
WHERE True
AND EXISTS(SELECT 1 FROM UNNEST(hits) WHERE ecommerceaction.action_type = '6')
AND NOT EXISTS(SELECT 1 FROM UNNEST(hits), UNNEST(product) WHERE productSKU IS NULL OR productQuantity IS NULL)
AND _TABLE_SUFFIX BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{days_interval}} DAY)) AND FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL {{days_interval_end}} DAY))
),
UNNEST(data) data
