-- Завдання 1. Перегляд REPEATED-полів для користувача

SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  event_name,
  event_params,
  user_properties,
  items
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
WHERE user_pseudo_id = '1540124.2144280285'
  AND EXISTS (
    SELECT 1
    FROM UNNEST(items) it
    WHERE it.item_name IS NOT NULL
      AND TRIM(it.item_name) != ''
      AND LOWER(it.item_name) NOT IN ('(not set)', 'not set')
  )
QUALIFY ROW_NUMBER() OVER (ORDER BY event_timestamp) = 1;


-- Завдання 2. Визначення розміру масивів

SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  event_name,
  event_params,
  user_properties,
  items,
  ARRAY_LENGTH(event_params) AS event_params_len,
  ARRAY_LENGTH(user_properties) AS user_properties_len,
  ARRAY_LENGTH(items) AS items_len
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
WHERE user_pseudo_id = '1540124.2144280285'
  AND EXISTS (
    SELECT 1
    FROM UNNEST(items) it
    WHERE it.item_name IS NOT NULL
      AND TRIM(it.item_name) != ''
      AND LOWER(it.item_name) NOT IN ('(not set)', 'not set')
  )
QUALIFY ROW_NUMBER() OVER (ORDER BY event_timestamp) = 1;




-- Завдання 3. Розгортання event_params

WITH one_event AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    event_params
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
  WHERE user_pseudo_id = '1540124.2144280285'
    AND EXISTS (
      SELECT 1
      FROM UNNEST(items) it
      WHERE it.item_name IS NOT NULL
        AND TRIM(it.item_name) != ''
        AND LOWER(it.item_name) NOT IN ('(not set)', 'not set')
    )
  QUALIFY ROW_NUMBER() OVER (ORDER BY event_timestamp) = 1
)
SELECT
  e.user_pseudo_id,
  e.event_name,
  ep.key,
  ep.value.string_value,
  ep.value.int_value,
  ep.value.double_value
FROM one_event e,
UNNEST(e.event_params) AS ep
ORDER BY ep.key;


-- Завдання 4. Аналіз частоти параметрів подій


SELECT
  ep.key,
  COUNT(*) AS key_frequency
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
UNNEST(event_params) AS ep
WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
GROUP BY ep.key
ORDER BY key_frequency DESC;

-- Завдання 5. Розгортання масиву items

SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  it.item_id,
  it.item_name,
  it.item_category,
  it.price,
  it.quantity
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
UNNEST(items) AS it;

-- Завдання 6. Зведена таблиця по товарах

SELECT
  it.item_id,
  it.item_name,
  COUNT(*) AS appearances_in_events,
  SUM(COALESCE(it.quantity, 0)) AS total_quantity,
  SUM(SAFE_CAST(it.price AS FLOAT64) * SAFE_CAST(it.quantity AS INT64)) AS total_revenue
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`,
UNNEST(items) AS it
WHERE it.item_name IS NOT NULL
  AND TRIM(it.item_name) != ''
  AND LOWER(it.item_name) NOT IN ('(not set)', 'not set')
GROUP BY it.item_id, it.item_name
ORDER BY total_revenue DESC;

-- Завдання 7. Фільтрація за значенням всередині ARRAY

SELECT
  user_pseudo_id,
  TIMESTAMP_MICROS(event_timestamp) AS event_datetime,
  event_name,
  items
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
WHERE EXISTS (
  SELECT 1
  FROM UNNEST(items) it
  WHERE it.item_category = 'Apparel'
);

-- Завдання 8. Робота з партиціями через _TABLE_SUFFIX

SELECT
  _TABLE_SUFFIX AS event_day,
  COUNT(DISTINCT user_pseudo_id) AS unique_users,
  COUNT(*) AS events_count,
  COUNTIF(event_name = 'purchase') AS purchase_events_count
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
GROUP BY event_day
ORDER BY event_day;

-- Завдання 9. Ранжування користувачів за витратами (на основі таблиць events_*)

WITH user_spend AS (
  SELECT
    user_pseudo_id,
    SUM(SAFE_CAST(it.price AS FLOAT64) * SAFE_CAST(it.quantity AS INT64)) AS total_spent
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS it
  GROUP BY user_pseudo_id
)
SELECT
  user_pseudo_id,
  total_spent,
  RANK() OVER (ORDER BY total_spent DESC) AS rnk,
  DENSE_RANK() OVER (ORDER BY total_spent DESC) AS dense_rnk,
  ROW_NUMBER() OVER (ORDER BY total_spent DESC) AS row_num
FROM user_spend
ORDER BY total_spent DESC
LIMIT 20;

-- Завдання 10. Нумерація подій у сесії (на основі items з таблиці events_20210131)

WITH base AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    (SELECT ep.value.int_value
     FROM UNNEST(event_params) ep
     WHERE ep.key = 'ga_session_id'
     LIMIT 1) AS ga_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131`
),
numbered AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    event_timestamp,
    event_name,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
    ) AS event_num_in_session
  FROM base
  WHERE ga_session_id IS NOT NULL
)
SELECT event_name
FROM numbered
WHERE event_num_in_session = 1
GROUP BY event_name
ORDER BY COUNT(*) DESC
LIMIT 1;
