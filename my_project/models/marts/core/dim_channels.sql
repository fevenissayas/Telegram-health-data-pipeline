{{ config(
  materialized='table',
  schema='marts'
) }}
SELECT
  telethon_channel_id AS channel_id,
  channel_username,
  channel_title,
  MIN(message_timestamp) AS first_scraped_message_timestamp,
  MAX(message_timestamp) AS last_scraoed_message_timestamp,
  COUNT(DISTINCT message_id) AS total_message_scraped,
  MAX(scraped_date) AS latest_scraped_date
FROM
  {{ ref('stg_telegram_messages') }}
WHERE
  telethon_channel_id IS NOT NULL
GROUP BY
  telethon_channel_id,
  channel_username,
  channel_title
ORDER BY
  channel_id
