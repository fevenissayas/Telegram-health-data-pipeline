{{ config(
  materialized='table',
  schema='marts',
) }}

WITH stg_messages AS (
  SELECT * FROM {{ ref('stg_telegram_messages') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_messages.message_id', 'stg_messages.telethon_channel_id']) }} AS message_pk,
  stg_messages.message_id,
  stg_messages.telethon_channel_id AS channel_id,
  DATE(stg_messages.message_timestamp) AS message_date,
  TO_CHAR(DATE(stg_messages.message_timestamp), 'YYYYMMDD')::INTEGER AS date_key,
  stg_messages.message_text,
  LENGTH(stg_messages.message_text) AS message_length,
  stg_messages.views_count,
  stg_messages.forwards_count,
  stg_messages.replies_count,
  stg_messages.media_type,
  stg_messages.has_media,
  stg_messages.media_file_name,
  stg_messages.raw_message_json,
  stg_messages.raw_loaded_at AS loaded_at
FROM
  stg_messages
WHERE
  stg_messages.telethon_channel_id IS NOT NULL
  {% if is_incremental() %}
    AND stg_messages.message_id NOT IN (
      SELECT message_id FROM {{ this }}
      WHERE channel_id = stg_messages.telethon_channel_id
    )
  {% endif %}