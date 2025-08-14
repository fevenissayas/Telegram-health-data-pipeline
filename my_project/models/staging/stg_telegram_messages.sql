{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    (raw_json->>'id')::BIGINT AS message_id,
    (raw_json->>'date')::TIMESTAMP AS message_timestamp,
    (raw_json->'peer_id'->>'channel_id')::BIGINT AS telethon_channel_id,
    COALESCE(raw_json->>'channel_username', channel_username) AS channel_username,
    COALESCE(raw_json->>'channel_title', channel_title) AS channel_title,
    raw_json->>'message' AS message_text,
    (raw_json->>'views')::INTEGER AS views_count,
    (raw_json->>'forwards')::INTEGER AS forwards_count,
    CASE
        WHEN raw_json->'replies' IS NOT NULL AND raw_json->'replies'->>'replies' IS NOT NULL
        THEN (raw_json->'replies'->>'replies')::INTEGER
        ELSE 0
    END AS replies_count,
    raw_json->>'media_type' AS media_type,
    (raw_json->>'media_type') IS NOT NULL AS has_media,
    raw_json->'media'->'document'->'attributes'->0->>'file_name' AS media_file_name,
    raw_json AS raw_message_json,
    scraped_date,
    loaded_at AS raw_loaded_at
FROM
    {{ source('raw', 'raw_telegram_messages') }}