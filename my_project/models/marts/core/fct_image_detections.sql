{{ config(
    materialized='incremental',
    unique_key='image_detection_pk',
    schema='marts',
    post_hook="CREATE UNIQUE INDEX IF NOT EXISTS fct_image_detections_unique_idx ON marts.fct_image_detections (image_detection_pk);"
) }}

WITH stg_detections AS (
    SELECT * FROM {{ ref('stg_yolo_detections') }}
),
fct_messages_base AS (
    SELECT
        message_pk,
        message_id,
        channel_id
    FROM
        {{ ref('fct_messages') }}
)
SELECT
    sd.image_detection_pk,
    sd.message_id,
    fmb.message_pk,
    sd.image_path,
    sd.scraped_date,
    sd.channel_name,
    sd.detected_object_class,
    sd.confidence_score,
    sd.detection_timestamp,
    sd.raw_loaded_at AS loaded_at
FROM
    stg_detections sd
INNER JOIN
    fct_messages_base fmb ON sd.message_id = fmb.message_id
WHERE
    1=1
    {% if is_incremental() %}
        AND sd.detection_timestamp > (SELECT MAX(detection_timestamp) FROM {{ this }})
    {% endif %}