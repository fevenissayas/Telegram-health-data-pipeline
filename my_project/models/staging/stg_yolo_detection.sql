{{ config(
    materialized='view',
    schema='staging'
) }}

SELECT
    {{ dbt_utils.surrogate_key([
        'raw_detection.message_id',
        'raw_detection.detected_object_class',
        'raw_detection.confidence_score',
        'raw_detection.detection_timestamp'
    ]) }} AS image_detection_pk,
    raw_detection.message_id,
    raw_detection.image_path,
    raw_detection.scraped_date,
    raw_detection.channel_name,
    raw_detection.detected_object_class,
    raw_detection.confidence_score,
    raw_detection.detection_timestamp,
    raw_detection.loaded_at AS raw_loaded_at
FROM
    {{ source('raw', 'raw_yolo_detections') }} raw_detection