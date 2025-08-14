from dagster import job, repository, schedule
from .ops import (
    scrape_telegram_data_op,
    load_raw_telegram_messages_op,
    run_yolo_detection_op,
    load_yolo_detections_op,
    run_dbt_transformations_op,
    run_dbt_tests_op
)

@job(name="telegram_data_pipeline")
def telegram_data_pipeline_job():

    scraped_result = scrape_telegram_data_op()

    loaded_raw_messages_result = load_raw_telegram_messages_op(scraped_result) 

    yolo_detected_images_result = run_yolo_detection_op(scraped_result)

    loaded_yolo_detections_result = load_yolo_detections_op(yolo_detected_images_result)

    dbt_transformed_result = run_dbt_transformations_op(loaded_raw_messages_result, loaded_yolo_detections_result)

    run_dbt_tests_op(dbt_transformed_result)

@schedule(
    cron_schedule="0 0 * * *",
    job_name="telegram_data_pipeline",
    execution_timezone="Africa/Addis_Ababa"
)
def daily_telegram_pipeline_schedule(context):
    return {}

@repository
def telegram_health_insights_repo():
    return [
        telegram_data_pipeline_job,
        daily_telegram_pipeline_schedule
    ]