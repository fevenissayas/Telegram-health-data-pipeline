import os
import subprocess
from dagster import op, get_dagster_logger

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(PROJECT_ROOT, 'my_project')
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, 'scripts')
SCRAP_DIR = os.path.join(PROJECT_ROOT, 'src')

@op
def scrape_telegram_data_op():
  
    logger = get_dagster_logger()
    logger.info("Starting Telegram data scraping...")
    try:
        result = subprocess.run(
            ["python", os.path.join(SCRAP_DIR, "telegram_scraper.py")],
            check=True,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )
        logger.info(f"Telegram scraper stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Telegram scraper stderr:\n{result.stderr}")
        logger.info("Telegram data scraping completed.")
        return "Scraping complete" 
    except subprocess.CalledProcessError as e:
        logger.error(f"Telegram scraper failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during Telegram scraping: {e}")
        raise

@op
def load_raw_telegram_messages_op(upstream_result):
    
    logger = get_dagster_logger()
    logger.info("Starting raw Telegram messages loading to PostgreSQL...")
    try:
        result = subprocess.run(
            ["python", os.path.join(SCRIPTS_DIR, "load_to_postgres.py")],
            check=True,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )
        logger.info(f"Raw messages loader stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Raw messages loader stderr:\n{result.stderr}")
        logger.info("Raw Telegram messages loaded to PostgreSQL.")
        return "Raw messages loaded" 
    except subprocess.CalledProcessError as e:
        logger.error(f"Raw messages loader failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during raw messages loading: {e}")
        raise

@op
def run_yolo_detection_op(upstream_result):
    
    logger = get_dagster_logger()
    logger.info("Starting YOLO object detection on images...")
    try:
        result = subprocess.run(
            ["python", os.path.join(SCRIPTS_DIR, "yolo_detector.py")],
            check=True,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )
        logger.info(f"YOLO detector stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"YOLO detector stderr:\n{result.stderr}")
        logger.info("YOLO object detection completed.")
        return "YOLO detection complete" 
    except subprocess.CalledProcessError as e:
        logger.error(f"YOLO detector failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during YOLO detection: {e}")
        raise

@op
def load_yolo_detections_op(upstream_result):
    
    logger = get_dagster_logger()
    logger.info("Starting YOLO detections loading to PostgreSQL...")
    try:
        result = subprocess.run(
            ["python", os.path.join(SCRIPTS_DIR, "load_yolo_to_pg.py")],
            check=True,
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT
        )
        logger.info(f"YOLO detections loader stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"YOLO detections loader stderr:\n{result.stderr}")
        logger.info("YOLO detections loaded to PostgreSQL.")
        return "YOLO detections loaded" 
    except subprocess.CalledProcessError as e:
        logger.error(f"YOLO detections loader failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during YOLO detections loading: {e}")
        raise

@op
def run_dbt_transformations_op(messages_loaded_result, yolo_loaded_result):
    """
    Orchestrates running dbt transformations to build data warehouse.
    """
    logger = get_dagster_logger()
    logger.info("Starting dbt transformations...")
    try:
        dbt_command = ["dbt", "run", "--full-refresh"]
        
        result = subprocess.run(
            dbt_command,
            check=True,
            capture_output=True,
            text=True,
            cwd=DBT_PROJECT_DIR
        )
        logger.info(f"dbt run stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"dbt run stderr:\n{result.stderr}")
        logger.info("dbt transformations completed.")
        return "dbt transformations complete"
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt run failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during dbt transformations: {e}")
        raise

@op
def run_dbt_tests_op(dbt_result):

    logger = get_dagster_logger()
    logger.info("Starting dbt tests...")
    try:
        result = subprocess.run(
            ["dbt", "test"],
            check=True,
            capture_output=True,
            text=True,
            cwd=DBT_PROJECT_DIR
        )
        logger.info(f"dbt test stdout:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"dbt test stderr:\n{result.stderr}")
        logger.info("dbt tests completed.")
        return "dbt tests complete"
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt test failed: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during dbt tests: {e}")
        raise