import os
import json
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

YOLO_DETECTIONS_FILE = 'data/processed/yolo_detections.jsonl'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('data/yolo_loader.log'),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def create_raw_yolo_table(cursor):
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.raw_yolo_detections (
        id SERIAL PRIMARY KEY,
        message_id BIGINT NOT NULL,
        image_path TEXT NOT NULL,
        scraped_date DATE NOT NULL,
        channel_name TEXT,
        detected_object_class TEXT NOT NULL,
        confidence_score NUMERIC(5, 4) NOT NULL,
        detection_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        raw_detection_json JSONB NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_raw_yolo_detections_message_id ON raw.raw_yolo_detections (message_id);
    CREATE INDEX IF NOT EXISTS idx_raw_yolo_detections_object_class ON raw.raw_yolo_detections (detected_object_class);
    """
    try:
        cursor.execute(create_table_sql)
        logger.info("Raw table 'raw.raw_yolo_detections' ensured to exist.")
    except Exception as e:
        logger.error(f"Error creating raw YOLO table: {e}", exc_info=True)
        raise

def load_yolo_detections_to_postgres():
    """
    Loads YOLO detection records from JSONL file into PostgreSQL.
    """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        cursor = conn.cursor()

        create_raw_yolo_table(cursor)

        total_detections_loaded = 0
        
        if not os.path.exists(YOLO_DETECTIONS_FILE):
            logger.info(f"No YOLO detections file found at {YOLO_DETECTIONS_FILE}. Skipping load.")
            return

        with open(YOLO_DETECTIONS_FILE, 'r', encoding='utf-8') as f:
            detections_to_insert = []
            for line_num, line in enumerate(f):
                try:
                    record = json.loads(line.strip())
                    
                    # Basic validation and type conversion
                    message_id = record.get('message_id')
                    image_path = record.get('image_path')
                    scraped_date = record.get('scraped_date')
                    channel_name = record.get('channel_name')
                    detected_object_class = record.get('detected_object_class')
                    confidence_score = record.get('confidence_score')
                    detection_timestamp = record.get('timestamp')

                    if not all([message_id, image_path, detected_object_class, confidence_score, detection_timestamp]):
                        logger.warning(f"Skipping malformed record on line {line_num + 1} in {YOLO_DETECTIONS_FILE}: Missing required fields. Record: {line.strip()}")
                        continue
                    
                    detections_to_insert.append((
                        message_id,
                        image_path,
                        scraped_date,
                        channel_name,
                        detected_object_class,
                        confidence_score,
                        detection_timestamp,
                        json.dumps(record)
                    ))

                    if len(detections_to_insert) >= 100:
                        insert_sql = """
                        INSERT INTO raw.raw_yolo_detections (message_id, image_path, scraped_date, channel_name, detected_object_class, confidence_score, detection_timestamp, raw_detection_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
                        """
                        cursor.executemany(insert_sql, detections_to_insert)
                        conn.commit()
                        total_detections_loaded += cursor.rowcount
                        logger.info(f"Loaded {total_detections_loaded} YOLO detections so far...")
                        detections_to_insert = []

                except json.JSONDecodeError as jde:
                    logger.error(f"Error decoding JSON on line {line_num + 1} in {YOLO_DETECTIONS_FILE}: {jde}. Line: {line.strip()}", exc_info=True)
                except Exception as e:
                    logger.error(f"Error processing record on line {line_num + 1} in {YOLO_DETECTIONS_FILE}: {e}. Line: {line.strip()}", exc_info=True)
            
            if detections_to_insert:
                insert_sql = """
                INSERT INTO raw.raw_yolo_detections (message_id, image_path, scraped_date, channel_name, detected_object_class, confidence_score, detection_timestamp, raw_detection_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb);
                """
                cursor.executemany(insert_sql, detections_to_insert)
                conn.commit()
                total_detections_loaded += cursor.rowcount

        logger.info(f"Successfully loaded {total_detections_loaded} YOLO detections into PostgreSQL.")

    except psycopg2.Error as pg_err:
        logger.error(f"PostgreSQL connection or query error: {pg_err}", exc_info=True)
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("PostgreSQL connection closed.")

if __name__ == '__main__':
    if not all([POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT]):
        logger.error("PostgreSQL environment variables not fully set. Please check your .env file.")
        exit(1)
    
    load_yolo_detections_to_postgres()