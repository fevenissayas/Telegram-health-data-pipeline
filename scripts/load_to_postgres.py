import os
import json
import psycopg2
import datetime
import logging
from dotenv import load_dotenv

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

RAW_DATA_LAKE_MESSAGES_DIR = 'data/raw/telegram_messages'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('data/raw_loader.log'),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

def create_raw_table(cursor):
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.raw_telegram_messages (
        id SERIAL PRIMARY KEY,
        message_id BIGINT UNIQUE NOT NULL,
        channel_username TEXT,
        channel_title TEXT,
        scraped_date DATE NOT NULL,
        raw_json JSONB NOT NULL,
        loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_raw_telegram_messages_message_id ON raw.raw_telegram_messages (message_id);
    CREATE INDEX IF NOT EXISTS idx_raw_telegram_messages_scraped_date ON raw.raw_telegram_messages (scraped_date);
    """
    try:
        cursor.execute(create_table_sql)
        logger.info("Raw table 'raw.raw_telegram_messages' ensured to exist.")
    except Exception as e:
        logger.error(f"Error creating raw table: {e}", exc_info=True)
        raise

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return str(obj)
        elif hasattr(obj, 'to_dict'):
            return obj.to_dict()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)

def load_json_to_postgres():
    """
    Loads JSON files from the data lake into the PostgreSQL raw.raw_telegram_messages table.
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

        create_raw_table(cursor)

        total_files_processed = 0
        total_messages_loaded = 0
        
        for date_dir in os.listdir(RAW_DATA_LAKE_MESSAGES_DIR):
            full_date_dir_path = os.path.join(RAW_DATA_LAKE_MESSAGES_DIR, date_dir)
            if not os.path.isdir(full_date_dir_path):
                continue
            
            scraped_date_str = os.path.basename(date_dir)
            try:
                scraped_date = datetime.datetime.strptime(scraped_date_str, '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"Skipping directory {date_dir}: Invalid date format.")
                continue

            for channel_dir in os.listdir(full_date_dir_path):
                full_channel_dir_path = os.path.join(full_date_dir_path, channel_dir)
                if not os.path.isdir(full_channel_dir_path):
                    continue

                messages_to_insert = []

                for filename in os.listdir(full_channel_dir_path):
                    if filename.endswith('.json'):
                        file_path = os.path.join(full_channel_dir_path, filename)
                        total_files_processed += 1
                        
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                raw_message_data = json.load(f)

                            message_id = raw_message_data.get('id')
                            channel_username = raw_message_data.get('channel_username')
                            channel_title = raw_message_data.get('channel_title')
                            
                            if message_id is None:
                                logger.warning(f"Skipping file {file_path}: 'id' not found in JSON.")
                                continue

                            messages_to_insert.append((
                                message_id,
                                channel_username,
                                channel_title,
                                scraped_date,
                                json.dumps(raw_message_data, cls=CustomEncoder)
                            ))

                        except json.JSONDecodeError as jde:
                            logger.error(f"Error decoding JSON from {file_path}: {jde}", exc_info=True)
                        except Exception as e:
                            logger.error(f"Error reading/parsing file {file_path}: {e}", exc_info=True)

                if messages_to_insert:
                    insert_sql = """
                    INSERT INTO raw.raw_telegram_messages (message_id, channel_username, channel_title, scraped_date, raw_json)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (message_id) DO NOTHING;
                    """
                    try:
                        cursor.executemany(insert_sql, messages_to_insert)
                        conn.commit() 
                        total_messages_loaded += cursor.rowcount 
                        logger.info(f"Loaded {cursor.rowcount} new messages from channel '{channel_dir}' in date '{date_dir}'. Total loaded: {total_messages_loaded}")
                    except psycopg2.errors.UniqueViolation as uv:
                         logger.warning(f"Unique violation during batch insert for channel '{channel_dir}' on {date_dir}. Some messages might be duplicates. {uv}", exc_info=True)
                         conn.rollback() 
                    except psycopg2.Error as pg_err:
                        logger.error(f"PostgreSQL batch insert error for channel '{channel_dir}' on {date_dir}: {pg_err}", exc_info=True)
                        conn.rollback()
                    except Exception as e:
                        logger.error(f"An unexpected error during batch insert for channel '{channel_dir}' on {date_dir}: {e}", exc_info=True)
                        conn.rollback()

        logger.info(f"Successfully processed {total_files_processed} files. Loaded {total_messages_loaded} total new messages into PostgreSQL.")

    except psycopg2.Error as pg_err:
        logger.error(f"PostgreSQL connection or initial table creation error: {pg_err}", exc_info=True)
        if conn:
            conn.rollback()
    except Exception as e:
        logger.error(f"An unexpected error occurred during overall process: {e}", exc_info=True)
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
    
    load_json_to_postgres()