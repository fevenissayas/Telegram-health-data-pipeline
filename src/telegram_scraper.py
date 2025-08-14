import asyncio
import json
import os
import datetime
import logging
from telethon import TelegramClient, errors
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_scraper_session")

BASE_DATA_DIR = 'data'
RAW_DATA_LAKE_MESSAGES_DIR = os.path.join(BASE_DATA_DIR, 'raw', 'telegram_messages')
RAW_DATA_LAKE_IMAGES_DIR = os.path.join(BASE_DATA_DIR, 'raw', 'telegram_images')
SESSION_DIR = os.path.join(BASE_DATA_DIR, 'sessions')

os.makedirs(RAW_DATA_LAKE_MESSAGES_DIR, exist_ok=True)
os.makedirs(RAW_DATA_LAKE_IMAGES_DIR, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(BASE_DATA_DIR, 'scraper.log')),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

channels = [
    "@CheMed123",
    "@lobelia4cosmetics",
    "@tikvahpharma",
    "@tenamereja",
    "@ethiopianfoodanddrugauthority",
    "@Thequorachannel",
    "@HakimApps_Guideline"
]

IMAGE_CHANNELS = [
    "@CheMed123",
    "@lobelia4cosmetics"
]

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return str(obj)
            return obj.to_dict()
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)

async def connect_and_scrape():
    """
    Connects to Telegram, scrapes messages and media from specified channels,
    and stores them in a partitioned data lake structure.
    """
    client = TelegramClient(os.path.join(SESSION_DIR, SESSION_NAME), API_ID, API_HASH)

    logger.info("Connecting to Telegram...")
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.send_code_request(PHONE_NUMBER)
            try:
                await client.sign_in(PHONE_NUMBER, input('Enter the code from Telegram: '))
            except errors.SessionPasswordNeededError:
                await client.sign_in(password=input('Two-step verification enabled. Enter your password: '))
        logger.info("Connected to Telegram successfully!")

    except Exception as e:
        logger.error(f"Error connecting to Telegram: {e}", exc_info=True)
        return

    for channel_username in channels:
        today_str = datetime.datetime.now().strftime('%Y-%m-%d')
        channel_message_path = os.path.join(RAW_DATA_LAKE_MESSAGES_DIR, today_str)
        channel_image_path = os.path.join(RAW_DATA_LAKE_IMAGES_DIR, today_str)

        os.makedirs(channel_message_path, exist_ok=True)
        os.makedirs(channel_image_path, exist_ok=True)

        logger.info(f"\nStarting scraping for channel: {channel_username}")
        try:
            entity = await client.get_entity(channel_username)
            offset_id = 0
            limit = 100
            total_messages_scraped = 0

            while True:
                history = await client(GetHistoryRequest(
                    peer=entity,
                    offset_id=offset_id,
                    offset_date=None,
                    add_offset=0,
                    limit=limit,
                    max_id=0,
                    min_id=0,
                    hash=0
                ))
                messages = history.messages

                if not messages:
                    break

                for message in messages:
                    message_raw_data = message.to_dict()
                    message_raw_data['channel_username'] = channel_username
                    message_raw_data['channel_title'] = entity.title

                    message_file_dir = os.path.join(channel_message_path, entity.title.replace(' ', '_'))
                    os.makedirs(message_file_dir, exist_ok=True)
                    message_file_path = os.path.join(message_file_dir, f"{message.id}.json")

                    try:
                        with open(message_file_path, 'w', encoding='utf-8') as f:
                            json.dump(message_raw_data, f, ensure_ascii=False, indent=4, cls=CustomEncoder)
                    except Exception as json_e:
                        logger.error(f"Error saving message {message.id} to JSON: {json_e}", exc_info=True)
                        continue

                    if channel_username in IMAGE_CHANNELS and message.media:
                        file_name = None
                        file_extension = None
                        if isinstance(message.media, MessageMediaPhoto):
                            file_name = f"{entity.title.replace(' ', '_')}_{message.id}{file_extension}"
                        elif isinstance(message.media, MessageMediaDocument) and message.media.document:
                            if message.media.document.attributes:
                                for attr in message.media.document.attributes:
                                    if hasattr(attr, 'file_name'):
                                        file_name = attr.file_name
                                        file_extension = os.path.splitext(file_name)[1]
                                        break
                            if not file_name:
                                file_name = f"{entity.title.replace(' ', '_')}_{message.id}_doc"
                                if message.media.document.mime_type and '/' in message.media.document.mime_type:
                                    file_extension = '.' + message.media.document.mime_type.split('/')[-1]
                                else:
                                    file_extension = '.bin'

                            if not file_extension:
                                file_extension = '.dat'


                        if file_name:
                            image_file_dir = os.path.join(channel_image_path, entity.title.replace(' ', '_'))
                            os.makedirs(image_file_dir, exist_ok=True)
                            image_file_path = os.path.join(image_file_dir, file_name)

                            try:
                                await client.download_media(message, file=image_file_path)
                                logger.info(f"Downloaded media for message {message.id} to {image_file_path}")
                            except Exception as dl_e:
                                logger.warning(f"Error downloading media for message {message.id} in channel {channel_username}: {dl_e}", exc_info=True)
                        else:
                            logger.info(f"Message {message.id} has media but no identifiable file name/type for download.")


                offset_id = messages[-1].id
                total_messages_scraped += len(messages)
                logger.info(f"  Fetched {len(messages)} messages. Total for {entity.title}: {total_messages_scraped}. Last message ID: {offset_id}")
                await asyncio.sleep(1)

            logger.info(f"Finished scraping {total_messages_scraped} messages from {entity.title}")

        except errors.FloodWaitError as fwe:
            logger.warning(f"Flood wait error for channel {channel_username}. Waiting for {fwe.seconds} seconds.", exc_info=True)
            await asyncio.sleep(fwe.seconds + 5)
        except Exception as e:
            logger.error(f"Error scraping channel {channel_username}: {e}", exc_info=True)

    await client.disconnect()
    logger.info("\nDisconnected from Telegram.")
    logger.info("Scraping process complete.")


if __name__ == '__main__':
    if not API_ID or not API_HASH or not PHONE_NUMBER:
        logger.error("API_ID, API_HASH, or PHONE_NUMBER not set in environment variables. Please check your .env file.")
        exit(1)

    asyncio.run(connect_and_scrape())