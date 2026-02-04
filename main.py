import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import time
from src.wb_api import WbAPI
from src.google_sheets import GoogleSheets
from dotenv import load_dotenv
import src.config as config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


if __name__ == "__main__":
    load_dotenv()
    HEADERS = {
        "Authorization": os.getenv('WB_TOKEN')
    }
    SERVICE_ACCOUNT_JSON = "sunny-might-477012-c4-04c66c69a92f.json"
    
    parser = WbAPI(headers=HEADERS)
    spreadsheet = GoogleSheets(service_account_json_path=SERVICE_ACCOUNT_JSON)

    parser.get_chats_api_and_make_chat_id_nm_id_dict()

    parser.get_events_api()
    parser.grouping_chats_by_chat_id()
    parser.processing_dialogs()
    parser.filtering_chat_ids_by_target_nm_ids(target_nm_ids=config.target_nmids)
    parser.filtering_dialogues_by_seller_message()
    
    daytime_csv_filename = f"{datetime.now(ZoneInfo("Europe/Moscow")).strftime("%H:%M:%S")}.csv"
    data_rows = parser.process_chat_events_and_generate_csv(filename=daytime_csv_filename)
    
    spreadsheet.write_into_sheetname(config.CHATS_SHEETNAME, data_rows)
    spreadsheet.update_time()
    
    while True:
        logger.info(f'sleep {config.UPDATE_TIME}')
        time.sleep(config.UPDATE_TIME)
        parser.all_events.clear()
        parser.grouped_events_by_chat_id.clear()
        parser.processed_dialogues.clear()
        parser.relevant_chat_ids.clear()
        parser.filtered_dialogues_by_seller_message.clear()
        
        parser.get_events_api()
        parser.grouping_chats_by_chat_id()
        parser.processing_dialogs()
        parser.filtering_chat_ids_by_target_nm_ids(target_nm_ids=config.target_nmids)
        parser.filtering_dialogues_by_seller_message()
    
        # for nm_id in config.target_nmids:
        daytime_csv_filename = f"{datetime.now().strftime("%H:%M:%S")}.csv"
        data_rows = parser.process_chat_events_and_generate_csv(filename=daytime_csv_filename)
        
        spreadsheet.write_into_sheetname(config.CHATS_SHEETNAME, data_rows)
        spreadsheet.update_time()
