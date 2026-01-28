import json
import logging
import sys
from datetime import datetime
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

    parser.get_data_from_wb_api()
    parser.grouping_chats()
    daytime_csv = f"{datetime.now().strftime("%H:%M:%S")}.csv"
    # for nm_id in config.target_nmids:
    data_rows = parser.process_chat_events_and_generate_csv(
        seller_message_to_filter=config.seller_message_to_filter,
        target_nm_id=config.target_nmids,
        filename=daytime_csv
    )
    spreadsheet.write_into_sheetname("Messages",data_rows)
    spreadsheet.update_time()
    
    while True:
        logger.info(f'sleep {config.UPDATE_TIME}')
        time.sleep(config.UPDATE_TIME)
        parser.all_events.clear()
        parser.grouped_events_by_chat_id.clear()
        parser.processed_dialogues.clear()
        parser.relevant_chat_ids.clear()
        parser.filtered_dialogues_by_seller_message.clear()
        
        parser.get_data_from_wb_api()
        parser.grouping_chats()
        
        # for nm_id in config.target_nmids:
        data_rows = parser.process_chat_events_and_generate_csv(
            seller_message_to_filter=config.seller_message_to_filter,
            target_nm_id=config.target_nmids,
            filename=daytime_csv
        )
        spreadsheet.write_into_sheetname("Messages",data_rows)
        spreadsheet.update_time()