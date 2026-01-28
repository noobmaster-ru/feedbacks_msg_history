import json
import logging
import sys

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
    

    parser.start_requesting()
    spreadsheet.write_into_sheetname("Messages", parser.text_messages)
    
    # очистили старые значения, чтобы не писать их снова
    parser.all_events.clear()
    parser.text_messages.clear()
    logger.info(f"continue parsing, sleep for {config.UPDATE_TIME}")
    while True:
        time.sleep(config.UPDATE_TIME)
        parser.parse_next_messages()
        spreadsheet.write_into_sheetname("Messages", parser.text_messages)
    
    # with open("text_messages.json", "w", encoding="utf-8") as f:
    #     json.dump(parser.text_messages, f, ensure_ascii=False, indent=2)

    # with open("all_events.json", "w", encoding="utf-8") as f:
    #     json.dump(parser.all_events, f, ensure_ascii=False, indent=2)
        
    # with open("chat_ids_nm_ids.json", "w", encoding="utf-8") as f:
    #     json.dump(parser.chat_ids_nm_ids, f, ensure_ascii=False, indent=2)
