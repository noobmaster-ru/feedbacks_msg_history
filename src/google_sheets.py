import gspread
import logging
from google.oauth2.service_account import Credentials
from typing import Optional, Tuple, List, Dict, Any
import src.config as config
from datetime import datetime

logger = logging.getLogger(__name__)

class GoogleSheets:
    def __init__(self, service_account_json_path):
        self.SHEETS_SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        self.SPREADSHEET_ID = "1lSdh2Q5j-fQ0YGkfKGeYlYAGDhoL05QKQnbXWP2YkIw"
        self.creds = Credentials.from_service_account_file(service_account_json_path, scopes=self.SHEETS_SCOPES)
        self.client = gspread.authorize(self.creds)
        self.HEADER = [
            "ChatID",
            "ClientName",
            "ClientRespondedToSellerMessage",
            "SellerRespondedToClientFirstMessage",
            "SellerMessageTimestamp",
        ]

    def update_time(self):
        sh = self.client.open_by_key(self.SPREADSHEET_ID)
        try:
            sheetname = sh.worksheet(config.INFO_SHEETNAME)
        except gspread.exceptions.WorksheetNotFound:
            sheetname = sh.add_worksheet(title=config.INFO_SHEETNAME, rows=1000, cols=26)
        daytime = f"{datetime.now().strftime("%H:%M:%S")}"
        sheetname.update("A1", [["Время обновления", daytime]])
        logger.info("update time in gs")
    
    def write_into_sheetname(self, sheetname: str, data_rows: Dict[str,str]):
        sh = self.client.open_by_key(self.SPREADSHEET_ID)
        try:
            sheetname = sh.worksheet(sheetname)
        except gspread.exceptions.WorksheetNotFound:
            sheetname = sh.add_worksheet(title=sheetname, rows=1000, cols=26)     
        first_row = sheetname.row_values(1)
        if first_row != self.HEADER:
            sheetname.update("A1", [self.HEADER])          
        sheetname.append_rows(data_rows[1:], value_input_option="RAW")
        logger.info("write data into gs")
