import gspread
from google.oauth2.service_account import Credentials
from typing import Optional, Tuple, List, Dict, Any
import src.config as config

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
            "add_time_uts",
            "date",
            "daytime",
            "client_name",
            "nm_id",
            "text",
        ]

    
    def write_into_sheetname(self, sheetname: str, data_json: Dict[str,str]):
        sh = self.client.open_by_key(self.SPREADSHEET_ID)
        sheetname = sh.worksheet(sheetname)
        first_row = sheetname.row_values(1)
        if first_row != self.HEADER:
            sheetname.update("A1", [self.HEADER])
                
        list_of_lists_text_messages = [
            [i["add_time_uts"],i["date"], i["daytime"], i["client_name"], i["nm_id"], i["text"],] 
            for i in data_json
        ]
        # prepared = [list(item.values()) for item in data_json]
        sheetname.append_rows(list_of_lists_text_messages, value_input_option="RAW")