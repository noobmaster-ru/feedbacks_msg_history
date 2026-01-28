import os
import json
import time
import csv
import logging
import requests
from typing import Optional, Tuple, List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials


# ---------- WB parsing ----------
def fetch_wb_events(initial_next_timestamp: int, headers: Dict[str, str], timeout_sec: int = 30):
    all_events = []
    next_timestamp = int(initial_next_timestamp)
    while True:

        url = f"https://buyer-chat-api.wildberries.ru/api/v1/seller/events?next={next_timestamp}"
        resp = requests.get(url, headers=headers, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
        
        result = (data or {}).get("result") or {}
        events = result.get("events") or []
        total = result.get("totalEvents", 0)
        if total == 0 or not events:
            next_timestamp = int(result.get("next", next_timestamp))
            break
        all_events.extend(events)
        next_timestamp = result.get("next")

    return all_events, next_timestamp


def events_to_client_rows(
    events: List[Dict[str, Any]]
):
    rows = []
    for ev in events:
        if ev.get("eventType") != "message":
            continue
        if ev.get("sender") != "client":
            continue

        add_ts = ev.get("addTimestamp")
        if add_ts is None:
            continue
        add_ts = int(add_ts)

    

        msg = ev.get("message") or {}
        text = (msg.get("text") or "").strip()


        if not text:
            continue


        rows.append([
            ev.get("addTime"),           # AddTime
            ev.get("clientName"),        # ClientName
            text,                        # Text
        ])

    rows.sort(key=lambda r: r[0])  # старые -> новые
    return rows


# ---------- Google Sheets ----------
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADER = [
    "AddTime",
    "ClientName",
    "Text",
]


def get_gs_client(service_account_json_path: str) -> gspread.Client:
    creds = Credentials.from_service_account_file(service_account_json_path, scopes=SHEETS_SCOPES)
    return gspread.authorize(creds)


def ensure_header(ws):
    # если лист пустой — пишем заголовок
    if ws.row_count == 0 or (ws.get("A1") == [[]]):
        ws.append_row(HEADER, value_input_option="RAW")
        return

    first_row = ws.row_values(1)
    if first_row != HEADER:
        # аккуратно: можно либо перезаписать 1 строку, либо оставить как есть
        ws.update("A1", [HEADER])




def append_rows_dedup(ws, rows: List[List[Any]]):
    ensure_header(ws)

    new_rows = [r for r in rows]
    if not new_rows:
        return 0

    # gspread ожидает строки без python-булей иногда ок, но лучше привести
    prepared = []
    for r in new_rows:
        rr = r.copy()
        # rr[8] = "TRUE" if rr[8] else "FALSE"   # HasAttachments
        prepared.append([str(x) if x is not None else "" for x in rr])

    ws.append_rows(prepared, value_input_option="RAW")
    return len(prepared)


# ---------- Cursor persistence ----------
def load_cursor(path: str, default_next: int) -> int:
    if not os.path.exists(path):
        return int(default_next)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return int(data.get("next_timestamp", default_next))


def save_cursor(path: str, next_timestamp: int):
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"next_timestamp": int(next_timestamp)}, f, ensure_ascii=False, indent=2)


# ---------- Orchestration ----------
def wb_to_google_sheet_once(
    headers: Dict[str, str],
    spreadsheet_id: str,
    worksheet_title: str,
    service_account_json_path: str,
    cursor_path: str,
    initial_next_timestamp: int,
    start_ts_ms: Optional[int] = None,
    end_ts_ms: Optional[int] = None,
) -> Tuple[int, int]:
    """
    Один прогон: WB -> rows -> append to sheet.
    Возвращает (next_timestamp, appended_rows_count)
    """
    next_ts = load_cursor(cursor_path, initial_next_timestamp)

    events, new_next_ts = fetch_wb_events(next_ts, headers=headers)
    rows = events_to_client_rows(events)

    gc = get_gs_client(service_account_json_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_title)

    appended = append_rows_dedup(ws, rows)

    save_cursor(cursor_path, new_next_ts)
    return new_next_ts, appended


if __name__ == "__main__":
    # ---- заполните свои значения ----
    HEADERS = {
        "Authorization": "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzg1MTQxMzUxLCJpZCI6IjAxOWJmNmRlLWJmMDktNzU1My05YmNiLWQ4ZTExODU1ZDI1NyIsImlpZCI6MzYyNzk1NzEsIm9pZCI6MTIwOTcwMywicyI6MTYxMjYsInNpZCI6ImMzZTRlZjZhLWMxODctNGM0OC1iZjhlLWU1ZDdlZjFlMWQzZiIsInQiOmZhbHNlLCJ1aWQiOjM2Mjc5NTcxfQ.SXj867c9dai8XCg8U5qSdUUPktl1kQOyecuTcB2rtXWIWrepIVPGjSVOHwLpLMqmy6NYmLOaQ0of6wdvv1Q5jw"
        # + остальные нужные заголовки, если вы их используете
    }

    SPREADSHEET_ID = "1lSdh2Q5j-fQ0YGkfKGeYlYAGDhoL05QKQnbXWP2YkIw"
    WORKSHEET_TITLE = "Messages"
    SERVICE_ACCOUNT_JSON = "sunny-might-477012-c4-04c66c69a92f.json"

    CURSOR_PATH = "wb_cursor.json"
    INITIAL_NEXT_TS = 1698040000000  # стартовый next

    next_ts = INITIAL_NEXT_TS
    while True:
        next_ts, appended = wb_to_google_sheet_once(
            headers=HEADERS,
            spreadsheet_id=SPREADSHEET_ID,
            worksheet_title=WORKSHEET_TITLE,
            service_account_json_path=SERVICE_ACCOUNT_JSON,
            cursor_path=CURSOR_PATH,
            initial_next_timestamp=next_ts,
        )
        logging.info(f"next_timestamp: {next_ts}, appended rows: {appended}")
        logging.info("sleep")
        time.sleep(60)
    # print("next_timestamp:", next_ts, "appended rows:", appended)