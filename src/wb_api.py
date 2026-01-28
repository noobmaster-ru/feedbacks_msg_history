import requests
import logging 
from typing import Optional, Tuple, List, Dict, Any
import json
import time
import src.config as config

logger = logging.getLogger(__name__)

class WbAPI:
    def __init__(self, headers: Dict[str, str]):
        self.headers = headers
        self.all_events = []
        self.text_messages = []
        self.chat_ids_nm_ids = {}

        self.next_timestep = None
        
    def cycle_parse(self, events: Dict[str,str]):
        for ev in events:
            if ev.get("sender") != "client":
                continue
            if not ev.get("message"):
                continue
            
            add_time_uts = ev.get("addTime")
            date = add_time_uts.split("T")[0]
            daytime = add_time_uts.split("T")[1][:-1]
            
            msg = ev.get("message") or {}
            chat_id = ev.get("chatID")
            is_new_chat = ev.get("isNewChat") or {}
            if is_new_chat:
                nm_id = None
                try:
                    nm_id = msg["attachments"]["goodCard"]["nmID"]
                    if not nm_id in list(self.chat_ids_nm_ids.values()):
                        self.chat_ids_nm_ids.update({chat_id: nm_id})
                except (KeyError, TypeError):
                    pass
                                
            if not msg:
                continue
            text = (msg.get("text") or "").strip()

            if not text:
                continue
            self.text_messages.append({
                "add_time_uts": add_time_uts,
                "chat_id": chat_id,
                "date": date,
                "daytime": daytime,
                "client_name": ev.get("clientName"),
                "text": text,   
            })
        
    def start_requesting(self):
        url = f"https://buyer-chat-api.wildberries.ru/api/v1/seller/events"
        resp = requests.get(url, headers=self.headers)
        resp.raise_for_status()

        logger.info(f"Response status: {resp.status_code}")
        data = resp.json()
        
        result = (data or {}).get("result") or {}
        events = result.get("events") or []
        self.all_events.extend(events)
        self.next_timestep = result.get("next")
        
        self.cycle_parse(events=events)
        time.sleep(config.SLEEP_TIME)
        
        self.parse_next_messages()

    def parse_next_messages(self):
        while True:
            url = f"https://buyer-chat-api.wildberries.ru/api/v1/seller/events?next={self.next_timestep}"
            resp = requests.get(url, headers=self.headers)
            resp.raise_for_status()
            logger.info(f"Response status: {resp.status_code}")
            data = resp.json()
            
            result = (data or {}).get("result") or {}
            events = result.get("events") or []
            total_events = result.get("totalEvents")
            if total_events == 0 or not events:
                self.next_timestep = result.get("next")
                break
            self.all_events.extend(events)

            self.cycle_parse(events=events)

            self.next_timestep = result.get("next")
            time.sleep(config.SLEEP_TIME)
        self.text_messages.sort(key=lambda item: item['add_time_uts'])  # старые -> новые
        # Обновляем каждый словарь в списке
        for msg in self.text_messages:
            chat_id = msg.get("chat_id")
            # Добавляем nm_id. Если chat_id нет в справочнике, запишем None
            msg["nm_id"] = self.chat_ids_nm_ids.get(chat_id)
