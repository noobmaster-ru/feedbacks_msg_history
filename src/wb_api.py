import requests
import logging 
from typing import Optional, Tuple, List, Dict, Any
import json
import time
import src.config as config
import csv

logger = logging.getLogger(__name__)

class WbAPI:
    def __init__(self, headers: Dict[str, str]):
        self.headers = headers
        self.next_timestamp = 0
        
        self.all_events = [] # make from get_events_api
        self.grouped_events_by_chat_id = {} # chat_id: event; make from grouping_chats_by_chat_id
        self.processed_dialogues = {} # chat_id: {sender, name, text, time}; make from processing_dialogs
        self.relevant_chat_ids = set() # все чаты, по config.target_nmids; make from filtering_chat_ids_by_target_nm_ids
        self.filtered_dialogues_by_seller_message = {} # make from filtering_dialogues_by_seller_message
        
        # make in get_chats_api_and_make_chat_id_nm_id_dict
        self.chat_id_nm_id_dict = {} # make from get_chats_api_and_make_chat_id_nm_id_dict
      
    # make  self.chat_id_nm_id_dict - need only one  time 
    def get_chats_api_and_make_chat_id_nm_id_dict(self):
        # 1. API Fetching Logic
        logger.info("======================")
        logger.info("Starting API fetching...")

        url = config.CHATS_URL
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
            data = response.json()
            result = data['result']
            for chat in result:
                chat_id = chat['chatID']
                # logger.info(f"{chat_id}")
                if chat.get('goodCard'):
                    nm_id = chat['goodCard']['nmID']
                    # logger.info(f"{nm_id}")
                    if chat_id not in self.chat_id_nm_id_dict:
                        self.chat_id_nm_id_dict[chat_id] = nm_id
            logger.info(f"find {len(self.chat_id_nm_id_dict)} chats-nmID links")
        except requests.exceptions.RequestException as e:
            logger.info(f"Error making API call: {e}")
       
    # make self.all_events  
    def get_events_api(self):
        # 1. API Fetching Logic
        logger.info("======================")
        logger.info("Starting API fetching...")
        url = config.EVENTS_URL
        try:
            response.raise_for_status()
            response = requests.get(url, headers=self.headers)
            data = response.json()
            
            if data['result']['totalEvents'] == 0:
                logger.info('No more events to fetch. Stopping.')
            self.all_events.extend(data['result']['events'])
            self.next_timestamp = data['result']['next']
            logger.info(f"Fetched {len(data['result']['events'])} events. Total events collected: {len(self.all_events)}. Timestamp: {self.next_timestamp}")
        except:
            pass
        
        while True:
            time.sleep(config.SLEEP_TIME)
            url = config.EVENTS_URL
            url += f'?next={self.next_timestamp}'

            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
                data = response.json()

                if data['result']['totalEvents'] == 0:
                    logger.info('No more events to fetch. Stopping.')
                    break

                self.all_events.extend(data['result']['events'])
                self.next_timestamp = data['result']['next']
                logger.info(f"Fetched {len(data['result']['events'])} events. Total events collected: {len(self.all_events)}. Timestamp: {self.next_timestamp}")
            except requests.exceptions.RequestException as e:
                logger.info(f"Error making API call: {e}")
                break
        logger.info(f"Finished API fetching. Total events collected: {len(self.all_events)}.")
 
    # make self.grouped_events_by_chat_id
    def grouping_chats_by_chat_id(self):
        # 2. Group Events by ChatID
        logger.info("======================")
        logger.info("Grouping chats")
        for event in self.all_events:
            chat_id = event.get('chatID')
            if chat_id not in self.grouped_events_by_chat_id:
                self.grouped_events_by_chat_id[chat_id] = []
            self.grouped_events_by_chat_id[chat_id].append(event)
        logger.info(f"Grouped {len(self.all_events)} events into {len(self.grouped_events_by_chat_id)} unique chat IDs.")

    # make self.processed_dialogues
    def processing_dialogs(self):
        # 3. Process Dialogues
        logger.info("======================")
        logger.info(f"Process Dialogues from {len(self.grouped_events_by_chat_id.keys())} chats")
        for chat_id, events_list in self.grouped_events_by_chat_id.items():
            dialogue_messages = []
            for event in events_list:
                if 'message' in event:
                    sender = event.get('sender')
                    client_name = event.get('clientName') if sender == 'client' else None
                    message_text = event['message'].get('text', '')
                    add_time = event.get('addTime')

                    if not message_text and event['message'].get('attachments'):
                        message_text = "[Attachment]"

                    if message_text:
                        dialogue_messages.append({
                            'sender': sender,
                            'name': client_name if client_name else (event.get('sender').capitalize() if sender else 'Unknown'),
                            'text': message_text,
                            'time': add_time
                        })

            dialogue_messages.sort(key=lambda x: x['time'])
            self.processed_dialogues[chat_id] = dialogue_messages

        logger.info(f"Processed {len(self.processed_dialogues)} chats into dialogues.")

    # make self.relevant_chat_ids
    def filtering_chat_ids_by_target_nm_ids(self, target_nm_ids: list):
        # 4. Filter by target_nmids
        logger.info("======================")
        logger.info("Filtering nm_ids by target_nmids")
        logger.info(f"Relevants chat ids: {self.relevant_chat_ids}")
        for chat_id, nm_id in self.chat_id_nm_id_dict.items():
            # logger.info(f"chat_id = {chat_id}, nm_id = {nm_id} , {target_nm_ids}")
            if nm_id in target_nm_ids:
                self.relevant_chat_ids.add(chat_id)
        logger.info(f"Found {len(self.relevant_chat_ids)} unique chat IDs associated with target nmIDs.")
        logger.info(f"Relevants chat ids: {self.relevant_chat_ids}")

    # make self.filtered_dialogues_by_seller_message
    def filtering_dialogues_by_seller_message(self): #seller_message_to_filter: str):
        # 5. Filter by Seller Message
        logger.info("Filter by Seller Message")
        # filtered_dialogues_by_seller_message = {}
        for chat_id in self.relevant_chat_ids:
            if chat_id in self.processed_dialogues:
                messages = self.processed_dialogues[chat_id]
                if messages:
                    # first_message = messages[0]
                    # if first_message.get('sender') == 'seller': # and first_message.get('text') == seller_message_to_filter:
                    self.filtered_dialogues_by_seller_message[chat_id] = messages
        logger.info(f"Filtered dialogues: {len(self.filtered_dialogues_by_seller_message)} dialogues start with the specified seller message and are associated with target nmIDs.")

    # make csv_data and save .csv
    def process_chat_events_and_generate_csv(self,  filename):
        # 6. Prepare CSV Data
        logger.info("======================")
        logger.info("Prepare CSV Data")
        csv_data = [['ChatID', 'ClientName', 'ClientRespondedToSellerMessage', 'SellerRespondedToClientFirstMessage', 'SellerMessageTimestamp']]
        data_rows = []

        for chat_id, messages in self.filtered_dialogues_by_seller_message.items():
            if messages:
                first_seller_message = messages[0]
                seller_message_timestamp = first_seller_message.get('time', 'Unknown time')

                client_responded_to_seller_message = False
                client_name = ''
                seller_responded_to_client_first_message = False

                first_client_message_index = -1

                for i in range(1, len(messages)):
                    if messages[i].get('sender') == 'client':
                        client_responded_to_seller_message = True
                        client_name = messages[i].get('name', 'Unknown Client')
                        first_client_message_index = i
                        break

                if client_responded_to_seller_message and first_client_message_index != -1:
                    for i in range(first_client_message_index + 1, len(messages)):
                        if messages[i].get('sender') == 'seller':
                            seller_responded_to_client_first_message = True
                            break

                data_rows.append([chat_id, client_name, client_responded_to_seller_message, seller_responded_to_client_first_message, seller_message_timestamp])

        # Sort data_rows by SellerMessageTimestamp
        data_rows.sort(key=lambda x: x[4])

        csv_data.extend(data_rows)

        # # 7. Save to CSV file
        # csv_file_path = filename
        # with open(csv_file_path, 'w', newline='') as csvfile:
        #     csv_writer = csv.writer(csvfile, delimiter=';')
        #     csv_writer.writerows(csv_data)

        # logger.info(f"Data successfully saved to {csv_file_path}. Total rows: {len(csv_data) - 1}.")
        # logger.info("======================")
        return csv_data