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
        self.all_events = []
        self.grouped_events_by_chat_id = {}
        
        self.processed_dialogues = {}
        self.relevant_chat_ids = set()
        self.filtered_dialogues_by_seller_message = {}
        
        self.next_timestamp = config.next_timestamp
        
    def get_data_from_wb_api(self):
        # 1. API Fetching Logic
        logger.info("======================")
        logger.info("Starting API fetching...")
        while True:
            time.sleep(config.SLEEP_TIME)
            url = f'https://buyer-chat-api.wildberries.ru/api/v1/seller/events?next={self.next_timestamp}'

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

    
    def grouping_chats(self):
        # 2. Group Events by ChatID
        logger.info("======================")
        logger.info("Grouping chats")
        for event in self.all_events:
            chat_id = event.get('chatID')
            if chat_id:
                if chat_id not in self.grouped_events_by_chat_id:
                    self.grouped_events_by_chat_id[chat_id] = []
                self.grouped_events_by_chat_id[chat_id].append(event)
        logger.info(f"Grouped {len(self.all_events)} events into {len(self.grouped_events_by_chat_id)} unique chat IDs.")

    

    
    def process_chat_events_and_generate_csv(self, seller_message_to_filter, target_nm_id, filename):
        # 3. Process Dialogues
        logger.info("======================")
        logger.info("Process Dialogues")
        processed_dialogues = {}
        for chat_id, events_list in self.grouped_events_by_chat_id.items():
            dialogue_messages = []
            for event in events_list:
                if event.get('eventType') == 'message' and 'message' in event:
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
            processed_dialogues[chat_id] = dialogue_messages
        logger.info(f"Processed {len(processed_dialogues)} chats into dialogues.")



        # 4. Filter by target_nmids
        logger.info("Filter by target_nmids")
        relevant_chat_ids = set()
        for chat_id, events_list in self.grouped_events_by_chat_id.items():
            for event in events_list:
                if event.get('eventType') == 'message' and 'message' in event:
                    message_content = event['message']
                    if 'attachments' in message_content and 'goodCard' in message_content['attachments']:
                        good_card = message_content['attachments']['goodCard']
                        if 'nmID' in good_card:
                            nmID = good_card['nmID']
                            if nmID in target_nm_id:
                                relevant_chat_ids.add(chat_id)
                                break
        logger.info(f"Found {len(relevant_chat_ids)} unique chat IDs associated with target nmIDs.")


        # 5. Filter by Seller Message
        logger.info("Filter by Seller Message")
        filtered_dialogues_by_seller_message = {}
        for chat_id in relevant_chat_ids:
            if chat_id in processed_dialogues:
                messages = processed_dialogues[chat_id]
                if messages:
                    first_message = messages[0]
                    if first_message.get('sender') == 'seller' and first_message.get('text') == seller_message_to_filter:
                        filtered_dialogues_by_seller_message[chat_id] = messages
        logger.info(f"Filtered dialogues: {len(filtered_dialogues_by_seller_message)} dialogues start with the specified seller message and are associated with target nmIDs.")

        # 6. Prepare CSV Data
        logger.info("Prepare CSV Data")
        csv_data = [['ChatID', 'ClientName', 'ClientRespondedToSellerMessage', 'SellerRespondedToClientFirstMessage', 'SellerMessageTimestamp']]
        data_rows = []

        for chat_id, messages in filtered_dialogues_by_seller_message.items():
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

        # 7. Save to CSV file
        csv_file_path = filename
        with open(csv_file_path, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=';')
            csv_writer.writerows(csv_data)

        logger.info(f"Data successfully saved to {csv_file_path}. Total rows: {len(csv_data) - 1}.")
        logger.info("======================")
        return csv_data