import logging
import os
import requests
from datetime import time
import re
import ndjson

USERNAME = os.getenv(key="USERNAME",default="admin")
PASSWORD = os.getenv(key="PASSWORD",default="omnitestchannel")
EXECUTION_TIMESTAMP = os.getenv(key="EXECUTION_TIMESTAMP", default="2025-06-02 00:00:00") 

def login():

    #  setup API endpoint and body

    logging.info("Login for get token")

    url = f"https://omnichannel.qiscus.com/api/v1/auth"
    payload = {
        "username" : USERNAME,
        "password" : PASSWORD
    }
    headers = {
        "Content-Type": "application/json"
    }

    # to secure error response get data using try catch
        
    # try:
        # get data by request post/get
        # data = requests.post(url=url, json=payload, headers=headers)

        # checking status code to make sure get data is good
        # if data.status_code == 200:
        
    # except:
    #     logging.info(f"there wrong open status code : code status")

    return {"token" : "abcdefghijkl"}

def get_room(token:str):

    logging.info("get room by user id")

    url = f"https://api.qiscus.com/api/v2.1/rest/get_or_create_channel"

    params = {
        "emails" : ["ads@sevenretails.id","alfisyah@gmail.com"]
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization" : token
    }

    # to secure error response get data using try catch
        
    # try:
        # get data by request post/get
        # data = requests.get(url=url, headers=headers)

        # checking status code to make sure get data is good
        # if data.status_code == 200:
        
    # except:
    #     logging.info(f"there wrong open status code : code status")

    return {
        "results": {
            "rooms": [
                {"room_id": "1", "room_name": "ads", "channel": "facebook_ads", "customer_phone": "+628111223344"},
                {"room_id": "2", "room_name": "campaign blast", "channel": "instagram_ads", "customer_phone": "+628999887766"}
            ]
        },
        "status": 200
    }

def get_message_from_room(token:str, room_id: str):

    logging.info(f"get message from room id {room_id}")

    url = "https://api.qiscus.com/api/v2.1/rest/get_message_from_room"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization" : token
    }

    # mockup data message rooms
    if room_id == "1":
        return [
            {
                "room_id": "1",
                "channel": "facebook_ads",
                "customer_phone": "+628111223344",
                "sender_type": "customer",
                "sender_id": "alfi",
                "message_text": "I'm Interested with spark",
                "message_time": "2025-06-09T08:00:00Z"
            },
            {
                "room_id": "1",
                "channel": "facebook_ads",
                "customer_phone": "+628111223344",
                "sender_type": "agent",
                "sender_id": "agent_007",
                "message_text": "Would you like to book a class?",
                "message_time": "2025-06-09T08:02:00Z"
            },
            {
                "room_id": "1",
                "channel": "facebook_ads",
                "customer_phone": "+628111223344",
                "sender_type": "customer",
                "sender_id": "alfi",
                "message_text": "Booking on June 12",
                "message_time": "2025-06-09T08:03:00Z"
            },
            {
                "room_id": "1",
                "channel": "facebook_ads",
                "customer_phone": "+628111223344",
                "sender_type": "system",
                "sender_id": "system",
                "message_text": "Transaction confirmed. Amount: 750000 IDR.",
                "message_time": "2025-06-09T08:10:00Z"
            }
        ]
    elif room_id == "2":
        return [
            {
                "room_id": "2",
                "channel": "instagram_ads",
                "customer_phone": "+628999887766",
                "sender_type": "customer",
                "sender_id": "cust_002",
                "message_text": "Hi",
                "message_time": "2025-06-09T09:00:00Z"
            },
            {
                "room_id": "2",
                "channel": "instagram_ads",
                "customer_phone": "+628999887766",
                "sender_type": "system",
                "sender_id": "system",
                "message_text": "Hello Dears, please choose this bellow button",
                "message_time": "2025-06-09T09:15:00Z"
            },
            {
                "room_id": "2",
                "channel": "instagram_ads",
                "customer_phone": "+628999887766",
                "sender_type": "customer",
                "sender_id": "cust_002",
                "message_text": "Booking for June 20",
                "message_time": "2025-06-09T09:05:00Z"
            },
            {
                "room_id": "2",
                "channel": "instagram_ads",
                "customer_phone": "+628999887766",
                "sender_type": "system",
                "sender_id": "system",
                "message_text": "Transaction confirmed. Amount: 1000000 IDR.",
                "message_time": "2025-06-09T09:15:00Z"
            }
        ]
    else:
        return []


def extract():
    logging.info("starting get data")

    # mockup leads message
    leads_message = ["Hi","Hello","I'm Interested"]

    tokenizer = login()

    logging.info(tokenizer)

    id_room = get_room(tokenizer)["results"]["rooms"]

    # ETL process

    funnel_record = []

    for room in id_room:
        room_id = room["room_id"]
        channel = room["channel"]
        phone_number = room["customer_phone"]

        message = get_message_from_room(tokenizer,room_id)
        
        # sorting message to gets leads date, transac date, etc

        sorted_message = sorted(message, key=lambda x: x["message_time"])

        lead_date = None
        booking_date = None
        transaction_date = None
        transaction_value = None

        for msg in sorted_message:
            text = msg["message_text"]


            # Leads Date
            if not lead_date and any(keyword.lower() in text.lower() for keyword in leads_message):
                lead_date = msg["message_time"]

            # Booking Date
            if not booking_date and re.search(r'\bbooking\b|\bbook\b', text, re.IGNORECASE):
                booking_date = msg["message_time"]

            # Transaction Date and Value
            if not transaction_date and 'Transaction confirmed' in text:
                transaction_date = msg["message_time"]
                match = re.search(r'Amount:\s*(\d+)', text)
                if match:
                    transaction_value = int(match.group(1))

        # Append record
        record = {
            "room_id": room_id,
            "leads_date": lead_date,
            "channel": channel,
            "phone_number": phone_number,
            "booking_date": booking_date,
            "transaction_date": transaction_date,
            "transaction_value": transaction_value,
            "execution_timestamp": EXECUTION_TIMESTAMP
        }

        funnel_record.append(record)

    # final data
    # data = ndjson.dumps(funnel_record)

    # safe data into google cloud storage
    # client = storage.Client()
    # bucket = client.bucket(bucket_name)
    # blob = bucket.blob(destination_prefix)

    # blob.upload_from_string(data)

    # dummy safe into local

    logging.info("safe final data into local")
    output_file = f"/app/data/funnel_data_{EXECUTION_TIMESTAMP}.ndjson"
    with open(output_file, "w") as f:
        for record in funnel_record:
            f.write(ndjson.dumps([record]))  # Wrap in list to make correct line
            f.write("\n")
    
    logging.info("job done")
