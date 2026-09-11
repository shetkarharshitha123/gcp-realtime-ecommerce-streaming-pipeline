import json import time import random import requests from google.cloud import pubsub_v1 
 
# PROJECT & TOPIC CONFIGURATION 
PROJECT_ID = "snappy-mapper-498509-e0" 
TOPIC_ID = "ecom-orders-topic" 
 publisher = pubsub_v1.PublisherClient() topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID) # Public E-Commerce API (Random Carts/Orders) 
API_BASE_URL = "https://fakestoreapi.com/carts" 
 def fetch_and_publish():     try: 
        # Fetch a random cart order (ID 1 through 7)         cart_id = random.randint(1, 7) 
        response = requests.get(f"{API_BASE_URL}/{cart_id}").json() 
         
        # Add an ingestion timestamp 
        response["ingested_at"] = time.strftime("%Y-%m%dT%H:%M:%SZ", time.gmtime()) 
                 payload = json.dumps(response).encode("utf-8")         future = publisher.publish(topic_path, payload) 
        print(f"Published Order Cart ID #{cart_id} to Pub/Sub. Message ID: {future.result()}")     except Exception as e: 
        print(f"Error publishing order: {e}") 
 if __name__ == "__main__": 
    print("Starting E-Commerce Order Streamer to Pub/Sub... Press Ctrl+C to stop.")     while True: 
        fetch_and_publish()         time.sleep(5)  # Fetch new order every 5 seconds 
