import os
from confluent_kafka import Producer
import time
import pandas as pd 
import json

if __name__ == "__main__":
    # configuration
    rate = 5

    conf = {
        'bootstrap.servers': 'localhost:9092' 
    }
    producerTopic = 'ec_web_logs_stream'
    producer = Producer(conf) 


    # open CSV file containing users' messages
    # Get the absolute path of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to the CSV file
    csvPath = os.path.join(script_dir, '../HadoopData/structured_streaming_101/raw/logs_201905_new.csv')

    with open(csvPath, encoding = 'utf-8') as new_logs:
        # Config
        batch_message_sent_count = 0
        error_count = 0    


        for line in new_logs:
            # remove the new line character
            line = line[:-1]
            print(line)
            try:
                producer.produce(producerTopic, json.dumps(line).encode('utf-8'))
                print("Sent......")
            except:
                error_count += 1
            batch_message_sent_count += 1

            if rate == batch_message_sent_count:
                batch_message_sent_count = 0
                time.sleep(5)
