import os
from bs4 import BeautifulSoup as bs
import requests
from datetime import datetime, timedelta
import numpy as np
import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from datetime import datetime, timedelta

def download_data(start_time,end_time):
    s = requests.session()
    URL = 'http://deprem.itu.edu.tr/'
    DWNLD = 'settings/download_data'
    LOGIN_ROUTE = 'login'
    HEADERS = {'origin': URL, 'referer': URL + LOGIN_ROUTE}
    soup = bs(s.get(URL+LOGIN_ROUTE).text, 'html.parser')
    csrf_token = soup.body.find(id="csrf_token")['valuea']
    id_login = "kilicbu16@itu.edu.tr"
    password = "B@mnC8L^VdwGqa"

    login_payload = {
                    'email': id_login,
                            'password': password,
                                    'remember_me': 'y',
                                            'submit': 'Giriş',
                                                    'csrf_token': csrf_token
                                                            }
    login_request = s.post(URL + LOGIN_ROUTE, headers=HEADERS, data=login_payload)

    if login_request.status_code == 200:
        cookies = login_request.cookies
        soup = bs(s.get(URL+'settings/bilgiler').text)

        HEADERS_DL = {'origin': URL, 'referer': URL + DWNLD}

        login_payload = {
                        'selected_records' : '1',
                                #'include_earthquakes' : 'y',
                                        #'include_demo' : 'y',
                                                'submit' : 'İndir',
                                                        'csrf_token' : csrf_token
                                                                }
        
        respFile = s.post(URL + DWNLD, headers=HEADERS_DL, data=login_payload)
        # Get the directory path of the Python script
        # script_directory = os.path.dirname(os.path.abspath(__file__))
        # file_path = os.path.join(script_directory, 'response1.dat')

        script_directory = "/home/ubuntu/Bilge"
        file_path = os.path.join(script_directory, 'response1.dat')

        if os.path.exists(file_path):
                os.remove(file_path)

        with open(file_path, "wb") as f:
                f.write(respFile.content)

        start = start_time
        end = end_time

        with open(file_path, "rb") as file:

            file.seek(int((start-np.datetime64('2000-01-01'))/np.timedelta64(1, 'm'))*4)
            minutes = int((end-start)/np.timedelta64(1, 'm'))
            data = np.frombuffer(file.read(minutes*4), np.float32)
            #print(data[:100])
            times = np.arange(start, end, np.timedelta64(1, 'm'))


        # Convert 'times' and 'data' arrays into a JSON structure
        json_data = []
        try:
            for i in range(len(times)):
                #print( str(times[i] + np.timedelta64(-3, 'h')))
                if np.isnan(data[i]):
                    entry = {
                        'time': str(times[i] + np.timedelta64(-3, 'h')),
                        'data': float(0.0)
                        }
                   # print(f"Index {i} +time {times[i] + np.timedelta64(-3, 'h')} + data {data[i]}")
                else:
                    entry = {
                        'time': str(times[i] + np.timedelta64(-3, 'h')),
                        'data': round(float(data[i]), 2)
                        }
                json_data.append(entry)
                    #print(f"time {times[i] + np.timedelta64(-3, 'h')} + data {data[i]}")
                #entry = {
                  #  'time': str(times[i] + np.timedelta64(-3, 'h')),
                   # 'data': float(data[i])
                                           # }
                    
            return json_data
        
        except:
            return None
                
    else:
        return None
 
def split_into_bulks(max_bulk_size,json_data):               
    # Accumulate JSON objects into a bulk message
    bulk_message = []

    # Convert and send each JSON object
    for item in json_data:
        json_object = json.dumps(item)
        if not bulk_message:
            # If the bulk message is empty, add the current JSON object
            bulk_message.append(json_object)
        else:
            # Check if adding the current JSON object exceeds the maximum bulk size
            if len(', '.join(bulk_message) + ', ' + json_object) <= max_bulk_size:
                # Add the current JSON object to the bulk message
                bulk_message.append(json_object)
            else:
                # Send the bulk message as a single Kafka message
                bulk_data = '[{}]'.format(', '.join(bulk_message))
                produce_json_data('electricRaw', bulk_data.encode())
                print(f"Batch gone: {np.datetime64('now')}")
                # Reset the bulk message for the next iteration
                bulk_message = [json_object]

    # Send the remaining bulk message if it is not empty
    if bulk_message:
        bulk_data = '[{}]'.format(', '.join(bulk_message))
        produce_json_data('electricRaw', bulk_data.encode())
        print(f"Final batch gone: {np.datetime64('now')}")

def latest_message(max_bulk_size,json_data):               
    # Accumulate JSON objects into a message
    message = []
    for i in range(len(json_data)):
        if json_data[i]['data'] != float('0.0'):
            message.append(json_data[i])
        else:
            break
    bulk_data = str(message)
    if bulk_data != '[]':
        produce_json_data('electricRaw', bulk_data.encode())
    else:
        return

    
def start_time(json_data,minute):
    for i in range(len(json_data)):
        if json_data[i]['data'] == float('0.0'):
            a =  np.datetime64(np.datetime64(json_data[i]['time'])+np.timedelta64(3, 'h') + np.timedelta64(minute, 'm'), 'm')
            return a
def end_time(json_data,minute):
    a =  np.datetime64(np.datetime64(json_data[-1]['time']) +np.timedelta64(3, 'h')+ np.timedelta64(minute, 'm'), 'm')
    return a

def produce_json_data(topic, json_data):
    producer_config = {
        'bootstrap.servers': 'localhost:9092'
    }

    producer = Producer(producer_config)

    try:
        # Convert the JSON data to a string
        #json_str = json.dumps(json_data)
        # Produce the JSON data to the topic
        producer.produce(topic,json_data)
        # Wait for the message to be delivered
        producer.flush()
    except KeyboardInterrupt:
        pass
    
def produceElectricData():
    start_time_data = np.datetime64('2022-04-23') 
    end_time_data = np.datetime64(np.datetime64('now')+np.timedelta64(3, 'h') + np.timedelta64(-10, 'm') , 'm')#
    print(f"First download started: {np.datetime64('now') + np.timedelta64(3, 'h')}")
    electric_data_first_batches = download_data(start_time_data,end_time_data)
    print(f"First download ended: {np.datetime64('now') + np.timedelta64(3, 'h')}")
    if electric_data_first_batches:
        max_message_size = 900000 # 1 message consists 13 days of data
        split_into_bulks(max_message_size,electric_data_first_batches)
        print(f"First message gone: {np.datetime64('now') + np.timedelta64(3, 'h')}")
        print("Wait for 60 secs")
        time.sleep(10)
        start_time_data = np.datetime64(np.datetime64(electric_data_first_batches[-1]['time'])+np.timedelta64(3, 'h') + np.timedelta64(1, 'm'))
        end_time_data = np.datetime64(np.datetime64('now')+np.timedelta64(3, 'h') , 'm')#+ np.timedelta64(-2, 'm')
        print(f"Latest download started: {np.datetime64('now') + np.timedelta64(3, 'h')}")
        electric_data_latest = download_data(start_time_data,end_time_data)
        print(f"Message: {electric_data_latest}")
        print(f"Latest download ended: {np.datetime64('now') + np.timedelta64(3, 'h')}")
    while True:
        if electric_data_latest:
            max_message_size = 900000 # 1 message consists 13 days of data
            latest_message(max_message_size,electric_data_latest)
            print(f"Message gone: {np.datetime64('now') + np.timedelta64(3, 'h')}")
            print("Wait for 60 secs")
            time.sleep(60)
            start_time_data = start_time(electric_data_latest,0)
            end_time_data = np.datetime64(np.datetime64('now')+np.timedelta64(3, 'h') , 'm') #end_time(electric_data_latest,0)
            print(f"Latest download started: {np.datetime64('now') + np.timedelta64(3, 'h')}")
            electric_data_latest = download_data(start_time_data,end_time_data)
            print(f"Latest download ended: {np.datetime64('now') + np.timedelta64(3, 'h')}")
            print(f"Message: {electric_data_latest}")
produceElectricData()
