import requests
import json
import csv
from datetime import date
import psycopg2
import os


""" this file is just a PoC containing functions and basic definitions for ETL"""
def extract_data():
    today=date.today()
    ACCESS_KEY= 'f98135edd6678737ff688fe02bbcc9e3'
    LIMIT = 100
    FLIGHT_STATUS="active"
    URL = 'http://api.aviationstack.com/v1/flights'
    QUERY_PARAMS= {'access_key':ACCESS_KEY,'limit':LIMIT,'flight_status':FLIGHT_STATUS}
    RAW_FILE_DIR = './data/raw'
    RAW_FILE = os.path.join(RAW_FILE_DIR,'raw_flight_data_'+str(today)+'.json')

    response= requests.get(URL,params=QUERY_PARAMS)
    if not os.path.isdir(RAW_FILE_DIR):
        os.makedirs(RAW_FILE_DIR)
        print("created folder : ", RAW_FILE_DIR)

    with open(RAW_FILE,'w') as f:
        json.dump(response.json(),f)
    
    
    return print(response)


def process_data():

    today=date.today()
    RAW_FILE_DIR='./data/raw/raw_flight_data_'+str(today)+'.json'
    STAGGED_FILE_DIR = './data/stagged'
    STAGGED_FILE = os.path.join(STAGGED_FILE_DIR,'stagged_flight_data'+str(today)+'.csv')
    if not os.path.isdir(STAGGED_FILE_DIR):
        os.makedirs(STAGGED_FILE_DIR)
        print("created folder : ", STAGGED_FILE_DIR)

    f=open(RAW_FILE_DIR,'r')

    json_data : dict = json.load(f)
    data=json_data['data']
    f.close()
    extracted_data: list = []
    header=['flight_date',
            'flight_status',
            'departure_airport',
            'departure_timezone',
            'arrival_airport',
            'arrival_timezone',
            'arrival_terminal', 
            'airline_name',
            'flight_number',
            'timestamp'
            ]
    
    for i in range (0,len(data)):
        row_data = [  
                    str(data[i]['flight_date']),
                    str(data[i]['flight_status']),
                    str(data[i]['departure']['airport']),
                    str(data[i]['departure']['timezone']).replace("/","-"),
                    str(data[i]['arrival']['airport']),
                    str(data[i]['arrival']['timezone']).replace("/","-"),
                    str(data[i]['arrival']['terminal']),
                    str(data[i]['airline']['name']),
                    str(data[i]['flight']['number']),
                    today
                          
                    ]

        extracted_data.append(row_data)

    

    with open(STAGGED_FILE,'w',newline='') as file:
        write=csv.writer(file)
        write.writerow(header)
        write.writerows(extracted_data)
    
    return (print('data extracted and processed_'+str(today)))

def write_db():
    today=date.today()
    STAGGED_FILE_DIR='./data/stagged/stagged_flight_data'+str(today)+'.csv'

    get_postgres_conn=PostgresHook(postgres_conn_id='fligooTest').get_conn()
    curr=get_postgres_conn.cursor()
   
    

    with open(STAGGED_FILE_DIR,'r') as f:
     curr.copy_expert("COPY processed.testdata FROM STDIN WITH CSV HEADER", f)
     get_postgres_conn.commit()

def postgres_write():
    conn = psycopg2.connect(    host="localhost",
                                database="testfligoo",
                                user="postgres",
                                password="admin")
    curr=conn.cursor()

    today=date.today()
    STAGGED_FILE_DIR='./data/stagged/stagged_flight_data'+str(today)+'.csv'
   
    

    with open(STAGGED_FILE_DIR,'r') as f:
        curr.copy_expert("COPY processed.testdata FROM STDIN WITH CSV HEADER", f)
        conn.commit()
    print('data stored into dw, have a nice day! :D')




def main():
    extract_data()
    process_data()
    postgres_write()
    

main()
