
import airflow
import os
import psycopg2
import requests
import json
import csv

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import timedelta
from datetime import datetime
from datetime import date




def extract_data():
    today=date.today()
    ACCESS_KEY= 'f98135edd6678737ff688fe02bbcc9e3'
    LIMIT = 100
    FLIGHT_STATUS="active"
    URL = 'http://api.aviationstack.com/v1/flights'
    QUERY_PARAMS= {'access_key':ACCESS_KEY,'limit':LIMIT,'flight_status':FLIGHT_STATUS}
    RAW_FILE_DIR = './airflow/data/raw/raw_flight_data_'+str(today)+'.json'

    response= requests.get(URL,params=QUERY_PARAMS)
    if not os.path.isdir(RAW_FILE_DIR):
    os.makedirs(RAW_FILE_DIR)
    print("created folder : ", RAW_FILE_DIR)

    with open(RAW_FILE_DIR,'w') as f:
        json.dump(response.json(),f)
    
    
    return print(response)


def process_data():

    today=date.today()
    RAW_FILE_DIR='airflow/data/raw/raw_flight_data_'+str(today)+'.json'
    STAGGED_FILE_DIR = 'airflow/data/stagged/stagged_flight_data'+str(today)+'.csv'
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

    

    with open(STAGGED_FILE_DIR,'w') as file:
        write=csv.writer(file)
        write.writerow(header)
        write.writerows(extracted_data)
    
    return (print('data extracted and processed_'+str(today)))

def write_db():
    today=date.today()
    STAGGED_FILE_DIR='airflow/data/stagged/stagged_flight_data'+str(today)+'.csv'

    get_postgres_conn=PostgresHook(postgres_conn_id='fligooTest').get_conn()
    curr=get_postgres_conn.cursor()
   
    

    with open(STAGGED_FILE_DIR,'r') as f:
     curr.copy_expert("COPY processed.testdata FROM STDIN WITH CSV HEADER", f)
     get_postgres_conn.commit()    


one_day_ago = datetime.combine(
    datetime.today() - timedelta(1),
    datetime.min.time()
)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': one_day_ago,
    'email': ['javiercvallejo1@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=1),
  
}


#tasks
dag = DAG('fligoo_dw_write',
          default_args=default_args,
          schedule_interval='0 8 * * *',
          catchup=False
          )

begin = DummyOperator(
    task_id='Begin',
    dag=dag)

end = DummyOperator(
    task_id='End',
    trigger_rule='none_failed',
    dag=dag)

send_request=PythonOperator(task_id='send_request'
            ,provide_context=True
            ,python_callable=extract_data
            ,dag=dag
            )

crush_data=PythonOperator(task_id='crush_data'
            ,provide_context=True
            ,python_callable=process_data
            ,dag=dag
            )


write_data=PythonOperator(task_id='write_data'
            ,provide_context=True
            ,python_callable=write_db
            ,dag=dag
            )

begin>>send_request>>crush_data>>write_data>>end