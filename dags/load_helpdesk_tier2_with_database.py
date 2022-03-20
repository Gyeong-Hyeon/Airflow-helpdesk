"""
데이터레이크 Tier 1 에 저장된 데이터를 
데이터레이크 Tier 2 와 데이터베이스에 저장합니다.
"""

import json
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator
from datahub_provider.entities import Dataset

from helpdesk_database_utils import insert_csv_to_database
from helpdesk_tier2_s3_utils import read_tier1_response, read_tier2_data, load_csv_tier2


dag_params = {
    'dag_id': 'load_helpdesk_tier2_with_database',
    'start_date': datetime(2020,9,2),
    'schedule_interval': '@daily',
    'catchup' : True,
    'params': {
        'retries' : 2,
        'retry_delay' : timedelta(seconds=2)
    },
    'tags':['Help Desk','Tier2','datalake'],
    'default_args':{'owner':'Gyeong-Hyeon'}
}

with DAG(**dag_params) as dag:

    def _load_helpdesk_question_to_tier2(**context):
        """
        tier1의 json 데이터를 읽어 파싱 후 csv로 변환하여 tier2에 저장하는 함수입니다.
        - hd : 헬프데스크
        파일명: s3://datalake-tier2-staging/{데이터 형식}/{팀 명칭}/{세부분류}/{날짜}/{세부 명칭}
        """

        date = context['templates_dict']['prev_execution_date']
        year, month, day = date[0:4], date[5:7], date[8:10]
        column = [['individual_id','type_tool_id','question_body','created_at','updated_at']]
        csv_data = read_tier1_response(year, month, day, 'issue', column)
        
        if csv_data == None:
            print(f"No Helpdesk Question CSV File in DataLake Tier1. : {year}/{month}/{day}")
            raise Exception
            

        s3_key = f'aib-db/TSA_QUES_HI/{year}/{month}/{day}/{year}-{month}-{day}.csv'
        load_csv_tier2(s3_key, csv_data)
        
        return f"Loaded Helpdesk Question CSV File in DataLake Tier2. : {year}/{month}/{day}"

    def _insert_helpdesk_question_to_database(**context):
        """
        tier2 에 날짜별로 저장된 데이터를 읽어서 데이터베이스에 저장하는 함수입니다.
        - hd : 헬프데스크
        """
        date = context['templates_dict']['prev_execution_date']
        year, month, day = date[0:4], date[5:7], date[8:10]
        
        s3_key = f'aib-db/TSA_QUES_HI/{year}/{month}/{day}/{year}-{month}-{day}.csv'
        s3_data = read_tier2_data(key=s3_key)

        if s3_data == None :
            return False
        elif s3_data != None:
            insert_csv_to_database('public.TSA_QUES_HI',s3_data)

        return f"Inserted Helpdesk Question to Database. : {year}/{month}/{day}"
        
    
    load_hd_data_to_tier2 = PythonOperator(
        task_id = 'load_hd_data_to_tier2',
        python_callable = _load_helpdesk_question_to_tier2,
        templates_dict={
            'prev_execution_date':"{{prev_execution_date}}"
        },
        inlets          = {
            "datasets" : [
                Dataset("s3",
                        "s3://datalake-tier1-raw/aib/help-desk/"),
                Dataset("postgres",
                        "prod.public.tca_type_to")
            ]
        },
        outlets         = {
            "datasets" : [
                Dataset("s3", "s3://datalake-tier2-staging/TSA_QUES_HI/")
            ]
        }
    )

    insert_hd_data_to_aib_db = PythonOperator(
        task_id = 'insert_hd_data_to_aib_db',
        python_callable = _insert_helpdesk_question_to_database,
        templates_dict={
            'prev_execution_date':"{{prev_execution_date}}"
        },
        inlets          = {
            "datasets" : [
                Dataset("s3", "s3://datalake-tier2-staging/TSA_QUES_HI/")
            ]
        },
        outlets         = {
            "datasets" : [
                Dataset("postgres", "prod.public.tsa_ques_hi")
            ]
        }
    )

    load_hd_data_to_tier2 >> insert_hd_data_to_aib_db
