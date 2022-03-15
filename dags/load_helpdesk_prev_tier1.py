import json
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.variable import Variable
from datahub_provider.entities import Dataset

from helpdesk_tier1_utils import extract_issues, extract_comments


dag_params = {
    'dag_id' : 'load_helpdesk_prev_tier1',
    'start_date' : datetime(2020,9,16),
    'schedule_interval' : '@daily',
    'catchup' : True,
    'params' : {
        'retries' : 3,
        'retry_delay' : timedelta(seconds=2)
    },
    'tags':['Help Desk','Tier1','datalake'],
    'default_args' : {'owner':'Gyeong-Hyeon'}
}

BUCKET = "datalake-tier1-raw"
TOKEN = Variable.get("github_token")
S3_HOOK = S3Hook(aws_conn_id='airflow_aws_conn')

def load_s3(updated_date, item, schema):
    """
    데이터를 S3에 저장합니다.
    """
        
    year, month, day = updated_date[0:4], updated_date[5:7], updated_date[8:10]
    file_name = f"{updated_date[10:]}-{item['node_id']}.json"
    datalake_path = f"aib/help-desk/{schema}/{year}/{month}/{day}/{file_name}" 
    byte_file = bytes(json.dumps(item, ensure_ascii=False).encode('UTF-8'))

    print("starting load to s3")

    try:
        S3_HOOK.load_bytes(
            bytes_data = byte_file,
            key = datalake_path,
            bucket_name = BUCKET
        )
        print("Data saved successfully")
        statusCode, body = 200, "S3 PUT SUCCESS "
    except Exception:
        import traceback
        
        print("Failed to save data")
        print(traceback.format_exc())
        statusCode, body = 500, "S3 PUT FAIL"
    
    return {
        "statusCode" : statusCode,
        "body" : body
    }

with DAG(**dag_params) as dag:
    
    def _hd_prev_tier1():
        """
        request로 받아온 issues를 읽어 파싱 후 S3에 저장하는 함수입니다.
        """
        all_issues = extract_issues('codestates','help-desk-ds', TOKEN)

        for page in all_issues:
            for issue in page:
                #pull request는 저장하지 않습니다.
                if "pull" in issue['html_url']:
                     print(f"{issue['url']} is pull request. passing...")
                     continue
                load_s3(issue['updated_at'], issue, 'issue')

                comments = extract_comments(issue['comments_url'], TOKEN).json()
                if len(comments) == 0:
                    continue
                for comment in comments:
                    load_s3(comment['updated_at'], comment, 'comment')            

        return {
            "statusCode" : 200,
            "body" : "Finished loading data"
        }


    hd_prev_tier1 = PythonOperator(
        task_id = 'hd_prev_tier1',
        python_callable = _hd_prev_tier1,
        inlets          = {
            "datasets" : [
                Dataset("github",
                        "[AIB] Help-Desk")
            ]
        },
        outlets         = {
            "datasets" : [
                Dataset("s3", "s3://datalake-tier1-raw/aib/help-desk/")
            ]
        }
    )

    hd_prev_tier1