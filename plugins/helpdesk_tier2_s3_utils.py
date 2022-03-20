import csv
import json
from io import StringIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from helpdesk_database_utils import get_pk_value

TIER1_BUCKET = "datalake-tier1-raw"
TIER2_BUCKET = "datalake-tier2-staging"
S3_HOOK = S3Hook(aws_conn_id='airflow_aws_conn')

def read_tier1_response(year, month, day, schema, csv_data) -> dict():
    """
    전달받을 날짜에 제출된 객체들을 데이터레이크 Tier1 에서 읽어옵니다.
    """
    prefix_key = f"aib/help-desk/{schema}/{year}/{month}/{day}"
    list_keys = S3_HOOK.list_keys(bucket_name=TIER1_BUCKET, prefix=prefix_key)
   
    if not list_keys:
        status_code, result = 204, f"No data to be loaded on {year}-{month}-{day}"
        return None
    
    for key in list_keys:
        resp_json = S3_HOOK.read_key(key=key, bucket_name=TIER1_BUCKET)
        resp_dict = json.loads(resp_json, encoding='utf-8-sig')
        github_id = resp_dict['user']['id']
        ind_id = get_pk_value('public.TSU_INDI','individual_id','github_id',github_id)
        print("ind_id : ", ind_id)
        type_tool_id = get_pk_value('public.TCA_TYPE_TO', 'type_tool_id', 'name', 'github')
        csv_data.append( 
                    [ind_id
                    ,type_tool_id
                    ,resp_dict['body']
                    ,resp_dict['created_at']
                    ,resp_dict['updated_at']]
                    )

    return csv_data

def read_tier2_data(key):
    """
    tier2 에 데이터가 저장되어 있는지 확인합니다.
    """
    s3_data = S3_HOOK.read_key(key=key, bucket_name=TIER2_BUCKET)
    return s3_data


def load_csv_tier2(s3_key, csv_data):
    """
    csv_data 를 데이터레이크 Tier 2에 적재합니다.
    """

    conn = S3_HOOK.get_conn()
    with StringIO() as file:
        csv_writer = csv.writer(file)
        csv_writer.writerows(csv_data)
        conn.put_object(Bucket=TIER2_BUCKET, Key=s3_key, Body=file.getvalue())
    print('Success to load data')

    return None