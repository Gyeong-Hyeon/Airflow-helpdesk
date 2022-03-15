import json
from sys import prefix
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


dag_params = {
    'dag_id' : 'hd_compare_buckets',
    'start_date' : datetime(2022,2,1),
    'schedule_interval' : '@once',
    'catchup' : False,
    'params' : {
        'retries' : 3,
        'retry_delay' : timedelta(seconds=2)
    },
    'tags':['Help Desk'],
    'default_args' : {'owner':'Gyeong-Hyeon'}
}

S3_HOOK = S3Hook(aws_conn_id='airflow_aws_conn')
BUCKET = "datalake-tier1-raw"
ORG_BUCKET = "airflow-periodic-data-storage"

def find_files(org_node_id, new_node_id, org_files, bucket):
    """
    org_node_id에는 있지만 new_node_id에는 없는 값을 찾습니다.
    해당 노드아이디를 가진 가장 최신 json파일을 bucket에서 찾아 각 파일의 이름과 내용을 리턴합니다.
    - file_list: new_node_id에 존재하지 않는 node_id를 가진 가장 최신 json 파일명 리스트
    - json_list: new_node_id에 존재하지 않는 node_id를 가진 가장 최신 json 객체 리스트
    """
    
    file_list = list()
    json_list = list()
    not_found = org_node_id.difference(new_node_id)
    for node_id in not_found:
        file_names = [path for path in org_files if node_id in path]
        resp_json = S3_HOOK.read_key(key=file_names[-1], bucket_name=bucket)
        
        file_list.append(file_names[-1])
        json_list.append(resp_json)
    
    return file_list, json_list


def compare_buckets(org_files, new_files):
    """
    periodic 버킷의 데이터와 Datalake 버킷의 데이터를 비교합니다.
    webhook으로는 같은 이슈에 여러 데이터가 저장되지만 api는 한 이슈당 가장 최신의 데이터만 받아오므로 set자료형으로 비교합니다.
    - org_files: 비교 기준이 되는 버킷(periodic)의 키 리스트
    - new_files: 비교하고자하는 버킷(Datalake)
    """

    org_node_id = set(map(lambda path: path[path.find('Z-')+2:], org_files))
    new_node_id = set(map(lambda path: path[path.find('Z-')+2:], new_files))
    
    if org_node_id == new_node_id:
        return "ALL NODE_IDS ARE SAVED"

    not_in_new = find_files(org_node_id, new_node_id, org_files, ORG_BUCKET)
    not_in_org = find_files(new_node_id, org_node_id, new_files, BUCKET)

    return not_in_new, not_in_org

with DAG(**dag_params) as dag:

    def _hd_compare_buckets():
        """
        api를 통해 받아온 데이터를 저장한 버킷에 데이터가 빠짐없이 저장되었는지 웹훅으로 받아오던 기존 버킷과 비교합니다.
        - 기존 버킷 json 경로 예시: redshift_tables/public/dim_helpdesk_issues/2022/01/19/2022-01-19T14:06:42Z-I_kwDOEJH-fc5CDOoa.json
        - 새로운 버킷(Tier4) json 경로 예시: aib/help-desk/issue/2022/01/19/T14:06:42Z-I_kwDOEJH-fc5CDOoa.json
        - 기존 버킷에는 저장되지 않았으나 새로운 버킷에는 저장된 데이터의 case는 무시합니다.
        - 기존 버켓에는 있으나 새로운 버켓에는 없는 파일의 경우 기존 버킷의 데이터를 이주합니다.   
        """
        schemas = [
            ('dim_help_desk_issues', 'issue'),
            ('dim_help_desk_comments', 'comment')
        ]
        
        for org_schema, new_schema in schemas:
            org_prefix, new_prefix = f"redshift_tables/public/{org_schema}/", f"aib/help-desk/{new_schema}/"
            org_files = S3_HOOK.list_keys(bucket_name=ORG_BUCKET, prefix=org_prefix)
            new_files = S3_HOOK.list_keys(bucket_name=BUCKET, prefix=new_prefix)

            compare_result = compare_buckets(org_files, new_files)

            if type(compare_result) == str:
                print(compare_result)
                continue
            
            print(f'finished checking {new_schema} node ids',
                    'NOT IN NEW:',
                    f'\tCNT:{len(compare_result[0][0])}',
                    f'\tFILE_NAMES:{compare_result[0][0]}',
                    sep='\n')
            
            for json_data in compare_result[0][1]:
                print(f'\tJSON_DATA:{json_data}')
                
            print('NOT IN OLD:',
                    f'\tCNT:{len(compare_result[1][0])}',
                    f'\tFILE_NAMES:{compare_result[1][0]}',
                    sep='\n')
            
            for json_data in compare_result[1][1]:
                print(f'\tJSON_DATA:{json_data}')

        return {
            "statusCode" : 200,
            "body" : "Finished compareing two buckets"
        }


    hd_compare_buckets = PythonOperator(
        task_id = 'hd_compare_buckets',
        python_callable = _hd_compare_buckets
    )

    hd_compare_buckets