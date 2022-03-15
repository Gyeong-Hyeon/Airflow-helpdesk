# Airflow-helpdesk

<p align='center'><img width="700" alt="pipeline" src="https://i.imgur.com/OAmJFQM.png"></p>

## Introduction 🙋

- Github의 issue 데이터가 발생할 때마다 데이터 레이크 Tier1에 적재하는 Webhook이 실행되기 이전의 데이터를 REST API를 통해 받아옵니다.
- 데이터 레이크 Tier1의 데이터를 daily 데이터 레이크 Tier2에 csv형태로 변환하여 저장합니다.
- 데이터 레이크 Tier2의 csv데이터를 daily 데이터 웨어하우스에 적재합니다.

## Tools 🛠

- **Data Lake**: `AWS S3`
- **ETL pipeline**: `Airflow`
- **Data Warehouse**: `Postgresql`

## Python files 🗂
### Dags
- `hd_compare_buckets.py` : 데이터 레이크 Tier1에 저장된 데이터 검수를 진행합니다.
- `load_helpdesk_prev_tier1.py` : Webhook 실행 이전 github 데이터를 REST API를 통해 받아와 Tier1에 저장합니다.
- `load_helpdesk_tier2_with_database.py`: 데이터 레이크 Tier1의 데이터를 데이터 웨어하우스 스키마에 맞는 데이터만 추출하여 csv형태로 Tier2에 저장하고, Tier2의 csv를 읽어 데이터 웨어하우스에 적재합니다.

### Plugins
- `helpdesk_database_utils.py` : 데이터 웨어하우스에 데이터를 저장하는 기능과 관련된 함수들이 작성되어 있습니다.
- `helpdesk_tier1_utils.py` : 데이터 레이크 tier1에 데이터를 저장하는 기능과 관련된 함수들이 작성되어 있습니다.
- `helpdesk_tier2_s3_utils.py` : 데이터 레이크 tier2에 데이터를 저장하는 기능과 관련된 함수들이 작성되어 있습니다.
