from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from fredapi import Fred
from io import StringIO
import pandas as pd

# airflow변수 설정 필요
AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
FRED_API_KEY = Variable.get("FRED_API_KEY")

# FRED SERIES ID (CPIAUCSL : 소비자 물가 지수)
SERIES_ID = 'CPIAUCSL'

# 데이터베이스, 스키마, 테이블 정보
SNOWFLAKE_DATABASE = "dev"
SNOWFLAKE_SCHEMA = "raw_data"
SNOWFLAKE_TABLE = SERIES_ID

# 테이블 생성 쿼리 : 컬럼 별도 설정 필요
CREATE_TABLE_SQL = f"""
DROP TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE};
CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
    date DATE,
    consumer_price_index FLOAT
);
"""

# 테이블 ZKVL 쿼리 : 컬럼 별도 설정 필요
COPY_INTO_SQL=f"""
    USE WAREHOUSE COMPUTE_WH;
    COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    FROM 's3://pjt-currenomics/{SNOWFLAKE_TABLE}/{SNOWFLAKE_TABLE}.csv'
    CREDENTIALS=(
        AWS_KEY_ID='{AWS_ACCESS_KEY}'
        AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}'
    )
    FILE_FORMAT=(
        TYPE='CSV',
        SKIP_HEADER=1,
        FIELD_OPTIONALLY_ENCLOSED_BY='"',
        NULL_IF = ('', 'NULL')
    );
"""

# 데이터프레임을 csv로 변환하여 S3로 업로드 (extract에서는 tmp에 저장)
def upload_csv_to_S3(data, is_tmp, ds_nodash):
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    if is_tmp:
        s3_hook.load_string(
            string_data=csv_data,
            key=f"tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv",
            bucket_name="pjt-currenomics",
            replace=True  
        )
    else:
        s3_hook.load_string(
            string_data=csv_data,
            key=f"{SNOWFLAKE_TABLE}/{SNOWFLAKE_TABLE}.csv",
            bucket_name="pjt-currenomics",
            replace=True  
        )


# extract 함수: FRED API에서 CPIAUCSL 데이터 추출
def extract(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    api_key = FRED_API_KEY
    fred = Fred(api_key=api_key)

    # CPIAUCSL 데이터 가져오기
    cpi_data = fred.get_series(SERIES_ID, start_date='2019-01-01')

    # pandas로 포팅 시 테이블 스키마 명시
    cpi_df = pd.DataFrame({
        "date": cpi_data.index,
        "consumer_price_index": cpi_data.values
    })

    # S3로 CSV저장
    upload_csv_to_S3(cpi_df, True, ds_nodash)


# transform 함수: 데이터를 변환 후 S3 본 테이블에 저장
def transform(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    file_content = s3_hook.read_key(
        key=f"tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv", 
        bucket_name='pjt-currenomics'
    )

    # Pandas로 데이터 로드
    df = pd.read_csv(StringIO(file_content))  # S3 데이터를 pandas 데이터프레임으로 읽기

    # Pandas로 데이터 처리로직 (필요시 추가 변환)
    # 예시로 데이터는 그대로 두고, 필요에 따라 처리
    modified_df = df

    # 변환 후 S3로 업로드
    upload_csv_to_S3(modified_df, False, ds_nodash)


# delete 함수: 임시 CSV 파일 삭제
def delete(**kwargs):
    ds_nodash = kwargs['ds_nodash']
    s3_hook = S3Hook(aws_conn_id="aws_conn_id")
    s3_hook.delete_objects(bucket="pjt-currenomics", keys=[f'tmp/{ds_nodash}_{SNOWFLAKE_TABLE}_tmp.csv'])


# DAG 정의
dag = DAG(
    dag_id = 'CPIAUCSL',
    start_date = datetime(2019,1,1), 
    schedule_interval = '0 9 * * *',  # 매일 오전 9시에 실행
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
    }
)

# 1. FRED API 호출 및 S3저장
extract_task = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        provide_context=True,
        dag = dag
    )

# 2. tmp에 저장된 CSV호출 후 데이터 변환 후 S3 본 테이블에 이관
transform_task = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        provide_context=True,
        dag = dag
    )

# 3. 테이블 생성 태스크 (IF NOT EXISTS)
create_table_task = SnowflakeOperator(
        task_id="create_table_if_not_exists",
        snowflake_conn_id='snowflake_conn_id',
        sql=CREATE_TABLE_SQL,
        dag = dag
    )

# 4. S3의 정제 데이터를 Snowflake로 COPY 태스크
load_task = SnowflakeOperator(
        task_id="load_to_snowflake",
        snowflake_conn_id='snowflake_conn_id',
        sql=COPY_INTO_SQL,
        dag = dag
    )

# 5. S3의 임시파일 삭제
delete_temp_data_task = PythonOperator(
        task_id = 'delete',
        python_callable = delete,
        provide_context=True,
        dag = dag
    )

# 태스크 순서 정의
extract_task >> transform_task >> create_table_task >> load_task >> delete_temp_data_task
