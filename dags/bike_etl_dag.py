from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/scripts')

# ========================================
# 기본 설정
# ========================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ========================================
# Task 함수들
# ========================================
def extract_task(**context):
    from extract import fetch_bike_data
    
    print("=" * 50)
    print("Extract 시작: API 데이터 수집")
    print("=" * 50)
    
    raw_data = fetch_bike_data()
    print(f"수집 완료: {raw_data['total_count']}개 대여소")
    
    context['ti'].xcom_push(key='raw_data', value=raw_data)
    return raw_data


def load_raw_task(**context):
    from load_raw import upload_to_s3
    
    print("=" * 50)
    print("Load Raw 시작: S3 원본 저장")
    print("=" * 50)
    
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    s3_path = upload_to_s3(raw_data)
    print(f"저장 완료: {s3_path}")
    
    return s3_path


def transform_task(**context):
    from transform import raw_to_dataframe, clean_bike_data
    
    print("=" * 50)
    print("Transform 시작: 데이터 정제")
    print("=" * 50)
    
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = raw_to_dataframe(raw_data)
    cleaned_df = clean_bike_data(df)
    print(f"정제 완료: {len(cleaned_df)}행")
    
    cleaned_df['collected_at'] = cleaned_df['collected_at'].astype(str)
    context['ti'].xcom_push(key='cleaned_data', value=cleaned_df.to_dict())
    
    return len(cleaned_df)


def validate_task(**context):
    import pandas as pd
    from validate import validate_bike_data
    
    print("=" * 50)
    print("Validate 시작: 데이터 품질 검증")
    print("=" * 50)
    
    cleaned_dict = context['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    cleaned_df = pd.DataFrame(cleaned_dict)
    
    result = validate_bike_data(cleaned_df)
    print(f"검증 완료: {result['status']}")
    
    return result


def load_cleaned_task(**context):
    import pandas as pd
    from load_cleaned import save_to_parquet_s3
    
    print("=" * 50)
    print("Load Cleaned 시작: S3 정제 데이터 저장")
    print("=" * 50)
    
    cleaned_dict = context['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    cleaned_df = pd.DataFrame(cleaned_dict)
    
    s3_path = save_to_parquet_s3(cleaned_df)
    print(f"저장 완료: {s3_path}")
    
    return s3_path


def create_mart_task(**context):
    import pandas as pd
    from mart import create_all_marts
    from load_mart import save_all_marts_to_s3
    
    print("=" * 50)
    print("Create Mart 시작: 분석 마트 생성")
    print("=" * 50)
    
    cleaned_dict = context['ti'].xcom_pull(key='cleaned_data', task_ids='transform')
    cleaned_df = pd.DataFrame(cleaned_dict)
    
    # 마트 생성
    marts = create_all_marts(cleaned_df)
    
    # S3에 저장
    saved_paths = save_all_marts_to_s3(marts)
    
    print("\n마트 저장 완료:")
    for name, path in saved_paths.items():
        print(f"  - {name}: {path}")
    
    return saved_paths


# ========================================
# DAG 정의
# ========================================
with DAG(
    dag_id='bike_rental_etl',
    default_args=default_args,
    description='서울시 공공자전거 ETL 파이프라인',
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['etl', 'bike', 'seoul'],
) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
        provide_context=True,
    )
    
    load_raw = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw_task,
        provide_context=True,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
        provide_context=True,
    )
    
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_task,
        provide_context=True,
    )
    
    load_cleaned = PythonOperator(
        task_id='load_cleaned',
        python_callable=load_cleaned_task,
        provide_context=True,
    )
    
    create_mart = PythonOperator(
        task_id='create_mart',
        python_callable=create_mart_task,
        provide_context=True,
    )
    