import pandas as pd
import boto3
import os
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )


def save_to_parquet_s3(df, prefix='cleaned/bike_rental'):
    """
    DataFrame을 Parquet 형태로 S3에 저장
    
    저장 경로: s3://bucket/cleaned/bike_rental/dt=YYYY-MM-DD/data.parquet
    """
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    today = datetime.now().strftime('%Y-%m-%d')
    s3_key = f"{prefix}/dt={today}/data.parquet"
    
    try:
        # DataFrame을 Parquet 바이트로 변환
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        
        # S3에 업로드
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        print(f"✅ Parquet 저장 성공: s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
        
    except Exception as e:
        print(f"❌ 저장 실패: {e}")
        raise


def read_parquet_from_s3(prefix='cleaned/bike_rental', date=None):
    """
    S3에서 Parquet 파일 읽기
    """
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    s3_key = f"{prefix}/dt={date}/data.parquet"
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        
        print(f"✅ 읽기 성공: s3://{bucket_name}/{s3_key}")
        return df
        
    except Exception as e:
        print(f"❌ 읽기 실패: {e}")
        raise


# 테스트
if __name__ == "__main__":
    # 테스트용 DataFrame
    test_df = pd.DataFrame({
        'station_id': ['ST-1', 'ST-2'],
        'station_name': ['망원역', '합정역'],
        'bike_available': [3, 10]
    })
    
    save_to_parquet_s3(test_df)