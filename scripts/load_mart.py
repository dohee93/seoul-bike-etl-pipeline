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


def save_mart_to_s3(df, mart_name):
    """
    마트 데이터를 S3에 저장
    
    저장 경로: s3://bucket/mart/{mart_name}/dt=YYYY-MM-DD/data.parquet
    """
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    today = datetime.now().strftime('%Y-%m-%d')
    s3_key = f"mart/{mart_name}/dt={today}/data.parquet"
    
    try:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        print(f"✅ 마트 저장 성공: s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
        
    except Exception as e:
        print(f"❌ 저장 실패: {e}")
        raise


def save_all_marts_to_s3(marts):
    """
    모든 마트를 S3에 저장
    """
    saved_paths = {}
    
    for mart_name, df in marts.items():
        path = save_mart_to_s3(df, mart_name)
        saved_paths[mart_name] = path
    
    return saved_paths


# 테스트
if __name__ == "__main__":
    test_df = pd.DataFrame({
        'station_id': ['ST-1', 'ST-2'],
        'avg_bike_available': [5.5, 10.2]
    })
    
    save_mart_to_s3(test_df, 'test_mart')