import boto3
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    """
    S3 클라이언트 생성
    """
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )


def upload_to_s3(data, prefix='raw/bike_rental'):
    """
    데이터를 S3에 JSON 형태로 저장
    
    저장 경로: s3://bucket/raw/bike_rental/dt=YYYY-MM-DD/data.json
    """
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    # 오늘 날짜로 파티션 키 생성
    today = datetime.now().strftime('%Y-%m-%d')
    
    # S3 키 (경로) 생성
    s3_key = f"{prefix}/dt={today}/data.json"
    
    try:
        # JSON 문자열로 변환
        json_data = json.dumps(data, ensure_ascii=False, indent=2)
        
        # S3에 업로드
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"✅ 업로드 성공: s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
        
    except Exception as e:
        print(f"❌ 업로드 실패: {e}")
        raise


def upload_to_s3_with_timestamp(data, prefix='raw/bike_rental'):
    """
    시간별로 여러 파일 저장하고 싶을 때 사용
    
    저장 경로: s3://bucket/raw/bike_rental/dt=YYYY-MM-DD/HHMMSS.json
    """
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    now = datetime.now()
    date_partition = now.strftime('%Y-%m-%d')
    timestamp = now.strftime('%H%M%S')
    
    s3_key = f"{prefix}/dt={date_partition}/{timestamp}.json"
    
    try:
        json_data = json.dumps(data, ensure_ascii=False, indent=2)
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"✅ 업로드 성공: s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
        
    except Exception as e:
        print(f"❌ 업로드 실패: {e}")
        raise


def list_s3_files(prefix='raw/bike_rental'):
    """
    S3에 저장된 파일 목록 확인
    """
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )
    
    if 'Contents' in response:
        print(f"\n=== S3 파일 목록 ({prefix}) ===")
        for obj in response['Contents']:
            print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        return response['Contents']
    else:
        print("저장된 파일이 없습니다.")
        return []

# S3에 저장된 데이터 불러오는 함수
def read_from_s3(prefix='raw/bike_rental', date=None):
    """
    S3에서 데이터 읽어오기
    
    date: 'YYYY-MM-DD' 형식. None이면 오늘 날짜
    """
    from datetime import datetime
    
    s3_client = get_s3_client()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    s3_key = f"{prefix}/dt={date}/data.json"
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        print(f"✅ 읽기 성공: s3://{bucket_name}/{s3_key}")
        return data
        
    except Exception as e:
        print(f"❌ 읽기 실패: {e}")
        raise

# 테스트
if __name__ == "__main__":
    # 테스트 데이터
    test_data = {
        'collected_at': datetime.now().isoformat(),
        'total_count': 2,
        'data': [
            {'stationId': 'ST-1', 'stationName': '테스트역 1번출구'},
            {'stationId': 'ST-2', 'stationName': '테스트역 2번출구'}
        ]
    }
    
    # 업로드 테스트
    upload_to_s3(test_data)
    
    # 파일 목록 확인
    list_s3_files()