import pandas as pd
import numpy as np
from datetime import datetime

def raw_to_dataframe(raw_data):
    """
    Raw JSON 데이터를 DataFrame으로 변환
    """
    df = pd.DataFrame(raw_data['data'])
    df['collected_at'] = raw_data['collected_at']
    
    print(f"✅ DataFrame 변환 완료: {len(df)}행")
    return df


def clean_bike_data(df):
    """
    데이터 정제 (Transform)
    
    1. 컬럼명 정리 (영문 소문자 + 스네이크 케이스)
    2. 타입 변환
    3. 결측치 처리
    4. 이상치 필터링
    5. 파생 컬럼 추가
    """
    
    # 원본 복사
    cleaned = df.copy()
    
    # ========================================
    # 1. 컬럼명 정리
    # ========================================
    column_mapping = {
        'stationId': 'station_id',
        'stationName': 'station_name',
        'rackTotCnt': 'rack_total',
        'parkingBikeTotCnt': 'bike_available',
        'shared': 'shared_rate',
        'stationLatitude': 'latitude',
        'stationLongitude': 'longitude',
        'collected_at': 'collected_at'
    }
    
    cleaned = cleaned.rename(columns=column_mapping)
    print("✅ 1. 컬럼명 정리 완료")
    
    # ========================================
    # 2. 타입 변환
    # ========================================
    # 숫자형 컬럼 변환 (API 응답이 전부 string)
    numeric_columns = ['rack_total', 'bike_available', 'shared_rate']
    
    for col in numeric_columns:
        cleaned[col] = pd.to_numeric(cleaned[col], errors='coerce')
    
    # 위도/경도 float 변환
    cleaned['latitude'] = pd.to_numeric(cleaned['latitude'], errors='coerce')
    cleaned['longitude'] = pd.to_numeric(cleaned['longitude'], errors='coerce')
    
    # 수집 시간 datetime 변환
    cleaned['collected_at'] = pd.to_datetime(cleaned['collected_at'])
    
    print("✅ 2. 타입 변환 완료")
    
    # ========================================
    # 3. 결측치 처리
    # ========================================
    before_count = len(cleaned)
    
    # 필수 컬럼에 결측치 있으면 제거
    required_columns = ['station_id', 'station_name', 'latitude', 'longitude']
    cleaned = cleaned.dropna(subset=required_columns)
    
    after_count = len(cleaned)
    dropped = before_count - after_count
    
    print(f"✅ 3. 결측치 처리 완료 (제거: {dropped}행)")
    
    # ========================================
    # 4. 이상치 필터링
    # ========================================
    before_count = len(cleaned)
    
    # 자전거 수가 음수이면 제거
    cleaned = cleaned[cleaned['bike_available'] >= 0]
    
    # 거치대 수가 0 이하이면 제거
    cleaned = cleaned[cleaned['rack_total'] > 0]
    
    # 서울 범위 밖 좌표 제거 (위도: 37.4~37.7, 경도: 126.7~127.2)
    cleaned = cleaned[
        (cleaned['latitude'] >= 37.4) & 
        (cleaned['latitude'] <= 37.7) &
        (cleaned['longitude'] >= 126.7) & 
        (cleaned['longitude'] <= 127.2)
    ]
    
    after_count = len(cleaned)
    dropped = before_count - after_count
    
    print(f"✅ 4. 이상치 필터링 완료 (제거: {dropped}행)")
    
    # ========================================
    # 5. 파생 컬럼 추가
    # ========================================
    # 빈 거치대 수
    cleaned['rack_empty'] = cleaned['rack_total'] - cleaned['bike_available']
    
    # 이용률 (자전거 있는 비율)
    cleaned['utilization_rate'] = round(
        cleaned['bike_available'] / cleaned['rack_total'] * 100, 2
    )
    
    # 수집 날짜 (파티션용)
    cleaned['dt'] = cleaned['collected_at'].dt.strftime('%Y-%m-%d')
    
    # 수집 시간 (시간대 분석용)
    cleaned['hour'] = cleaned['collected_at'].dt.hour
    
    print("✅ 5. 파생 컬럼 추가 완료")
    
    # ========================================
    # 6. 컬럼 순서 정리
    # ========================================
    column_order = [
        'dt',
        'collected_at',
        'hour',
        'station_id',
        'station_name',
        'latitude',
        'longitude',
        'rack_total',
        'bike_available',
        'rack_empty',
        'shared_rate',
        'utilization_rate'
    ]
    
    cleaned = cleaned[column_order]
    
    print(f"\n✅ Transform 완료: {len(cleaned)}행, {len(cleaned.columns)}컬럼")
    
    return cleaned


def print_data_summary(df, title="데이터 요약"):
    """
    데이터 요약 정보 출력
    """
    print(f"\n{'='*50}")
    print(f" {title}")
    print('='*50)
    print(f"행 수: {len(df)}")
    print(f"컬럼 수: {len(df.columns)}")
    print(f"\n컬럼 목록:")
    for col in df.columns:
        print(f"  - {col}: {df[col].dtype}")
    print(f"\n처음 3행:")
    print(df.head(3).to_string())
    print(f"\n기본 통계:")
    print(df.describe().to_string())


# 테스트
if __name__ == "__main__":
    # 테스트용 샘플 데이터
    sample_raw = {
        'collected_at': '2024-01-15T14:30:00',
        'total_count': 3,
        'data': [
            {
                'stationId': 'ST-1',
                'stationName': '망원역 1번출구',
                'rackTotCnt': '15',
                'parkingBikeTotCnt': '3',
                'shared': '20',
                'stationLatitude': '37.5556',
                'stationLongitude': '126.9106'
            },
            {
                'stationId': 'ST-2',
                'stationName': '합정역 2번출구',
                'rackTotCnt': '20',
                'parkingBikeTotCnt': '10',
                'shared': '50',
                'stationLatitude': '37.5496',
                'stationLongitude': '126.9139'
            },
            {
                'stationId': 'ST-3',
                'stationName': '홍대입구역',
                'rackTotCnt': '25',
                'parkingBikeTotCnt': '0',
                'shared': '0',
                'stationLatitude': '37.5571',
                'stationLongitude': '126.9256'
            }
        ]
    }
    
    # Transform 실행
    df = raw_to_dataframe(sample_raw)
    cleaned_df = clean_bike_data(df)
    
    # 결과 확인
    print_data_summary(cleaned_df, "Cleaned 데이터")