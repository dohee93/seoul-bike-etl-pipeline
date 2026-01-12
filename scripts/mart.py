import pandas as pd
from datetime import datetime

def create_daily_station_summary(df):
    """
    일별 대여소별 요약 마트
    
    - 각 대여소의 일별 평균 자전거 수
    - 평균 거치율
    - 데이터 수집 횟수
    """
    
    print("=" * 50)
    print(" 마트 생성: 일별 대여소 요약")
    print("=" * 50)
    
    mart_df = df.groupby(['dt', 'station_id', 'station_name']).agg({
        'bike_available': ['mean', 'min', 'max'],
        'rack_total': 'first',
        'latitude': 'first',
        'longitude': 'first',
        'shared_rate': 'mean',
        'utilization_rate': 'mean',
    }).round(2)
    
    # 컬럼명 정리
    mart_df.columns = [
        'avg_bike_available',
        'min_bike_available', 
        'max_bike_available',
        'rack_total',
        'latitude',
        'longitude',
        'avg_shared_rate',
        'avg_utilization_rate',
    ]
    
    mart_df = mart_df.reset_index()
    
    print(f"✅ 생성 완료: {len(mart_df)}행")
    
    return mart_df


def create_hourly_pattern(df):
    """
    시간대별 이용 패턴 마트
    
    - 시간대별 평균 자전거 가용 수
    - 시간대별 평균 거치율
    """
    
    print("=" * 50)
    print(" 마트 생성: 시간대별 이용 패턴")
    print("=" * 50)
    
    mart_df = df.groupby(['dt', 'hour']).agg({
        'bike_available': 'mean',
        'rack_empty': 'mean',
        'shared_rate': 'mean',
        'utilization_rate': 'mean',
        'station_id': 'count'  # 데이터 수
    }).round(2)
    
    mart_df.columns = [
        'avg_bike_available',
        'avg_rack_empty',
        'avg_shared_rate',
        'avg_utilization_rate',
        'record_count'
    ]
    
    mart_df = mart_df.reset_index()
    
    print(f"✅ 생성 완료: {len(mart_df)}행")
    
    return mart_df


def create_station_ranking(df):
    """
    대여소 랭킹 마트
    
    - 가장 인기 있는 대여소 (자전거가 적은 곳 = 많이 빌려감)
    - 가장 한산한 대여소 (자전거가 많은 곳)
    """
    
    print("=" * 50)
    print(" 마트 생성: 대여소 랭킹")
    print("=" * 50)
    
    mart_df = df.groupby(['station_id', 'station_name', 'latitude', 'longitude']).agg({
        'bike_available': 'mean',
        'rack_total': 'first',
        'utilization_rate': 'mean',
    }).round(2)
    
    mart_df.columns = [
        'avg_bike_available',
        'rack_total',
        'avg_utilization_rate',
    ]
    
    mart_df = mart_df.reset_index()
    
    # 이용률 기준 랭킹 (높을수록 자전거가 많이 남아있음)
    mart_df['availability_rank'] = mart_df['avg_utilization_rate'].rank(ascending=False).astype(int)
    
    # 정렬
    mart_df = mart_df.sort_values('availability_rank')
    
    print(f"✅ 생성 완료: {len(mart_df)}행")
    
    return mart_df


def create_all_marts(df):
    """
    모든 마트 테이블 생성
    """
    
    print("\n" + "=" * 50)
    print(" 전체 마트 생성 시작")
    print("=" * 50 + "\n")
    
    marts = {
        'daily_station_summary': create_daily_station_summary(df),
        'hourly_pattern': create_hourly_pattern(df),
        'station_ranking': create_station_ranking(df),
    }
    
    print("\n" + "=" * 50)
    print(" 전체 마트 생성 완료!")
    print("=" * 50)
    
    for name, mart_df in marts.items():
        print(f"  - {name}: {len(mart_df)}행")
    
    return marts


# 테스트
if __name__ == "__main__":
    # 테스트용 데이터
    test_df = pd.DataFrame({
        'dt': ['2024-01-15'] * 6,
        'hour': [9, 9, 9, 10, 10, 10],
        'station_id': ['ST-1', 'ST-2', 'ST-3', 'ST-1', 'ST-2', 'ST-3'],
        'station_name': ['역1', '역2', '역3', '역1', '역2', '역3'],
        'bike_available': [5, 10, 3, 4, 12, 2],
        'rack_total': [15, 20, 10, 15, 20, 10],
        'rack_empty': [10, 10, 7, 11, 8, 8],
        'latitude': [37.5, 37.51, 37.52, 37.5, 37.51, 37.52],
        'longitude': [126.9, 126.91, 126.92, 126.9, 126.91, 126.92],
        'shared_rate': [33, 50, 30, 27, 60, 20],
        'utilization_rate': [33.33, 50.0, 30.0, 26.67, 60.0, 20.0],
    })
    
    marts = create_all_marts(test_df)
    
    print("\n=== Daily Station Summary ===")
    print(marts['daily_station_summary'].head())
    
    print("\n=== Hourly Pattern ===")
    print(marts['hourly_pattern'].head())
    
    print("\n=== Station Ranking ===")
    print(marts['station_ranking'].head())