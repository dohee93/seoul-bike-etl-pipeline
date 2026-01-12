import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import time

# .env 파일에서 환경변수 로드
load_dotenv()

import time

def fetch_bike_data_with_retry(max_retries=3):
    """
    재시도 로직이 포함된 데이터 수집
    """
    for attempt in range(max_retries):
        try:
            return fetch_bike_data()
        except Exception as e:
            print(f"시도 {attempt + 1}/{max_retries} 실패: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 10  # 10초, 20초, 30초
                print(f"{wait_time}초 후 재시도...")
                time.sleep(wait_time)
            else:
                print("최대 재시도 횟수 초과")
                raise

# extract.py 상단에 임시로 추가
def test_api():
    """API 연결 테스트"""
    import os
    from dotenv import load_dotenv
    import requests
    
    load_dotenv()
    
    api_key = os.getenv('SEOUL_API_KEY')
    print(f"1. API 키 확인: {api_key[:10]}..." if api_key else "❌ API 키 없음!")
    
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bikeList/1/5/"
    print(f"2. 요청 URL: {url}")
    
    response = requests.get(url, timeout=30)
    print(f"3. 응답 코드: {response.status_code}")
    print(f"4. 응답 내용: {response.text[:500]}")


def fetch_bike_data():
    """
    서울시 공공자전거 실시간 대여정보 수집
    """
    api_key = os.getenv('SEOUL_API_KEY')
    
    if not api_key:
        raise ValueError("SEOUL_API_KEY가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    # 전체 데이터 수집 (1~1000번)
    # 서울시 API는 한 번에 최대 1000건까지 조회 가능
    start_index = 1
    end_index = 1000
    
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bikeList/{start_index}/{end_index}/"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()  # HTTP 에러 발생 시 예외 발생
        
        data = response.json()
        
        # 응답 코드 확인
        result_code = data.get('rentBikeStatus', {}).get('RESULT', {}).get('CODE')
        
        if result_code != 'INFO-000':
            raise Exception(f"API 에러: {result_code}")
        
        # 대여소 목록 추출
        stations = data['rentBikeStatus']['row']
        
        print(f"[{datetime.now()}] 수집 완료: {len(stations)}개 대여소")
        
        return {
            'collected_at': datetime.now().isoformat(),
            'total_count': len(stations),
            'data': stations
        }
        
    except requests.exceptions.Timeout:
        print("API 요청 시간 초과")
        raise
    except requests.exceptions.RequestException as e:
        print(f"API 요청 실패: {e}")
        raise


def fetch_all_bike_data():
    """
    전체 대여소 데이터 수집 (1000개 이상일 경우 페이징)
    """
    api_key = os.getenv('SEOUL_API_KEY')
    all_stations = []
    
    # 먼저 전체 개수 확인
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bikeList/1/1/"
    response = requests.get(url, timeout=30)
    data = response.json()
    total_count = data['rentBikeStatus']['list_total_count']
    
    print(f"전체 대여소 수: {total_count}")
    
    # 1000개씩 페이징하며 수집
    for start in range(1, total_count + 1, 1000):
        end = min(start + 999, total_count)
        
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/bikeList/{start}/{end}/"
        response = requests.get(url, timeout=30)
        data = response.json()
        
        stations = data['rentBikeStatus']['row']
        all_stations.extend(stations)
        
        print(f"수집: {start}~{end} ({len(stations)}개)")
    
    return {
        'collected_at': datetime.now().isoformat(),
        'total_count': len(all_stations),
        'data': all_stations
    }


# 직접 실행 시 테스트
if __name__ == "__main__":
    test_api()
    result = fetch_bike_data()
    print(f"\n=== 수집 결과 ===")
    print(f"수집 시간: {result['collected_at']}")
    print(f"대여소 수: {result['total_count']}")
    print(f"\n=== 샘플 데이터 (첫 번째 대여소) ===")
    print(result['data'][0])