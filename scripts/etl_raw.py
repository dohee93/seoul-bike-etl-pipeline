from extract import fetch_bike_data
from load_raw import upload_to_s3

def run_extract_and_load():
    """
    수집 → S3 저장 전체 파이프라인 실행
    """
    print("=" * 50)
    print("1단계: 데이터 수집 시작")
    print("=" * 50)
    
    # 데이터 수집
    data = fetch_bike_data()
    print(f"수집 완료: {data['total_count']}개 대여소\n")
    
    print("=" * 50)
    print("2단계: S3 업로드 시작")
    print("=" * 50)
    
    # S3 업로드
    s3_path = upload_to_s3(data)
    
    print("\n" + "=" * 50)
    print("✅ 파이프라인 완료!")
    print(f"저장 위치: {s3_path}")
    print("=" * 50)
    
    return s3_path


if __name__ == "__main__":
    run_extract_and_load()