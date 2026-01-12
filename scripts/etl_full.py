from extract import fetch_bike_data
from load_raw import upload_to_s3, read_from_s3
from transform import raw_to_dataframe, clean_bike_data, print_data_summary
from load_cleaned import save_to_parquet_s3

def run_full_etl():
    """
    전체 ETL 파이프라인 실행
    Extract → Load Raw → Transform → Load Cleaned
    """

    # ========================================
    # 1. Extract: API에서 데이터 수집
    # ========================================
    print("\n" + "="*50)
    print(" 1단계: Extract (데이터 수집)")
    print("="*50)
    
    raw_data = fetch_bike_data()
    print(f"수집 완료: {raw_data['total_count']}개 대여소")

    # ========================================
    # 2. Load Raw: S3 Raw Layer에 저장
    # ========================================
    print("\n" + "="*50)
    print(" 2단계: Load Raw (S3 원본 저장)")
    print("="*50)
    
    raw_path = upload_to_s3(raw_data)

    # ========================================
    # 3. Transform: 데이터 정제
    # ========================================
    print("\n" + "="*50)
    print(" 3단계: Transform (데이터 정제)")
    print("="*50)
    
    df = raw_to_dataframe(raw_data)
    cleaned_df = clean_bike_data(df)

    # ========================================
    # 4. Load Cleaned: S3 Cleaned Layer에 저장
    # ========================================
    print("\n" + "="*50)
    print(" 4단계: Load Cleaned (S3 정제 데이터 저장)")
    print("="*50)
    
    cleaned_path = save_to_parquet_s3(cleaned_df)

    # ========================================
    # 완료 요약
    # ========================================
    print("\n" + "="*50)
    print(" ✅ ETL 파이프라인 완료!")
    print("="*50)
    print(f"Raw 저장: {raw_path}")
    print(f"Cleaned 저장: {cleaned_path}")
    print(f"처리된 행 수: {len(cleaned_df)}")
    
    return cleaned_df


if __name__ == "__main__":
    result = run_full_etl()
    print_data_summary(result, "최종 결과")