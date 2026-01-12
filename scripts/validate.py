import pandas as pd

class DataValidationError(Exception):
    """데이터 검증 실패 시 발생하는 예외"""
    pass


def validate_bike_data(df):
    """
    공공자전거 데이터 품질 검증
    
    검증 항목:
    1. 데이터가 비어있지 않은지
    2. 필수 컬럼이 존재하는지
    3. 필수 컬럼에 null이 없는지
    4. 숫자 값이 유효한 범위인지
    5. 최소 데이터 건수 충족하는지
    """
    
    errors = []
    warnings = []
    
    print("=" * 50)
    print(" 데이터 품질 검증 시작")
    print("=" * 50)
    
    # ========================================
    # 1. 데이터가 비어있지 않은지
    # ========================================
    if len(df) == 0:
        errors.append("❌ 데이터가 비어있습니다.")
    else:
        print(f"✅ 1. 데이터 존재 확인: {len(df)}행")
    
    # ========================================
    # 2. 필수 컬럼 존재 확인
    # ========================================
    required_columns = [
        'station_id',
        'station_name', 
        'bike_available',
        'rack_total',
        'latitude',
        'longitude'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        errors.append(f"❌ 필수 컬럼 누락: {missing_columns}")
    else:
        print(f"✅ 2. 필수 컬럼 존재 확인: {len(required_columns)}개")
    
    # ========================================
    # 3. 필수 컬럼 null 체크
    # ========================================
    if not missing_columns:
        null_counts = df[required_columns].isnull().sum()
        columns_with_nulls = null_counts[null_counts > 0]
        
        if len(columns_with_nulls) > 0:
            for col, count in columns_with_nulls.items():
                warnings.append(f"⚠️ {col}에 null 값 {count}개")
        else:
            print("✅ 3. 필수 컬럼 null 없음")
    
    # ========================================
    # 4. 숫자 값 유효성 검사
    # ========================================
    if 'bike_available' in df.columns:
        negative_bikes = (df['bike_available'] < 0).sum()
        if negative_bikes > 0:
            errors.append(f"❌ 자전거 수 음수: {negative_bikes}건")
        else:
            print("✅ 4. 자전거 수 유효 (음수 없음)")
    
    if 'rack_total' in df.columns:
        invalid_racks = (df['rack_total'] <= 0).sum()
        if invalid_racks > 0:
            warnings.append(f"⚠️ 거치대 수 0 이하: {invalid_racks}건")
        else:
            print("✅ 5. 거치대 수 유효")
    
    # ========================================
    # 5. 최소 데이터 건수 확인
    # ========================================
    MIN_ROWS = 100  # 최소 100개 대여소 데이터 필요
    
    if len(df) < MIN_ROWS:
        errors.append(f"❌ 데이터 부족: {len(df)}행 (최소 {MIN_ROWS}행 필요)")
    else:
        print(f"✅ 6. 최소 데이터 건수 충족: {len(df)} >= {MIN_ROWS}")
    
    # ========================================
    # 6. 위도/경도 범위 확인 (서울 범위)
    # ========================================
    if 'latitude' in df.columns and 'longitude' in df.columns:
        invalid_coords = (
            (df['latitude'] < 37.4) | (df['latitude'] > 37.7) |
            (df['longitude'] < 126.7) | (df['longitude'] > 127.2)
        ).sum()
        
        if invalid_coords > 0:
            warnings.append(f"⚠️ 서울 범위 밖 좌표: {invalid_coords}건")
        else:
            print("✅ 7. 좌표 범위 유효 (서울 내)")
    
    # ========================================
    # 결과 출력
    # ========================================
    print("\n" + "=" * 50)
    print(" 검증 결과")
    print("=" * 50)
    
    if warnings:
        print("\n⚠️ 경고:")
        for w in warnings:
            print(f"  {w}")
    
    if errors:
        print("\n❌ 오류:")
        for e in errors:
            print(f"  {e}")
        raise DataValidationError(f"데이터 검증 실패: {len(errors)}개 오류 발견")
    
    print("\n✅ 모든 검증 통과!")
    
    return {
        'status': 'success',
        'rows': len(df),
        'warnings': warnings,
        'errors': errors
    }


# 테스트
if __name__ == "__main__":
    # 테스트용 데이터
    test_df = pd.DataFrame({
        'station_id': ['ST-1', 'ST-2', 'ST-3'],
        'station_name': ['역1', '역2', '역3'],
        'bike_available': [5, 10, 3],
        'rack_total': [15, 20, 10],
        'latitude': [37.5, 37.51, 37.52],
        'longitude': [126.9, 126.91, 126.92]
    })
    
    result = validate_bike_data(test_df)
    print(f"\n결과: {result}")