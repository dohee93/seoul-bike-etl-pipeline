# 서울시 공공자전거 API 스키마

## 엔드포인트
- URL: `http://openapi.seoul.go.kr:8088/{KEY}/json/bikeList/{START}/{END}/`
- Method: GET

## 응답 필드

| 필드명 | 타입 | 설명 | 예시 |
|--------|------|------|------|
| stationId | string | 대여소 ID | "ST-4" |
| stationName | string | 대여소 이름 | "망원역 1번출구" |
| parkingBikeTotCnt | string | 주차된 자전거 수 | "3" |
| rackTotCnt | string | 총 거치대 수 | "15" |
| shared | string | 거치율(%) | "20" |
| stationLatitude | string | 위도 | "37.555" |
| stationLongitude | string | 경도 | "126.910" |