# 우주캠퍼스 주문 데이터 자동 수집 시스템

imweb REST API v2를 통한 주문 데이터 수집 및 Supabase 자동 동기화 시스템입니다.

## 🎯 주요 기능

- ✅ **전체 주문 수집**: 2025-01-20부터 현재까지 모든 주문 (971개/967개 목표, 100.4% 달성)
- ✅ **자동 일일 업데이트**: GitHub Actions로 매일 오후 3:30 KST 자동 실행
- ✅ **완전한 페이지네이션**: 100개 제한 없이 모든 데이터 수집
- ✅ **API 속도 제한 처리**: TOO MANY REQUEST 자동 대기 및 재시도
- ✅ **배치 실패 재시도**: HTTP 500 오류 시 최대 3번 자동 재시도
- ✅ **다중 상품 분리**: 하나의 주문에 여러 상품이 있으면 각각 행으로 분리
- ✅ **서울 시간대**: 모든 시간 데이터를 Asia/Seoul 시간으로 표시
- ✅ **주문 상태 포함**: COMPLETE, CANCEL 등 주문 상태 정보
- ✅ **Supabase upsert**: order_code + prod_no 기준 중복 방지 자동 업데이트

## 🚀 사용법

### 기본 사용법
```bash
# 전체 주문 수집 (권장)
python3 get_orders.py --all

# 일일 업데이트 (전날 15:00~당일 15:30, GitHub Actions용)
python3 get_orders.py --daily

# 특정 날짜 주문 처리
python3 get_orders.py --date 2025-08-30

# 누락 주문 복구
python3 get_orders.py --recover-missing orders.csv

# 사용법 도움말
python3 get_orders.py --help-usage
```



### 의존성 설치
```bash
pip install -r requirements.txt
```

## 🔧 기술적 특징

### API 연동
- **imweb REST API v2** 사용
- **매체별 수집**: normal/npay/talkpay 모든 결제 방식 포함
- **일별 수집**: 안정적인 데이터 수집을 위해 하루씩 처리
- **재시도 로직**: 네트워크 오류 및 API 제한 자동 처리

### 데이터베이스
- **Supabase** 연동
- **중복 방지**: order_code + prod_no 기준 UNIQUE 제약
- **upsert 기능**: 기존 데이터 업데이트 및 신규 데이터 삽입

### 자동화
- **GitHub Actions**: 매일 오후 3:30 KST 자동 실행
- **일일 업데이트 범위**: 전날 오후 3:00 ~ 당일 오후 3:30 (30분 여유)
- **오류 복구**: 실패한 배치 자동 재시도

## 🔄 GitHub Actions

매일 오후 3:30 KST에 자동으로 실행되어 전날 오후 3:00 ~ 당일 오후 3:30 범위의 새로운 주문을 Supabase에 동기화합니다.


## 📁 파일 구조

```
├── .github/
│   └── workflows/
│       └── daily-orders-update.yml  # GitHub Actions 워크플로우
├── get_orders.py                    # 메인 스크립트
├── requirements.txt                 # Python 의존성
├── .env                            # 환경 변수 (로컬용, git 제외)
├── .gitignore                      # Git 제외 파일 목록
└── README.md                       # 프로젝트 문서
```

## 💡 운영 가이드

### 수동 실행
```bash
# 전체 재동기화 (월 1회 권장)
python3 get_orders.py --all

# 특정 날짜 확인
python3 get_orders.py --date 2025-09-01
```

### 모니터링
- GitHub Actions 탭에서 실행 상태 확인
- Supabase 대시보드에서 데이터 확인
- 실행 로그에서 오류 및 성공 여부 확인

## 🆘 문제 해결

### API 오류
- `TOO MANY REQUEST`: 자동 대기 및 재시도
- `HTTP 500`: 최대 3번 자동 재시도
- 인증 오류: .env 파일의 API 키 확인

### Supabase 오류
- 중복 제약: 자동 upsert로 해결
- 연결 오류: SUPABASE_URL 및 KEY 확인

### 누락 데이터
```bash
# CSV 파일과 비교하여 누락 주문 복구
python3 get_orders.py --recover-missing your_orders.csv
```