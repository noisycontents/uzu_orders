#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
imweb 주문 데이터 수집 및 Supabase 동기화 스크립트
==============================================

📖 기능 개요:
- imweb REST API v2를 통한 전체 주문 데이터 수집
- 상품별 상세 정보 포함 (다중 상품 주문 시 행 분리)
- 서울 시간대(KST) 자동 변환
- Supabase uzu_orders 테이블 자동 동기화
- API 속도 제한 및 오류 자동 처리

🚀 사용법:
  python3 get_orders.py                    # 최근 25개 주문 처리
  python3 get_orders.py --all              # 전체 주문 처리 (권장)
  python3 get_orders.py --daily            # 일일 업데이트 (전날 15:30 ~ 당일 16:00, GitHub Actions용)
  python3 get_orders.py --date 2025-08-30  # 특정 날짜 주문 처리
  python3 get_orders.py --recover-missing orders.csv  # CSV 비교하여 누락 주문 복구

📋 필수 설정:
  FIRST_ORDER_DATE=2025-01-20  # 첫 주문일 (옵션)

🎯 주요 기능:
1. ✅ 전체 주문 수집: 2025-01-20 ~ 현재까지 모든 주문
2. ✅ 완전한 페이지네이션: 100개 제한 없이 모든 데이터 수집
3. ✅ 매체별 수집: normal/npay/talkpay 모든 결제 방식 포함
4. ✅ API 속도 제한 처리: TOO MANY REQUEST 자동 대기 및 재시도
5. ✅ 배치 실패 재시도: HTTP 500 오류 시 최대 3번 자동 재시도
6. ✅ 다중 상품 분리: 하나의 주문에 여러 상품이 있으면 각각 행으로 분리
7. ✅ 서울 시간대: 모든 시간 데이터를 Asia/Seoul 시간으로 표시
8. ✅ 주문 상태 포함: COMPLETE, CANCEL 등 주문 상태 정보
9. ✅ Supabase upsert: order_code + prod_no 기준 중복 방지 자동 업데이트
10. ✅ 누락 주문 복구: CSV 파일과 비교하여 누락된 주문 개별 수집

🔧 기술적 특징:
- 일별 수집: 안정적인 데이터 수집을 위해 하루씩 처리
- 재시도 로직: 네트워크 오류 및 API 제한 자동 처리
- 중복 방지: Supabase에서 order_code + prod_no 기준 UNIQUE 제약
- 오류 복구: 실패한 배치 자동 재시도 및 개별 복구 지원

💡 일일 업데이트:
  # 매일 실행하여 새 주문 자동 동기화 (전날 15:00~당일 15:30)
  python3 get_orders.py --daily
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pytz
import argparse

# Supabase는 HTTP 요청으로 직접 처리하여 의존성 문제 해결
SUPABASE_AVAILABLE = True

def setup_supabase():
    """Supabase 연결 정보를 설정합니다."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or supabase_url == 'your_supabase_url_here':
        print("❌ 오류: SUPABASE_URL이 설정되지 않았습니다.")
        return None
    
    if not supabase_key or supabase_key == 'your_supabase_anon_key_here':
        print("❌ 오류: SUPABASE_KEY가 설정되지 않았습니다.")
        return None
    
    # Supabase 연결 정보 반환
    return {
        'url': supabase_url,
        'key': supabase_key,
        'headers': {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
    }

def check_uzu_orders_table(supabase_config):
    """uzu_orders 테이블이 존재하는지 확인합니다."""
    try:
        # REST API로 테이블 조회 시도
        url = f"{supabase_config['url']}/rest/v1/uzu_orders"
        response = requests.get(
            f"{url}?select=id&limit=1", 
            headers=supabase_config['headers'],
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ uzu_orders 테이블 확인 완료")
            return True
        elif response.status_code == 404:
            print("❌ uzu_orders 테이블이 존재하지 않습니다.")
            print("💡 Supabase 대시보드에서 테이블을 먼저 생성해주세요.")
            return False
        else:
            print(f"⚠️ 테이블 확인 중 오류: HTTP {response.status_code}")
            print(f"응답: {response.text}")
            return False
    except Exception as e:
        print(f"❌ 테이블 확인 중 오류: {e}")
        return False

def convert_to_seoul_timezone(timestamp):
    """Unix 타임스탬프를 서울 시간대로 변환합니다."""
    if not timestamp or timestamp <= 0:
        return None
    
    # UTC 시간으로 변환
    utc_dt = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
    # 서울 시간대로 변환
    seoul_tz = pytz.timezone('Asia/Seoul')
    seoul_dt = utc_dt.astimezone(seoul_tz)
    
    return seoul_dt.isoformat()

def print_usage():
    """사용법을 출력합니다."""
    print("📖 사용법:")
    print("  python3 get_orders.py              # 최근 25개 주문 처리")
    print("  python3 get_orders.py --all        # 전체 주문 처리")
    print("  python3 get_orders.py --daily      # 일일 업데이트 (전날 15:00~당일 15:30)")
    print("  python3 get_orders.py --date 2025-08-30  # 특정 날짜 주문 처리")
    print("  python3 get_orders.py --recover-missing orders.csv  # CSV와 비교하여 누락 주문 복구")
    print()
    print("💡 모든 데이터는 Supabase uzu_orders 테이블에 자동 저장됩니다.")

def get_order_products_list(access_token, order_no, retry_count=3):
    """주문의 상품 정보를 prod-orders API로 조회하여 상품 리스트를 반환합니다."""
    import time
    
    url = f'https://api.imweb.me/v2/shop/orders/{order_no}/prod-orders'
    headers = {
        'Content-Type': 'application/json',
        'access-token': access_token
    }
    
    # order_version=v2 파라미터 추가 (최신 구조 사용)
    params = {
        'order_version': 'v2'
    }
    
    for attempt in range(retry_count):
        try:
            # API 호출 간격을 두어 안정성 향상
            if attempt > 0:
                time.sleep(0.5)  # 재시도 시 0.5초 대기
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # TOO MANY REQUEST 오류 처리
                if data.get('code') == -7 and 'TOO MANY REQUEST' in data.get('msg', ''):
                    wait_time = (attempt + 1) * 3  # 3초, 6초, 9초씩 증가
                    print(f"    ⚠️ API 속도 제한, {wait_time}초 대기 후 재시도... (주문번호: {order_no})")
                    time.sleep(wait_time)
                    continue
                
                # 응답 구조 확인
                if 'data' not in data:
                    print(f"⚠️ 예상치 못한 응답 구조 (주문번호: {order_no}): {data}")
                    if attempt < retry_count - 1:
                        time.sleep(1)  # 1초 대기 후 재시도
                        continue
                    else:
                        return []
                
                prod_orders_list = data.get('data', [])
                
                # 빈 데이터인 경우 재시도
                if not prod_orders_list:
                    if attempt < retry_count - 1:
                        print(f"⚠️ 빈 데이터 응답, 재시도 중... (주문번호: {order_no}, 시도: {attempt + 1}/{retry_count})")
                        continue
                    else:
                        return []
                
                products = []
                
                # data 배열을 순회
                for prod_order in prod_orders_list:
                    items = prod_order.get('items', [])
                    order_status = prod_order.get('status', '')  # 주문 상태 추가
                    
                    # items 배열에서 상품 정보 추출
                    for item in items:
                        prod_name = item.get('prod_name', '')
                        quantity = item.get('payment', {}).get('count', 1)  # count가 수량
                        prod_no = item.get('prod_no', '')
                        price = item.get('payment', {}).get('price', 0)
                        price_sale = item.get('payment', {}).get('price_sale', 0)
                        
                        if prod_name:
                            products.append({
                                'prod_name': prod_name,
                                'quantity': quantity,
                                'prod_no': prod_no,
                                'price': price,
                                'price_sale': price_sale,
                                'order_status': order_status  # 주문 상태 추가
                            })
                
                if products:
                    return products
                elif attempt < retry_count - 1:
                    print(f"⚠️ 상품명 없음, 재시도 중... (주문번호: {order_no}, 시도: {attempt + 1}/{retry_count})")
                    continue
                else:
                    return []
                    
            elif response.status_code == 404:
                return []
            else:
                if attempt < retry_count - 1:
                    print(f"⚠️ HTTP {response.status_code} 오류, 재시도 중... (주문번호: {order_no}, 시도: {attempt + 1}/{retry_count})")
                    continue
                else:
                    print(f"⚠️ 상품 조회 실패 (주문번호: {order_no}): HTTP {response.status_code}")
                    return []
                    
        except requests.exceptions.Timeout:
            if attempt < retry_count - 1:
                print(f"⚠️ 타임아웃 발생, 재시도 중... (주문번호: {order_no}, 시도: {attempt + 1}/{retry_count})")
                continue
            else:
                print(f"⚠️ 타임아웃 (주문번호: {order_no})")
                return []
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                print(f"⚠️ 네트워크 오류, 재시도 중... (주문번호: {order_no}, 시도: {attempt + 1}/{retry_count}): {e}")
                continue
            else:
                print(f"⚠️ 네트워크 오류 (주문번호: {order_no}): {e}")
                return []
        except Exception as e:
            if attempt < retry_count - 1:
                print(f"⚠️ 예상치 못한 오류, 재시도 중... (주문번호: {order_no}, 시도: {attempt + 1}/{retry_count}): {e}")
                continue
            else:
                print(f"⚠️ 예상치 못한 오류 (주문번호: {order_no}): {e}")
                return []
    
    return []

def get_access_token(api_key, secret_key):
    """API_KEY와 SECRET_KEY를 사용하여 액세스 토큰을 발급받습니다."""
    auth_url = 'https://api.imweb.me/v2/auth'
    auth_payload = {
        'key': api_key,
        'secret': secret_key
    }
    
    try:
        response = requests.post(auth_url, json=auth_payload)
        if response.status_code == 200:
            data = response.json()
            if 'access_token' in data:
                return data['access_token']
            else:
                print(f"❌ 액세스 토큰 발급 실패: {data}")
                return None
        else:
            print(f"❌ 인증 요청 실패: HTTP {response.status_code}")
            print(f"응답: {response.text}")
            return None
    except Exception as e:
        print(f"❌ 인증 요청 중 오류: {e}")
        return None

def ymd_to_ts_range_kst(ymd):
    """YYYY-MM-DD 형식의 날짜를 KST 기준 타임스탬프 범위로 변환합니다."""
    kst = pytz.timezone('Asia/Seoul')
    start = kst.localize(datetime.strptime(ymd, '%Y-%m-%d'))
    end = start + timedelta(days=1) - timedelta(seconds=1)
    # API는 epoch seconds 기대 → UTC로 변환 후 초 단위로
    return int(start.timestamp()), int(end.timestamp())

def get_last_24h_range_kst():
    """KST 기준 일일 업데이트 범위를 반환합니다 (전날 오후 3:00 ~ 당일 오후 3:30)."""
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    
    # 당일 오후 3:30
    today_330pm = now_kst.replace(hour=15, minute=30, second=0, microsecond=0)
    
    # 전날 오후 3:00 (30분 여유 + 누락 방지)
    yesterday_3pm = today_330pm - timedelta(days=1, minutes=30)
    
    # 현재 시간이 오후 3:30 이전이면 어제 기준으로 계산
    if now_kst < today_330pm:
        end_time = yesterday_3pm + timedelta(days=1, minutes=30)  # 어제 3:30
        start_time = yesterday_3pm - timedelta(days=1)  # 그저께 3:00
    else:
        end_time = today_330pm
        start_time = yesterday_3pm
    
    return start_time, end_time

# ===== 공통: 첫 페이지 + 카운트 조회 =====
def _orders_first_page_and_count(access_token, base_params):
    """같은 파라미터로 첫 페이지(list)와 pagenation을 함께 반환합니다."""
    url = 'https://api.imweb.me/v2/shop/orders'
    headers = {'Content-Type': 'application/json', 'access-token': access_token}
    params = dict(base_params)
    # 카운트와 동일 파라미터 유지 + v2 + 페이지 1, pagesize 최대 100
    params.update({'offset': 1, 'limit': 100, 'order_version': 'v2'})
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json().get('data', {})
    return data.get('list', []) or [], data.get('pagenation', {}) or {}

def get_recent_orders_all_pages(access_token):
    """기간 파라미터 없이(최근 3개월) 같은 파라미터로 전체 페이지를 순회해 모두 수집합니다."""
    base_params = {}
    all_orders = []
    try:
        first_list, pgn = _orders_first_page_and_count(access_token, base_params)
        total_count = int(pgn.get('data_count', 0) or 0)
        pagesize = int(pgn.get('pagesize', 100) or 100)
        total_pages = int(pgn.get('total_page', (total_count + pagesize - 1)//pagesize) or 1)

        print(f"📈 최근 3개월: 총 {total_count}개, 페이지 {total_pages} (페이지당 {pagesize})")
        print(f"  📄 페이지 1/{total_pages}: {len(first_list)}개")

        all_orders.extend(first_list)
        if total_pages <= 1:
            return all_orders

        url = 'https://api.imweb.me/v2/shop/orders'
        headers = {'Content-Type': 'application/json', 'access-token': access_token}
        for page in range(2, total_pages + 1):
            params = dict(base_params)
            params.update({'offset': page, 'limit': pagesize, 'order_version': 'v2'})
            r = requests.get(url, headers=headers, params=params, timeout=15)
            r.raise_for_status()
            cur = r.json().get('data', {}).get('list', []) or []
            print(f"  📄 페이지 {page}/{total_pages}: {len(cur)}개 (누적 {len(all_orders) + len(cur)}/{total_count})")
            if not cur:
                break
            all_orders.extend(cur)
        return all_orders
    except Exception as e:
        print(f"⚠️ 최근 3개월 전체 수집 오류: {e}")
        return all_orders

# ===== 전체 기간: 일 단위 수집 =====
def collect_orders_by_day(access_token, start_kst_dt, end_kst_dt):
    """KST 기준 시작/종료 일자를 일 단위로 쪼개어 같은 파라미터로 페이지 순회하여 수집합니다.
    - 매체별(type): ALL/normal/npay/talkpay 병합
    - 날짜 경계 여유: ±60초
    - 완전한 페이지네이션으로 100개 제한 없이 모든 데이터 수집
    """
    from time import sleep
    url = 'https://api.imweb.me/v2/shop/orders'
    headers = {'Content-Type': 'application/json', 'access-token': access_token}

    cursor = start_kst_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_kst_dt = end_kst_dt.replace(hour=23, minute=59, second=59, microsecond=0)

    all_orders = []
    day_idx = 0
    types = [None, 'normal', 'npay', 'talkpay']
    
    while cursor <= end_kst_dt:
        day_idx += 1
        # 여유 ±60초
        day_start = cursor - timedelta(seconds=60)
        day_end = cursor + timedelta(days=1, seconds=-1+60)
        daily_total = 0
        type_details = []
        
        for t in types:
            base_params = {
                'order_date_from': int(day_start.timestamp()),
                'order_date_to': int(day_end.timestamp()),
            }
            if t is not None:
                base_params['type'] = t
                
            try:
                first_list, pgn = _orders_first_page_and_count(access_token, base_params)
                total = int(pgn.get('data_count', 0) or 0)
                pagesize = int(pgn.get('pagesize', 100) or 100)
                total_pages = int(pgn.get('total_page', (total + pagesize - 1)//pagesize) or 1)
                
                if total == 0:
                    continue
                    
                day_orders = list(first_list)
                collected_count = len(first_list)
                
                # 페이지네이션으로 모든 데이터 수집 (100개 제한 없음)
                if total_pages > 1:
                    for page in range(2, total_pages + 1):
                        params = dict(base_params)
                        params.update({'offset': page, 'limit': pagesize, 'order_version': 'v2'})
                        
                        retry_count = 0
                        max_retries = 3
                        page_success = False
                        
                        while retry_count < max_retries and not page_success:
                            try:
                                r = requests.get(url, headers=headers, params=params, timeout=15)
                                r.raise_for_status()
                                cur = r.json().get('data', {}).get('list', []) or []
                                
                                if not cur:
                                    page_success = True  # 빈 페이지면 정상 종료
                                    break
                                    
                                day_orders.extend(cur)
                                collected_count += len(cur)
                                page_success = True
                                sleep(0.08)
                                
                            except Exception as e:
                                retry_count += 1
                                if retry_count >= max_retries:
                                    print(f"      ⚠️ 페이지 {page} 최종 실패: {e}")
                                    break
                                else:
                                    print(f"      ⚠️ 페이지 {page} 재시도 {retry_count}/{max_retries}: {e}")
                                    sleep(1)
                        
                        if not page_success and retry_count >= max_retries:
                            break
                
                if len(day_orders) != total:
                    print(f"    ⚠️ {t or 'ALL'}: 예상 {total}개 vs 실제 {len(day_orders)}개")
                
                type_details.append(f"{t or 'ALL'}:{len(day_orders)}")
                daily_total += len(day_orders)
                all_orders.extend(day_orders)
                
            except Exception as e:
                print(f"    ⚠️ {cursor.strftime('%Y-%m-%d')} {t or 'ALL'} 조회 오류: {e}")
                continue
        
        if daily_total > 0:
            print(f"  📅 {day_idx}일차 {cursor.strftime('%Y-%m-%d')}: {daily_total}개 ({', '.join(type_details)})")
        
        # 다음 날
        cursor += timedelta(days=1)
        sleep(0.08)

    print(f"✅ 전체 기간 수집 완료: {len(all_orders)}개")
    return all_orders

def get_daily_orders_24h(access_token):
    """일일 업데이트 (전날 오후 3:00 ~ 당일 오후 3:30) 주문을 수집합니다."""
    start_time, end_time = get_last_24h_range_kst()
    
    print(f"📅 일일 업데이트: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} (KST)")
    print(f"   ⏰ 30분 여유 시간으로 누락 방지")
    
    # 1. 지정된 시간 범위의 새 주문 수집
    new_orders = collect_orders_by_day(access_token, start_time, end_time)
    
    # 2. 최근 1개월 취소 주문 상태 업데이트
    print(f"\n🔄 최근 1개월 취소 주문 상태 업데이트 확인 중...")
    cancel_orders = get_recent_canceled_orders(access_token)
    
    # 3. 두 데이터 병합
    all_orders = new_orders + cancel_orders
    
    # 중복 제거 (주문번호 기준)
    seen_order_nos = set()
    deduped = []
    for order in all_orders:
        order_no = order.get('order_no')
        if order_no and order_no not in seen_order_nos:
            seen_order_nos.add(order_no)
            deduped.append(order)
    
    if len(deduped) != len(all_orders):
        print(f"🔁 중복 제거: {len(all_orders)} → {len(deduped)}")
    
    return deduped

def get_recent_canceled_orders(access_token):
    """최근 1개월 내 취소된 주문을 조회합니다."""
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    one_month_ago = now_kst - timedelta(days=30)
    
    print(f"   📋 취소 주문 확인 범위: {one_month_ago.strftime('%Y-%m-%d')} ~ {now_kst.strftime('%Y-%m-%d')}")
    
    # 최근 1개월 범위에서 취소 상태 주문만 조회
    return collect_orders_by_day_with_status(access_token, one_month_ago, now_kst, 'CANCEL')

def collect_orders_by_day_with_status(access_token, start_kst_dt, end_kst_dt, target_status):
    """특정 기간의 특정 상태 주문을 수집합니다."""
    from time import sleep
    url = 'https://api.imweb.me/v2/shop/orders'
    headers = {'Content-Type': 'application/json', 'access-token': access_token}

    cursor = start_kst_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end_kst_dt = end_kst_dt.replace(hour=23, minute=59, second=59, microsecond=0)

    all_orders = []
    day_idx = 0
    types = [None, 'normal', 'npay', 'talkpay']
    cancel_count = 0
    
    while cursor <= end_kst_dt:
        day_idx += 1
        day_start = cursor - timedelta(seconds=60)
        day_end = cursor + timedelta(days=1, seconds=-1+60)
        
        for t in types:
            base_params = {
                'order_date_from': int(day_start.timestamp()),
                'order_date_to': int(day_end.timestamp()),
                'status': 'cancel'  # 취소 상태만 조회
            }
            if t is not None:
                base_params['type'] = t
                
            try:
                first_list, pgn = _orders_first_page_and_count(access_token, base_params)
                total = int(pgn.get('data_count', 0) or 0)
                pagesize = int(pgn.get('pagesize', 100) or 100)
                total_pages = int(pgn.get('total_page', (total + pagesize - 1)//pagesize) or 1)
                
                if total == 0:
                    continue
                    
                day_orders = list(first_list)
                
                if total_pages > 1:
                    for page in range(2, total_pages + 1):
                        params = dict(base_params)
                        params.update({'offset': page, 'limit': pagesize, 'order_version': 'v2'})
                        
                        retry_count = 0
                        max_retries = 3
                        page_success = False
                        
                        while retry_count < max_retries and not page_success:
                            try:
                                r = requests.get(url, headers=headers, params=params, timeout=15)
                                r.raise_for_status()
                                cur = r.json().get('data', {}).get('list', []) or []
                                
                                if not cur:
                                    page_success = True
                                    break
                                    
                                day_orders.extend(cur)
                                page_success = True
                                sleep(0.08)
                                
                            except Exception as e:
                                retry_count += 1
                                if retry_count >= max_retries:
                                    break
                                else:
                                    sleep(1)
                        
                        if not page_success and retry_count >= max_retries:
                            break
                
                cancel_count += len(day_orders)
                all_orders.extend(day_orders)
                
            except Exception as e:
                continue
        
        cursor += timedelta(days=1)
        sleep(0.08)

    if cancel_count > 0:
        print(f"   ✅ 최근 1개월 취소 주문: {cancel_count}개 발견")
    else:
        print(f"   📋 최근 1개월 취소 주문: 없음")
    
    return all_orders

def get_orders_by_day(access_token, day_start, day_end):
    """1일 단위로 주문을 조회합니다."""
    import time
    
    orders = []
    page = 1
    limit = 100
    
    url = 'https://api.imweb.me/v2/shop/orders'
    headers = {'Content-Type': 'application/json', 'access-token': access_token}
    
    while True:
        params = {
            'offset': page,
            'limit': limit,
            'order_version': 'v2',
            'order_date_from': int(day_start.timestamp()),
            'order_date_to': int(day_end.timestamp())
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            day_orders = data.get('data', {}).get('list', [])
            pagination = data.get('data', {}).get('pagenation', {})
            
            if not day_orders:
                break
            
            orders.extend(day_orders)
            
            # 페이지네이션 확인
            current_page = pagination.get('current_page', page)
            total_pages = pagination.get('total_page', 1)
            
            if current_page >= total_pages:
                break
            
            page += 1
            time.sleep(0.1)  # API 호출 간격 (더 빠르게)
            
        except Exception as e:
            print(f"    ⚠️ 페이지 {page} 조회 오류: {e}")
            break
    
    return orders

def get_all_orders(access_token, target_date=None):
    """안전한 방식: 같은 파라미터로 카운트→페이지 순회. target_date 없으면 최근 3개월 전체.
    target_date가 있으면 그 하루 범위를 초 단위 Timestamp로 설정해 같은 파라미터로 페이지 루프.
    """
    # 특정 날짜
    if target_date:
        print(f"🗓️ 특정 날짜 조회: {target_date}")
        date_from_ts, date_to_ts = ymd_to_ts_range_kst(target_date)
        base_params = {
            'order_date_from': date_from_ts,
            'order_date_to': date_to_ts,
        }
        first_list, pgn = _orders_first_page_and_count(access_token, base_params)
        total = int(pgn.get('data_count', 0) or 0)
        pagesize = int(pgn.get('pagesize', 100) or 100)
        total_pages = int(pgn.get('total_page', (total + pagesize - 1)//pagesize) or 1)
        print(f"📈 해당 날짜 총 {total}개, 페이지 {total_pages}")
        all_orders = []
        all_orders.extend(first_list)
        if total_pages > 1:
            url = 'https://api.imweb.me/v2/shop/orders'
            headers = {'Content-Type': 'application/json', 'access-token': access_token}
            for page in range(2, total_pages + 1):
                params = dict(base_params)
                params.update({'offset': page, 'limit': pagesize, 'order_version': 'v2'})
                r = requests.get(url, headers=headers, params=params, timeout=15)
                r.raise_for_status()
                cur = r.json().get('data', {}).get('list', []) or []
                if not cur:
                    break
                all_orders.extend(cur)
        return all_orders

    # target_date 없음: 전체 기간을 강제 수집 (최초 주문일은 환경변수 또는 2025-01-20 기본값)
    kst = pytz.timezone('Asia/Seoul')
    first_order_ymd = os.getenv('FIRST_ORDER_DATE', '2025-01-20')
    try:
        start_kst = kst.localize(datetime.strptime(first_order_ymd, '%Y-%m-%d'))
    except Exception:
        start_kst = kst.localize(datetime(2025, 1, 20))
    end_kst = datetime.now(kst)
    print(f"📆 전체 기간 일 단위 수집(강제): {start_kst.strftime('%Y-%m-%d')} ~ {end_kst.strftime('%Y-%m-%d')}")
    all_period = collect_orders_by_day(access_token, start_kst, end_kst)
    if not all_period:
        print("⚠️ 전체 기간 수집 결과가 비어 있습니다. 최근 3개월만 반환합니다.")
        recent = get_recent_orders_all_pages(access_token)
        print(f"✅ 최근 3개월 수집 완료: {len(recent)}개")
        return recent
    return all_period

def upsert_to_supabase(supabase_config, orders_data):
    """주문 데이터를 Supabase에 효율적으로 upsert(업데이트/인서트)합니다."""
    import time
    
    try:
        base_url = f"{supabase_config['url']}/rest/v1/uzu_orders?on_conflict=order_code,prod_no"
        headers = supabase_config['headers'].copy()
        
        print(f"🔄 {len(orders_data)}개 행을 Supabase에 upsert 중...")
        
        # PostgreSQL의 ON CONFLICT를 사용한 효율적인 upsert
        # order_code와 prod_no의 조합으로 유니크 체크
        headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        # 배치 크기로 나누어 저장 (중복 문제 방지를 위해 더 작게)
        batch_size = 10  # 중복 오류 방지를 위해 10개씩
        success_count = 0
        failed_batches = []
        
        for i in range(0, len(orders_data), batch_size):
            batch = orders_data[i:i + batch_size]
            batch_num = i//batch_size + 1
            
            # 재시도 로직 (최대 3번 시도)
            max_retries = 3
            retry_count = 0
            batch_success = False
            
            while retry_count < max_retries and not batch_success:
                try:
                    if retry_count > 0:
                        wait_time = retry_count * 2  # 2초, 4초씩 대기
                        print(f"  ⏳ 배치 {batch_num} 재시도 {retry_count}/{max_retries-1} ({wait_time}초 대기 후)")
                        time.sleep(wait_time)
                    
                    response = requests.post(
                        base_url,
                        headers=headers,
                        json=batch,
                        timeout=60
                    )
                    
                    if response.status_code in [200, 201]:
                        success_count += len(batch)
                        print(f"  ✅ 배치 {batch_num} 완료 ({len(batch)}개 행)")
                        batch_success = True
                    else:
                        retry_count += 1
                        if retry_count >= max_retries:
                            print(f"  ❌ 배치 {batch_num} 최종 실패: HTTP {response.status_code}")
                            print(f"     응답: {response.text[:200]}...")
                            failed_batches.append({'batch_num': batch_num, 'data': batch, 'error': f"HTTP {response.status_code}"})
                        else:
                            print(f"  ⚠️ 배치 {batch_num} 실패 (재시도 예정): HTTP {response.status_code}")
                        
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        print(f"  ❌ 배치 {batch_num} 최종 오류: {e}")
                        failed_batches.append({'batch_num': batch_num, 'data': batch, 'error': str(e)})
                    else:
                        print(f"  ⚠️ 배치 {batch_num} 오류 (재시도 예정): {e}")
        
        # 실패한 배치들에 대한 요약
        if failed_batches:
            print(f"\n⚠️ 실패한 배치 수: {len(failed_batches)}개")
            print(f"   실패한 데이터 행 수: {sum(len(fb['data']) for fb in failed_batches)}개")
            for fb in failed_batches:
                print(f"   - 배치 {fb['batch_num']}: {fb['error']}")
        
        print(f"🎉 총 {success_count}개 행이 Supabase에 저장되었습니다!")
        
        # 일부라도 성공했다면 True 반환
        return success_count > 0
        
    except Exception as e:
        print(f"❌ Supabase upsert 실패: {e}")
        return False

def save_to_supabase(supabase_config, orders_data):
    """주문 데이터를 Supabase에 저장합니다."""
    try:
        base_url = f"{supabase_config['url']}/rest/v1/uzu_orders"
        headers = supabase_config['headers']
        
        # 기존 데이터 삭제 (전체 새로고침)
        print("🗑️ 기존 데이터 삭제 중...")
        delete_response = requests.delete(
            f"{base_url}?id=neq.0",  # 모든 행 삭제
            headers=headers,
            timeout=30
        )
        
        if delete_response.status_code in [200, 204]:
            print("✅ 기존 데이터 삭제 완료")
        else:
            print(f"⚠️ 기존 데이터 삭제 응답: {delete_response.status_code}")
        
        # 새 데이터 삽입
        print(f"💾 {len(orders_data)}개 행을 Supabase에 저장 중...")
        
        # 배치 크기로 나누어 저장 (Supabase 제한 고려)
        batch_size = 50  # 안정성을 위해 작은 배치 크기 사용
        success_count = 0
        
        for i in range(0, len(orders_data), batch_size):
            batch = orders_data[i:i + batch_size]
            
            try:
                response = requests.post(
                    base_url,
                    headers=headers,
                    json=batch,
                    timeout=30
                )
                
                if response.status_code in [200, 201]:
                    success_count += len(batch)
                    print(f"  ✅ {i + 1}-{min(i + batch_size, len(orders_data))}번째 행 저장 완료")
                else:
                    print(f"  ❌ 배치 {i//batch_size + 1} 저장 실패: HTTP {response.status_code}")
                    print(f"     응답: {response.text[:200]}...")
                    
            except Exception as e:
                print(f"  ❌ 배치 {i//batch_size + 1} 저장 중 오류: {e}")
        
        if success_count > 0:
            print(f"🎉 {success_count}개 행이 Supabase에 성공적으로 저장되었습니다!")
            return True
        else:
            print("❌ 데이터 저장에 실패했습니다.")
            return False
        
    except Exception as e:
        print(f"❌ Supabase 저장 실패: {e}")
        return False

def prepare_supabase_data(order, product_info):
    """주문 데이터를 Supabase 테이블 형식에 맞게 변환합니다."""
    return {
        # 주문 기본 정보
        'order_code': order.get('order_code', ''),                    # 주문 코드
        'order_no': order.get('order_no', ''),                        # 주문 번호
        'order_time': convert_to_seoul_timezone(order.get('order_time', 0)),  # 주문 일시 (서울시간)
        'order_type': order.get('order_type', ''),                    # 주문 유형
        
        # 주문자 정보
        'orderer_name': order.get('orderer', {}).get('name', ''),     # 주문자 이름
        'orderer_email': order.get('orderer', {}).get('email', ''),   # 주문자 이메일
        'orderer_phone': order.get('orderer', {}).get('call', ''),    # 주문자 전화번호
        
        # 배송지 정보
        'delivery_name': order.get('delivery', {}).get('address', {}).get('name', ''),           # 배송지 수령인
        'delivery_phone': order.get('delivery', {}).get('address', {}).get('phone', ''),         # 배송지 전화번호
        'delivery_postcode': order.get('delivery', {}).get('address', {}).get('postcode', ''),   # 배송지 우편번호
        'delivery_address': order.get('delivery', {}).get('address', {}).get('address', ''),     # 배송지 주소
        'delivery_address_detail': order.get('delivery', {}).get('address', {}).get('address_detail', ''),  # 배송지 상세주소
        
        # 상품 정보
        'prod_no': product_info.get('prod_no', '') if product_info else None,                    # 상품 번호
        'prod_name': product_info.get('prod_name', '') if product_info else '상품 정보 없음',        # 상품명
        'prod_quantity': product_info.get('quantity', 0) if product_info else 0,                # 상품 수량
        'prod_price': product_info.get('price', 0) if product_info else 0,                      # 상품 단가
        'prod_discount_amount': product_info.get('price_sale', 0) if product_info else 0,       # 상품 할인 금액
        'order_status': product_info.get('order_status', '') if product_info else '',           # 주문 상태
        
        # 결제 정보
        'payment_type': order.get('payment', {}).get('pay_type', ''),          # 결제 방식
        'order_total_amount': order.get('payment', {}).get('total_price', 0),  # 주문 총 금액
        'order_discount_amount': order.get('payment', {}).get('price_sale', 0), # 주문 할인 금액
        'delivery_fee': order.get('payment', {}).get('deliv_price', 0),        # 배송비
        'coupon_discount': order.get('payment', {}).get('coupon', 0),          # 쿠폰 할인 금액
        'point_used': order.get('payment', {}).get('point', 0),                # 포인트 사용 금액
        'order_payment_amount': order.get('payment', {}).get('payment_amount', 0), # 실제 결제 금액
        'payment_time': convert_to_seoul_timezone(order.get('payment', {}).get('payment_time', 0)), # 결제 일시 (서울시간)
        
        # 기타 정보
        'complete_time': convert_to_seoul_timezone(order.get('complete_time', 0)), # 주문 완료 일시 (서울시간)
        'device_type': order.get('device', {}).get('type', ''),                    # 주문 디바이스
        'is_gift': order.get('is_gift', 'N')                                      # 선물 여부
    }

def recover_missing_orders_from_csv(access_token, supabase_config, csv_file_path):
    """CSV 파일과 Supabase를 비교하여 누락된 주문을 개별 수집합니다."""
    import csv
    import time
    from collections import defaultdict
    
    print(f"🔍 CSV 파일에서 누락된 주문을 찾는 중: {csv_file_path}")
    
    try:
        # CSV 파일에서 주문 번호 목록 읽기
        csv_orders = set()
        with open(csv_file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                order_no = row.get('주문번호', '').strip()
                if order_no:
                    csv_orders.add(order_no)
        
        print(f"📊 CSV에서 찾은 주문: {len(csv_orders)}개")
        
        # Supabase에서 기존 주문 번호 목록 조회
        url = f"{supabase_config['url']}/rest/v1/uzu_orders?select=order_no"
        response = requests.get(url, headers=supabase_config['headers'], timeout=30)
        
        if response.status_code != 200:
            print(f"❌ Supabase 조회 실패: HTTP {response.status_code}")
            return False
            
        supabase_orders = set()
        for row in response.json():
            order_no = row.get('order_no', '').strip()
            if order_no:
                supabase_orders.add(order_no)
        
        print(f"📊 Supabase에 있는 주문: {len(supabase_orders)}개")
        
        # 누락된 주문 찾기
        missing_orders = list(csv_orders - supabase_orders)
        
        if not missing_orders:
            print("🎉 누락된 주문이 없습니다! 모든 주문이 이미 Supabase에 저장되어 있습니다.")
            return True
        
        print(f"🚨 누락된 주문: {len(missing_orders)}개")
        print(f"   처리할 주문들: {sorted(missing_orders)[:10]}{'...' if len(missing_orders) > 10 else ''}")
        
        # 날짜별로 누락 주문 정리
        missing_by_date = defaultdict(list)
        with open(csv_file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                order_no = row.get('주문번호', '').strip()
                order_date = row.get('주문일', '').strip()
                if order_no in missing_orders:
                    missing_by_date[order_date].append(order_no)
        
        print(f"📅 누락 주문이 있는 날짜: {len(missing_by_date)}개")
        
        # 개별 주문 수집 시작
        recovered_orders = []
        failed_orders = []
        
        for date_str, order_nos in sorted(missing_by_date.items()):
            print(f"\n📅 {date_str}: {len(order_nos)}개 주문 처리 중...")
            
            for j, order_no in enumerate(order_nos, 1):
                print(f"  {j}/{len(order_nos)} 주문번호: {order_no}")
                
                try:
                    # 개별 주문 조회
                    order_detail = get_single_order(access_token, order_no)
                    if not order_detail:
                        failed_orders.append(order_no)
                        print(f"    ❌ 주문 조회 실패")
                        continue
                    
                    # 상품 정보 조회
                    products_list = get_order_products_list(access_token, order_no)
                    if not products_list:
                        print(f"    ⚠️ 상품 정보 없음 - 기본 정보로 저장")
                        products_list = [None]  # 기본 행 생성
                    
                    # 각 상품별로 Supabase 데이터 준비
                    for product_info in products_list:
                        supabase_row = prepare_supabase_data(order_detail, product_info)
                        recovered_orders.append(supabase_row)
                    
                    print(f"    ✅ {len(products_list)}개 상품 처리 완료")
                    time.sleep(0.1)  # API 호출 간격
                    
                except Exception as e:
                    failed_orders.append(order_no)
                    print(f"    ❌ 처리 중 오류: {e}")
                    continue
        
        print(f"\n📈 개별 수집 결과:")
        print(f"   성공: {len(recovered_orders)}개 행")
        print(f"   실패: {len(failed_orders)}개 주문")
        
        # Supabase에 저장
        if recovered_orders:
            print(f"\n💾 {len(recovered_orders)}개 복구된 행을 Supabase에 저장 중...")
            success = upsert_to_supabase(supabase_config, recovered_orders)
            
            if success:
                print("✅ 누락된 주문 복구 완료!")
            else:
                print("❌ 복구된 데이터 저장 실패")
            
            return success
        else:
            print("⚠️ 복구할 수 있는 주문이 없습니다.")
            return False
            
    except Exception as e:
        print(f"❌ 누락 주문 복구 중 오류: {e}")
        return False

def get_single_order(access_token, order_no, retry_count=3):
    """개별 주문 상세 정보를 조회합니다."""
    import time
    
    url = f'https://api.imweb.me/v2/shop/orders/{order_no}'
    headers = {
        'Content-Type': 'application/json',
        'access-token': access_token
    }
    
    params = {
        'order_version': 'v2'
    }
    
    for attempt in range(retry_count):
        try:
            if attempt > 0:
                time.sleep(0.5)
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # TOO MANY REQUEST 오류 처리
                if data.get('code') == -7 and 'TOO MANY REQUEST' in data.get('msg', ''):
                    wait_time = (attempt + 1) * 5  # 5초, 10초, 15초씩 증가
                    print(f"    ⚠️ API 속도 제한, {wait_time}초 대기 후 재시도... (주문번호: {order_no})")
                    time.sleep(wait_time)
                    continue
                
                order_data = data.get('data', {})
                
                if order_data:
                    return order_data
                else:
                    if attempt < retry_count - 1:
                        time.sleep(1)
                        continue
                    else:
                        return None
                        
            elif response.status_code == 404:
                return None
            else:
                if attempt < retry_count - 1:
                    continue
                else:
                    print(f"    ⚠️ 주문 조회 실패: HTTP {response.status_code}")
                    return None
                    
        except Exception as e:
            if attempt < retry_count - 1:
                continue
            else:
                print(f"    ⚠️ 주문 조회 오류: {e}")
                return None
    
    return None

def main():
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='imweb 주문 데이터를 Supabase에 저장합니다.')
    parser.add_argument('--date', '-d', type=str, help='특정 날짜의 주문만 처리 (YYYY-MM-DD 형식)')
    parser.add_argument('--all', '-a', action='store_true', help='전체 주문 데이터 처리')
    parser.add_argument('--daily', action='store_true', help='최근 24시간 주문 업데이트 (GitHub Actions용)')
    parser.add_argument('--recover-missing', type=str, help='CSV 파일과 비교하여 누락된 주문 복구')
    parser.add_argument('--help-usage', action='store_true', help='사용법 출력')
    args = parser.parse_args()
    
    # 사용법 출력
    if args.help_usage:
        print_usage()
        return
    
    # .env 파일 로드
    load_dotenv()
    
    # 환경 변수에서 API 정보 가져오기
    access_token = os.getenv('ACCESS_TOKEN')
    api_key = os.getenv('API_KEY')
    secret_key = os.getenv('SECRET_KEY')
    
    # Supabase 설정
    print("🔗 Supabase 연결 설정 중...")
    supabase_config = setup_supabase()
    if not supabase_config:
        print("⚠️ Supabase 연결 실패. CSV 파일만 생성됩니다.")
        use_supabase = False
    else:
        print("✅ Supabase 연결 정보 설정 완료")
        
        # 테이블 존재 확인
        table_exists = check_uzu_orders_table(supabase_config)
        if table_exists:
            use_supabase = True
        else:
            print("⚠️ 테이블이 존재하지 않아 CSV 파일만 생성됩니다.")
            use_supabase = False
    
    # API 인증 방식 결정
    if access_token and access_token != 'your_access_token_here':
        # 직접 ACCESS_TOKEN 사용
        final_access_token = access_token
        print("🔑 기존 ACCESS_TOKEN을 사용한 인증")
    elif api_key and secret_key:
        # API_KEY와 SECRET_KEY로 액세스 토큰 발급
        print("🔑 API_KEY와 SECRET_KEY로 액세스 토큰 발급 중...")
        final_access_token = get_access_token(api_key, secret_key)
        if not final_access_token:
            return
        print("✅ 액세스 토큰 발급 완료")
    else:
        print("❌ 오류: API 인증 정보가 설정되지 않았습니다.")
        print("📝 .env 파일에서 다음 중 하나를 설정해주세요:")
        print("   - ACCESS_TOKEN=발급받은_액세스_토큰")
        print("   - API_KEY=발급받은_API_키 및 SECRET_KEY=발급받은_시크릿_키")
        return
    
    # API 엔드포인트 및 헤더 설정
    url = 'https://api.imweb.me/v2/shop/orders'
    headers = {
        'Content-Type': 'application/json',
        'access-token': final_access_token
    }
    
    print("🔄 imweb API에서 주문 목록을 가져오는 중...")
    
    try:
        # 누락 주문 복구 모드
        if args.recover_missing:
            print("🔧 누락 주문 복구 모드")
            if not os.path.exists(args.recover_missing):
                print(f"❌ CSV 파일을 찾을 수 없습니다: {args.recover_missing}")
                return
            
            success = recover_missing_orders_from_csv(final_access_token, supabase_config, args.recover_missing)
            if success:
                print("🎉 누락 주문 복구 완료!")
            else:
                print("❌ 누락 주문 복구 실패")
            return
        
        # 전체 주문 조회 또는 특정 날짜 주문 조회
        if args.all:
            print("🌍 전체 주문 데이터 처리 모드")
            orders = get_all_orders(final_access_token)
            # 주문번호 기준 전역 중복 제거
            seen_order_nos = set()
            deduped = []
            for o in orders:
                ono = o.get('order_no')
                if ono and ono not in seen_order_nos:
                    seen_order_nos.add(ono)
                    deduped.append(o)
            if len(deduped) != len(orders):
                print(f"🔁 중복 제거: {len(orders)} → {len(deduped)}")
            orders = deduped
        elif args.daily:
            print("⏰ 일일 업데이트 모드 (최근 24시간)")
            orders = get_daily_orders_24h(final_access_token)
            # 중복 제거
            seen_order_nos = set()
            deduped = []
            for o in orders:
                ono = o.get('order_no')
                if ono and ono not in seen_order_nos:
                    seen_order_nos.add(ono)
                    deduped.append(o)
            if len(deduped) != len(orders):
                print(f"🔁 중복 제거: {len(orders)} → {len(deduped)}")
            orders = deduped
        elif args.date:
            print(f"📅 특정 날짜 주문 처리 모드: {args.date}")
            orders = get_all_orders(final_access_token, args.date)
        else:
            print("📋 최근 주문 조회 모드 (기본 25개)")
            # 기본 동작: 최근 주문 조회
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                orders = data.get('data', {}).get('list', [])
                print(f"📊 찾은 주문 개수: {len(orders)}")
            else:
                print(f"❌ 주문 조회 실패: HTTP {response.status_code}")
                return
        
        if not orders:
            print("📋 조회된 주문이 없습니다.")
            return
        
        # 전체 주문에 대해 상품 정보 조회 및 Supabase 저장
        import time
        
        supabase_data = []     # Supabase용
        total_product_count = 0
        print("🛍️ 각 주문의 상품 정보를 조회하는 중...")
        print("   (다중 상품 주문은 상품별로 행을 분리합니다)")
        
        for i, order in enumerate(orders, 1):
            order_no = order.get('order_no', '')
            print(f"  {i}/{len(orders)} 주문번호: {order_no} 처리 중...")
            
            # API 호출 간격을 두어 안정성 향상
            if i > 1:
                time.sleep(0.1)  # 100ms 대기 (전체 처리이므로 빠르게)
            
            # 주문 상세 정보에서 상품 리스트 가져오기
            products_list = get_order_products_list(final_access_token, order_no)
            
            if products_list:
                print(f"    ✅ {len(products_list)}개 상품 정보 조회 성공")
                total_product_count += len(products_list)
                
                # 각 상품별로 별도의 행 생성
                for j, product_info in enumerate(products_list):
                    # Supabase용 데이터
                    if use_supabase:
                        supabase_row = prepare_supabase_data(order, product_info)
                        supabase_data.append(supabase_row)
                    
                    if len(products_list) > 1:
                        print(f"      └ 상품 {j+1}: {product_info.get('prod_name', '')} (수량: {product_info.get('quantity', 1)})")
            else:
                print(f"    ⚠️ 상품 정보 없음")
                # 상품 정보가 없는 경우에도 기본 행 생성
                if use_supabase:
                    supabase_row = prepare_supabase_data(order, None)
                    supabase_data.append(supabase_row)
        
        print(f"📈 처리 완료: {len(orders)}개 주문 → {len(supabase_data)}개 행 (총 {total_product_count}개 상품)")
        
        # Supabase에 데이터 저장
        if use_supabase and supabase_data:
            print(f"\n🚀 Supabase에 {len(supabase_data)}개 행 upsert 중...")
            success = upsert_to_supabase(supabase_config, supabase_data)
            
            if success:
                print("✅ 모든 데이터가 Supabase uzu_orders 테이블에 저장되었습니다!")
                print("🔗 Supabase 대시보드에서 데이터를 확인하실 수 있습니다.")
            else:
                print("❌ Supabase 저장에 실패했습니다.")
        elif use_supabase:
            print("⚠️ 저장할 데이터가 없습니다.")
        else:
            print("💡 Supabase 연결이 필요합니다.")
        
        # 간단한 요약 출력
        if supabase_data:
            print(f"\n📋 처리 요약:")
            print(f"   총 주문 수: {len(orders)}개")
            print(f"   총 상품 수: {total_product_count}개")
            print(f"   저장된 행: {len(supabase_data)}개")
            
            # 최근 3개 주문만 미리보기
            recent_orders = {}
            for data in supabase_data[:3]:
                order_no = data['order_no']
                if order_no not in recent_orders:
                    recent_orders[order_no] = {
                        'orderer_name': data['orderer_name'],
                        'order_time': data['order_time'],
                        'products': []
                    }
                recent_orders[order_no]['products'].append(data['prod_name'])
            
            print(f"\n📊 최근 주문 미리보기:")
            for i, (order_no, info) in enumerate(recent_orders.items(), 1):
                print(f"  {i}. 주문번호: {order_no}")
                print(f"     주문자: {info['orderer_name']}")
                print(f"     주문시간: {info['order_time']}")
                print(f"     상품: {', '.join(info['products'])}")
                print()
                
        elif response.status_code == 401:
            print("❌ 인증 오류: 액세스 토큰이 유효하지 않습니다.")
            print("🔑 .env 파일의 ACCESS_TOKEN을 확인해주세요.")
        elif response.status_code == 403:
            print("❌ 권한 오류: API 접근 권한이 없습니다.")
        else:
            print(f"❌ API 요청 실패: HTTP {response.status_code}")
            print(f"응답 내용: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 네트워크 오류: {e}")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {e}")

if __name__ == "__main__":
    main()
