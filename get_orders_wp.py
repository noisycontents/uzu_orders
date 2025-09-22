#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
워드프레스 WooCommerce 주문 데이터 수집 및 Supabase 동기화 스크립트
================================================================

📖 기능 개요:
- WooCommerce REST API를 통한 주문 데이터 수집
- 특정 상품 ID 기반 필터링
- Supabase uzu_orders 테이블 자동 동기화
- 일일 업데이트 지원 (get_orders.py의 --daily와 동일)

🚀 사용법:
  python3 get_orders_wp.py --test-product 237513    # 특정 상품 테스트
  python3 get_orders_wp.py --daily                  # 일일 업데이트 (get_orders.py와 동일한 시간대)
  python3 get_orders_wp.py --date 2025-09-08        # 특정 날짜 주문 처리

💡 설정:
  DOK_WP_WOO_Consumer_KEY=your_consumer_key
  DOK_WP_WOO_Consumer_SECRET=your_consumer_secret
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pytz
import argparse
import base64
import urllib3

# SSL 경고 비활성화 (개발/테스트용)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_woocommerce_auth():
    """WooCommerce API 인증 정보를 설정합니다."""
    consumer_key = os.getenv('DOK_WP_WOO_Consumer_KEY')
    consumer_secret = os.getenv('DOK_WP_WOO_Consumer_SECRET')
    
    if not consumer_key or consumer_key == 'your_consumer_key':
        print("❌ 오류: DOK_WP_WOO_Consumer_KEY가 설정되지 않았습니다.")
        return None
    
    if not consumer_secret or consumer_secret == 'your_consumer_secret':
        print("❌ 오류: DOK_WP_WOO_Consumer_SECRET가 설정되지 않았습니다.")
        return None
    
    # HTTP Basic Auth 헤더 생성
    credentials = f"{consumer_key}:{consumer_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    return {
        'consumer_key': consumer_key,
        'consumer_secret': consumer_secret,
        'headers': {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/json'
        }
    }

def setup_supabase():
    """Supabase 연결 정보를 설정합니다."""
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url:
        print("❌ 오류: SUPABASE_URL이 설정되지 않았습니다.")
        return None
    
    if not supabase_key:
        print("❌ 오류: SUPABASE_KEY가 설정되지 않았습니다.")
        return None
    
    return {
        'url': supabase_url,
        'key': supabase_key,
        'headers': {
            'apikey': supabase_key,
            'Authorization': f'Bearer {supabase_key}',
            'Content-Type': 'application/json'
        }
    }

def format_phone_number(phone):
    """전화번호를 올바른 형식으로 변환합니다."""
    if not phone:
        return ''
    
    phone_str = str(phone).strip()
    
    # 빈 문자열이면 그대로 반환
    if not phone_str:
        return ''
    
    # 이미 + 기호가 있으면 그대로 반환
    if phone_str.startswith('+'):
        return phone_str
    
    # 한국 번호 패턴 (010, 011 등으로 시작)이면 그대로 반환
    if phone_str.startswith(('010', '011', '016', '017', '018', '019')):
        return phone_str
    
    # 한국 번호에서 0이 누락된 경우 (10, 11, 16, 17, 18, 19로 시작하고 10자리)
    if (phone_str.startswith(('10', '11', '16', '17', '18', '19')) and 
        len(phone_str) == 10):
        return '0' + phone_str
    
    # 숫자로 시작하고 한국 번호가 아니면 국가번호로 간주하여 + 추가
    if phone_str[0].isdigit():
        # 0049, 001 등의 잘못된 패턴 수정
        if phone_str.startswith('0049'):
            return '+49' + phone_str[4:]  # 0049 → +49
        elif phone_str.startswith('001'):
            return '+1' + phone_str[3:]   # 001 → +1
        elif phone_str.startswith('0086'):
            return '+86' + phone_str[4:]  # 0086 → +86
        elif phone_str.startswith('0033'):
            return '+33' + phone_str[4:]  # 0033 → +33
        elif phone_str.startswith('0044'):
            return '+44' + phone_str[4:]  # 0044 → +44
        elif phone_str.startswith('0081'):
            return '+81' + phone_str[4:]  # 0081 → +81
        else:
            return '+' + phone_str
    
    # 기타 경우는 그대로 반환
    return phone_str

def convert_wp_date_to_kst_iso(date_str):
    """WordPress 날짜를 KST ISO 형식으로 변환합니다."""
    if not date_str:
        return None
    
    try:
        # WordPress 날짜 형식: 2024-12-27T17:03:00
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        
        # UTC라고 가정하고 KST로 변환
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
        
        # KST로 변환
        kst = pytz.timezone('Asia/Seoul')
        kst_dt = dt.astimezone(kst)
        
        return kst_dt.isoformat()
        
    except Exception as e:
        print(f"⚠️ 날짜 변환 오류: {date_str} - {e}")
        return None

def get_last_24h_range_kst():
    """KST 기준 일일 업데이트 범위를 반환합니다 (전전날 23:00 ~ 전날 24:00, 총 25시간).
    get_orders.py와 동일한 시간 범위를 사용합니다."""
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    
    # GitHub Actions가 오전 1시에 실행되므로
    # 전전날 23:00 ~ 전날 24:00 (25시간) 범위로 설정
    
    # 전날 자정 (24:00 = 다음날 00:00)
    yesterday_midnight = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 전전날 23:00 (2일 전 + 23시간)
    day_before_yesterday_11pm = yesterday_midnight - timedelta(days=1, hours=1)
    
    # 전날 24:00 (= 당일 00:00)
    yesterday_24h = yesterday_midnight
    
    start_time = day_before_yesterday_11pm  # 전전날 23:00
    end_time = yesterday_24h                # 전날 24:00
    
    return start_time, end_time

def filter_orders_by_product_name(orders, target_keywords):
    """상품명에 특정 키워드가 포함된 주문만 필터링합니다."""
    filtered_orders = []
    
    for order in orders:
        line_items = order.get('line_items', [])
        has_target_product = False
        
        for item in line_items:
            item_name = item.get('name', '').lower()
            
            # 키워드 중 하나라도 포함되면 해당 주문 포함
            for keyword in target_keywords:
                if keyword.lower() in item_name:
                    has_target_product = True
                    break
            
            if has_target_product:
                break
        
        if has_target_product:
            filtered_orders.append(order)
    
    return filtered_orders

def get_woocommerce_orders_by_date_range(wc_auth, start_time, end_time, page=1, per_page=100):
    """WooCommerce API에서 특정 날짜 범위의 주문을 조회합니다."""
    
    # WooCommerce REST API 엔드포인트 (dasdeutsch.com)
    wp_domain = 'https://dasdeutsch.com'
    url = f"{wp_domain}/wp-json/wc/v3/orders"
    
    # ISO 8601 형식으로 변환 (WooCommerce API 요구사항)
    after_iso = start_time.isoformat() if start_time else None
    before_iso = end_time.isoformat() if end_time else None
    
    params = {
        'page': page,
        'per_page': per_page,
        'status': 'any',  # 모든 상태의 주문
        'orderby': 'date',
        'order': 'desc'
    }
    
    # 날짜 범위 필터 추가
    if after_iso:
        params['after'] = after_iso
    if before_iso:
        params['before'] = before_iso
    
    try:
        print(f"🔍 WooCommerce API 호출: 날짜 범위 조회, 페이지 {page}")
        if after_iso and before_iso:
            print(f"   기간: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} (KST)")
        print(f"   URL: {url}")
        print(f"   파라미터: {params}")
        
        response = requests.get(
            url,
            headers=wc_auth['headers'],
            params=params,
            timeout=30,
            verify=False  # SSL 검증 비활성화 (개발/테스트용)
        )
        
        print(f"📊 API 응답: HTTP {response.status_code}")
        
        if response.status_code == 200:
            orders = response.json()
            
            # 페이지네이션 정보 추출
            pagination = {
                'total_orders': response.headers.get('X-WP-Total', len(orders)),
                'total_pages': response.headers.get('X-WP-TotalPages', 1),
                'current_page': page,
                'per_page': per_page
            }
            
            print(f"✅ 성공: {len(orders)}개 주문 조회")
            return orders, pagination
            
        elif response.status_code == 401:
            print("❌ 인증 실패: Consumer Key/Secret이 유효하지 않습니다")
            return [], {}
            
        else:
            print(f"❌ API 호출 실패: HTTP {response.status_code}")
            print(f"   응답: {response.text[:200]}...")
            return [], {}
            
    except Exception as e:
        print(f"❌ API 호출 중 오류 발생: {e}")
        return [], {}

def get_woocommerce_orders_all(wc_auth, page=1, per_page=100):
    """WooCommerce API에서 모든 주문을 조회합니다 (상품명으로 클라이언트 측 필터링용)."""
    
    # WooCommerce REST API 엔드포인트 (dasdeutsch.com)
    wp_domain = 'https://dasdeutsch.com'
    url = f"{wp_domain}/wp-json/wc/v3/orders"
    
    params = {
        'page': page,
        'per_page': per_page,
        'status': 'any',  # 모든 상태의 주문
        'orderby': 'date',
        'order': 'desc'
    }
    
    try:
        print(f"🔍 WooCommerce API 호출: 모든 주문 조회, 페이지 {page}")
        print(f"   URL: {url}")
        print(f"   파라미터: {params}")
        
        response = requests.get(
            url,
            headers=wc_auth['headers'],
            params=params,
            timeout=30,
            verify=False  # SSL 검증 비활성화 (개발/테스트용)
        )
        
        print(f"📊 API 응답: HTTP {response.status_code}")
        
        if response.status_code == 200:
            orders = response.json()
            
            # 페이지네이션 정보 추출
            pagination = {
                'total_orders': response.headers.get('X-WP-Total', len(orders)),
                'total_pages': response.headers.get('X-WP-TotalPages', 1),
                'current_page': page,
                'per_page': per_page
            }
            
            print(f"✅ 성공: {len(orders)}개 주문 조회")
            return orders, pagination
            
        elif response.status_code == 401:
            print("❌ 인증 실패: Consumer Key/Secret이 유효하지 않습니다")
            return [], {}
            
        else:
            print(f"❌ API 호출 실패: HTTP {response.status_code}")
            print(f"   응답: {response.text[:200]}...")
            return [], {}
            
    except Exception as e:
        print(f"❌ API 호출 중 오류 발생: {e}")
        return [], {}

def get_woocommerce_orders_by_product(wc_auth, product_id, page=1, per_page=100):
    """WooCommerce API에서 특정 상품 ID로 주문을 조회합니다."""
    
    # WooCommerce REST API 엔드포인트 (dasdeutsch.com)
    wp_domain = 'https://dasdeutsch.com'
    url = f"{wp_domain}/wp-json/wc/v3/orders"
    
    params = {
        'product': product_id,  # 특정 상품 ID로 필터링
        'page': page,
        'per_page': per_page,
        'status': 'any'  # 모든 상태의 주문
    }
    
    try:
        print(f"🔍 WooCommerce API 호출: 상품 ID {product_id}, 페이지 {page}")
        print(f"   URL: {url}")
        print(f"   파라미터: {params}")
        
        response = requests.get(
            url,
            headers=wc_auth['headers'],
            params=params,
            timeout=30,
            verify=False  # SSL 검증 비활성화 (개발/테스트용)
        )
        
        print(f"📊 API 응답: HTTP {response.status_code}")
        
        if response.status_code == 200:
            orders = response.json()
            
            # 페이지네이션 정보 추출
            pagination = {
                'total_orders': response.headers.get('X-WP-Total', len(orders)),
                'total_pages': response.headers.get('X-WP-TotalPages', 1),
                'current_page': page,
                'per_page': per_page
            }
            
            print(f"✅ 성공: {len(orders)}개 주문 조회")
            return orders, pagination
            
        elif response.status_code == 401:
            print("❌ 인증 실패: Consumer Key/Secret이 유효하지 않습니다")
            print("💡 확인 사항:")
            print("   1. WooCommerce → 설정 → 고급 → REST API")
            print("   2. Consumer Key/Secret 재생성")
            print("   3. 권한을 'Read/Write'로 설정")
            return [], {}
            
        else:
            print(f"❌ API 호출 실패: HTTP {response.status_code}")
            print(f"   응답: {response.text[:200]}...")
            return [], {}
            
    except Exception as e:
        print(f"❌ API 호출 중 오류 발생: {e}")
        return [], {}

def get_recent_canceled_orders_wp(wc_auth):
    """최근 1개월 내 취소된 주문을 조회합니다 (get_orders.py와 동일한 로직)."""
    kst = pytz.timezone('Asia/Seoul')
    now_kst = datetime.now(kst)
    one_month_ago = now_kst - timedelta(days=30)
    
    print(f"   📋 취소 주문 확인 범위: {one_month_ago.strftime('%Y-%m-%d')} ~ {now_kst.strftime('%Y-%m-%d')}")
    
    # 최근 1개월 범위에서 취소 상태 주문만 조회
    return get_woocommerce_orders_by_status_and_date_range(wc_auth, 'cancelled', one_month_ago, now_kst)

def get_woocommerce_orders_by_status_and_date_range(wc_auth, status, start_time, end_time):
    """특정 상태와 날짜 범위의 WooCommerce 주문을 조회합니다."""
    wp_domain = 'https://dasdeutsch.com'
    url = f"{wp_domain}/wp-json/wc/v3/orders"
    
    # ISO 8601 형식으로 변환
    after_iso = start_time.isoformat() if start_time else None
    before_iso = end_time.isoformat() if end_time else None
    
    params = {
        'status': status,  # 특정 상태 (cancelled, refunded 등)
        'per_page': 100,
        'orderby': 'date',
        'order': 'desc'
    }
    
    # 날짜 범위 필터 추가
    if after_iso:
        params['after'] = after_iso
    if before_iso:
        params['before'] = before_iso
    
    all_orders = []
    page = 1
    
    try:
        while True:
            params['page'] = page
            
            response = requests.get(
                url,
                headers=wc_auth['headers'],
                params=params,
                timeout=30,
                verify=False
            )
            
            if response.status_code == 200:
                orders = response.json()
                
                if not orders:
                    break
                
                all_orders.extend(orders)
                
                # 페이지네이션 확인
                total_pages = int(response.headers.get('X-WP-TotalPages', 1))
                if page >= total_pages:
                    break
                    
                page += 1
            else:
                print(f"   ❌ 취소 주문 조회 실패: HTTP {response.status_code}")
                break
                
    except Exception as e:
        print(f"   ❌ 취소 주문 조회 중 오류: {e}")
    
    return all_orders

def convert_woocommerce_to_supabase_format(wc_order):
    """WooCommerce 주문을 Supabase 형식으로 변환합니다."""
    
    # 기본 주문 정보
    order_id = str(wc_order.get('id', ''))
    order_no = order_id  # WordPress 주문 번호 (접두사 없이)
    
    # 주문 시간 변환
    order_time_str = wc_order.get('date_created', '')
    order_time = convert_wp_date_to_kst_iso(order_time_str)
    
    # 결제 시간 변환 (date_paid 또는 date_created 사용)
    payment_time_str = wc_order.get('date_paid', '') or order_time_str
    payment_time = convert_wp_date_to_kst_iso(payment_time_str)
    
    # 고객 정보 (billing)
    billing = wc_order.get('billing', {})
    orderer_name = f"{billing.get('first_name', '')} {billing.get('last_name', '')}".strip()
    orderer_email = billing.get('email', '')
    orderer_phone = format_phone_number(billing.get('phone', ''))
    
    # 주문 상태 매핑
    status_mapping = {
        'pending': '결제대기',
        'processing': '주문접수', 
        'on-hold': '보류',
        'completed': '배송완료',
        'cancelled': 'CANCEL',
        'refunded': '환불됨',
        'failed': '결제실패'
    }
    order_status = status_mapping.get(wc_order.get('status', ''), wc_order.get('status', ''))
    
    # 결제 정보 (정수로 변환)
    total_amount = int(float(wc_order.get('total', 0)))
    discount_amount = int(float(wc_order.get('discount_total', 0)))
    
    # 상품 정보 (line_items에서 추출)
    line_items = wc_order.get('line_items', [])
    converted_orders = []
    
    if not line_items:
        # 상품이 없는 경우 기본 정보만 저장
        converted_orders.append({
            'order_no': order_no,
            'order_time': order_time,
            'payment_time': payment_time,  # 결제 시간 추가
            'order_status': order_status,
            'orderer_name': orderer_name,
            'orderer_email': orderer_email, 
            'orderer_phone': orderer_phone,
            'delivery_phone': orderer_phone,  # 기본값으로 주문자 번호 사용
            'prod_name': '상품 정보 없음',
            'prod_quantity': 1,
            'prod_price': total_amount,
            'coupon_discount': discount_amount,
            'order_payment_amount': total_amount,
            'order_code': f'w{order_id}',  # WordPress 주문 코드 (w + 주문ID)
            'prod_no': '0'
        })
    else:
        # 각 상품별로 행 생성 (상품 ID 237513만)
        for item in line_items:
            product_id = item.get('product_id', 0)
            
            # 상품 ID 237513만 처리
            if product_id != 237513:
                continue
                
            prod_name = item.get('name', '상품명 없음')
            prod_quantity = int(item.get('quantity', 1))
            prod_price = int(float(item.get('total', 0))) + discount_amount  # 할인 전 가격 (정수)
            
            # prod_no는 실제 상품 ID 사용
            prod_no = str(product_id)
            
            
            converted_orders.append({
                'order_no': order_no,
                'order_time': order_time,
                'payment_time': payment_time,  # 결제 시간 추가
                'order_status': order_status,
                'orderer_name': orderer_name,
                'orderer_email': orderer_email,
                'orderer_phone': orderer_phone,
                'delivery_phone': orderer_phone,  # 기본값으로 주문자 번호 사용
                'prod_name': prod_name,
                'prod_quantity': prod_quantity,
                'prod_price': prod_price,
                'coupon_discount': int(discount_amount / len(line_items)),  # 할인을 상품별로 분할 (정수)
                'order_payment_amount': int(total_amount / len(line_items)),  # 결제금액을 상품별로 분할 (정수)
                'order_code': f'w{order_id}',  # WordPress 주문 코드 (w + 주문ID)
                'prod_no': prod_no
            })
    
    return converted_orders

def upsert_to_supabase(supabase_config, orders_data):
    """변환된 주문 데이터를 Supabase에 upsert합니다."""
    if not orders_data:
        print("📋 저장할 데이터가 없습니다.")
        return
    
    # 중복 제거 (order_no, prod_no 기준)
    unique_orders = {}
    for order in orders_data:
        key = (order['order_no'], order['prod_no'])
        unique_orders[key] = order
    
    orders_data = list(unique_orders.values())
    print(f"🔍 중복 제거 후: {len(orders_data)}개 행")
    
    # Supabase upsert (order_no, prod_no 기준)
    url = f"{supabase_config['url']}/rest/v1/uzu_orders?on_conflict=order_no,prod_no"
    
    try:
        response = requests.post(
            url,
            headers=supabase_config['headers'],
            json=orders_data,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            print(f"✅ Supabase upsert 성공: {len(orders_data)}개 행")
        else:
            print(f"❌ Supabase upsert 실패: HTTP {response.status_code}")
            print(f"   응답: {response.text[:200]}...")
            
    except Exception as e:
        print(f"❌ Supabase upsert 중 오류: {e}")

def main():
    # 명령행 인자 파싱
    parser = argparse.ArgumentParser(description='WooCommerce 주문 데이터를 Supabase에 저장합니다.')
    parser.add_argument('--test-product', type=str, help='특정 상품 ID 테스트')
    parser.add_argument('--test-connection', action='store_true', help='API 연결만 테스트')
    parser.add_argument('--daily', action='store_true', help='일일 업데이트 (get_orders.py와 동일한 시간대)')
    parser.add_argument('--date', type=str, help='특정 날짜 주문 처리 (YYYY-MM-DD 형식)')
    args = parser.parse_args()
    
    # .env 파일 로드
    load_dotenv()
    
    # WooCommerce Consumer Key/Secret 설정
    print("🔗 WooCommerce API 연결 설정 중...")
    wc_auth = setup_woocommerce_auth()
    if not wc_auth:
        return
    print("✅ WooCommerce API 인증 정보 설정 완료")
    
    # Supabase 설정
    print("🔗 Supabase 연결 설정 중...")
    supabase_config = setup_supabase()
    if not supabase_config:
        return
    print("✅ Supabase 연결 정보 설정 완료")
    
    try:
        # 연결 테스트만 하는 경우
        if args.test_connection:
            print("🧪 연결 테스트 모드")
            orders, pagination = get_woocommerce_orders_by_product(wc_auth, "237513", page=1, per_page=1)
            if orders:
                print("✅ WooCommerce API 연결 성공!")
            return
        
        # 일일 업데이트 모드 (독일어 관련 상품만 수집)
        if args.daily:
            print("⏰ 일일 업데이트 모드 (상품 ID 237513만 수집)")
            
            # get_orders.py와 동일한 시간 범위 사용
            start_time, end_time = get_last_24h_range_kst()
            print(f"📅 업데이트 기간: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} (KST)")
            print(f"   ⏰ 25시간 범위로 누락 방지 (GitHub Actions 오전 1시 실행)")
            print(f"🎯 대상 상품: ID 237513")
            
            # 지정된 시간 범위의 모든 주문 조회 후 상품명으로 필터링
            all_orders = []
            page = 1
            
            while True:
                orders, pagination = get_woocommerce_orders_by_date_range(wc_auth, start_time, end_time, page=page, per_page=100)
                
                if not orders:
                    break
                
                all_orders.extend(orders)
                
                # 페이지네이션 확인
                total_pages = int(pagination.get('total_pages', 1))
                if page >= total_pages:
                    break
                    
                page += 1
            
            # 상품 ID 237513 필터링 및 시간 범위 정확한 필터링 (클라이언트 측) - 결제 시간 기준
            time_filtered = []
            for order in all_orders:
                # 상품 ID 237513이 포함된 주문만 처리
                line_items = order.get('line_items', [])
                has_target_product = False
                
                for item in line_items:
                    product_id = item.get('product_id', 0)
                    if product_id == 237513:
                        has_target_product = True
                        break
                
                if not has_target_product:
                    continue
                # 결제 시간 우선 사용, 없으면 생성 시간 사용
                payment_date_str = order.get('date_paid', '') or order.get('date_created', '')
                if payment_date_str:
                    try:
                        # WordPress 시간은 이미 KST 기준이므로 그대로 사용
                        kst = pytz.timezone('Asia/Seoul')
                        
                        if payment_date_str.endswith('Z'):
                            # Z가 있는 경우는 UTC로 처리
                            order_dt = datetime.fromisoformat(payment_date_str.replace('Z', '+00:00'))
                            order_kst = order_dt.astimezone(kst)
                        else:
                            # Z가 없으면 이미 KST 기준으로 해석
                            order_dt = datetime.fromisoformat(payment_date_str)
                            if order_dt.tzinfo is None:
                                # 시간대 정보가 없으면 KST로 가정 (WordPress 기본 설정)
                                order_kst = kst.localize(order_dt)
                            else:
                                order_kst = order_dt.astimezone(kst)
                        
                        # 지정된 시간 범위 내인지 정확히 확인 (결제 시간 기준)
                        if start_time <= order_kst <= end_time:
                            time_filtered.append(order)
                            payment_type = "결제" if order.get('date_paid') else "생성"
                            print(f"    ✅ 범위 내 주문: {order.get('id')} | {order_kst.strftime('%Y-%m-%d %H:%M:%S')} KST ({payment_type})")
                        else:
                            payment_type = "결제" if order.get('date_paid') else "생성"
                            print(f"    ❌ 범위 외 주문: {order.get('id')} | {order_kst.strftime('%Y-%m-%d %H:%M:%S')} KST ({payment_type})")
                    except Exception as e:
                        print(f"    ⚠️ 날짜 변환 오류: {payment_date_str} - {e}")
                        continue
            
            print(f"✅ 일일 업데이트 조회 완료: {len(all_orders)}개 주문 → {len(time_filtered)}개 상품 ID 237513 & 시간 범위 내")
            all_orders = time_filtered
            
        # 특정 날짜 모드 (상품 ID 237513만 수집)
        elif args.date:
            print(f"📅 특정 날짜 모드: {args.date} (상품 ID 237513만 수집)")
            
            try:
                # 특정 날짜를 KST 기준으로 변환
                kst = pytz.timezone('Asia/Seoul')
                date_dt = datetime.strptime(args.date, '%Y-%m-%d')
                start_time = kst.localize(date_dt.replace(hour=0, minute=0, second=0))
                end_time = kst.localize(date_dt.replace(hour=23, minute=59, second=59))
                
                print(f"📅 조회 기간: {start_time.strftime('%Y-%m-%d %H:%M')} ~ {end_time.strftime('%Y-%m-%d %H:%M')} (KST)")
                print(f"🎯 대상 상품: ID 237513")
                
                # 해당 날짜의 모든 주문 조회 후 상품명으로 필터링
                all_orders = []
                page = 1
                
                while True:
                    orders, pagination = get_woocommerce_orders_by_date_range(wc_auth, start_time, end_time, page=page, per_page=100)
                    
                    if not orders:
                        break
                    
                    all_orders.extend(orders)
                    
                    # 페이지네이션 확인
                    total_pages = int(pagination.get('total_pages', 1))
                    if page >= total_pages:
                        break
                        
                    page += 1
                
                # 상품 ID 237513만 필터링
                filtered_orders = []
                for order in all_orders:
                    line_items = order.get('line_items', [])
                    has_target_product = False
                    
                    for item in line_items:
                        product_id = item.get('product_id', 0)
                        if product_id == 237513:
                            has_target_product = True
                            break
                    
                    if has_target_product:
                        filtered_orders.append(order)
                
                print(f"✅ 특정 날짜 조회 완료: {len(all_orders)}개 주문 → {len(filtered_orders)}개 상품 ID 237513 주문")
                all_orders = filtered_orders
                
            except ValueError:
                print("❌ 날짜 형식 오류: YYYY-MM-DD 형식으로 입력해주세요.")
                return
        
        # 특정 상품 테스트
        elif args.test_product:
            print(f"🧪 테스트 모드: 상품 ID {args.test_product}")
            
            # 특정 상품 ID로 주문 조회
            orders, pagination = get_woocommerce_orders_by_product(wc_auth, args.test_product)
            
            if orders:
                print(f"\n📊 조회 결과:")
                print(f"   총 주문: {pagination.get('total_orders', len(orders))}개")
                print(f"   현재 페이지: {pagination.get('current_page', 1)}/{pagination.get('total_pages', 1)}")
                
                print(f"\n📋 주문 샘플 (처음 5개):")
                for i, order in enumerate(orders[:5], 1):
                    order_id = order.get('id', 'N/A')
                    order_status = order.get('status', 'N/A')
                    customer = order.get('billing', {})
                    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
                    order_date = order.get('date_created', 'N/A')[:10]
                    
                    print(f"   {i}. 주문 ID: {order_id} | 상태: {order_status}")
                    print(f"      고객: {customer_name} | 날짜: {order_date}")
                
                # Supabase 형식으로 변환
                print(f"\n🔄 Supabase 형식으로 변환 중...")
                all_converted_orders = []
                
                for order in orders:
                    converted = convert_woocommerce_to_supabase_format(order)
                    all_converted_orders.extend(converted)
                
                print(f"✅ 변환 완료: {len(orders)}개 주문 → {len(all_converted_orders)}개 행")
                
                # Supabase에 저장
                print(f"\n🚀 Supabase에 저장 중...")
                upsert_to_supabase(supabase_config, all_converted_orders)
                
                print(f"\n🎉 테스트 완료!")
                
            else:
                print("📋 해당 상품 ID로 조회된 주문이 없습니다.")
            
            return
        
        else:
            # 기본: 도움말 출력
            parser.print_help()
            return
        
        # 공통 처리: 조회된 주문을 Supabase에 저장
        if 'all_orders' in locals() and all_orders:
            # 취소된 주문 상태 업데이트 (get_orders.py와 동일)
            print(f"\n🔄 최근 1개월 취소 주문 상태 업데이트 확인 중...")
            cancel_orders = get_recent_canceled_orders_wp(wc_auth)
            
            if cancel_orders:
                print(f"✅ 취소 주문 {len(cancel_orders)}개 발견")
                all_orders.extend(cancel_orders)
            else:
                print(f"📋 취소 주문 없음")
            print(f"\n🔄 Supabase 형식으로 변환 중...")
            all_converted_orders = []
            
            for order in all_orders:
                converted = convert_woocommerce_to_supabase_format(order)
                all_converted_orders.extend(converted)
            
            print(f"✅ 변환 완료: {len(all_orders)}개 주문 → {len(all_converted_orders)}개 행")
            
            # Supabase에 저장
            print(f"\n🚀 Supabase에 저장 중...")
            upsert_to_supabase(supabase_config, all_converted_orders)
            
            print(f"\n🎉 처리 완료!")
            
            # 요약 정보 출력
            print(f"\n📋 처리 요약:")
            print(f"   총 주문 수: {len(all_orders)}개")
            print(f"   저장된 행: {len(all_converted_orders)}개")
            
            if all_orders:
                print(f"\n📊 최근 주문 미리보기:")
                for i, order in enumerate(all_orders[:3], 1):
                    order_id = order.get('id', 'N/A')
                    customer = order.get('billing', {})
                    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip()
                    order_date = order.get('date_created', 'N/A')[:10]
                    
                    print(f"  {i}. 주문 ID: {order_id}")
                    print(f"     고객: {customer_name} | 날짜: {order_date}")
        
        elif 'all_orders' in locals():
            print("📋 조회된 주문이 없습니다.")
        
    except KeyboardInterrupt:
        print("\n\n⚠️ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()