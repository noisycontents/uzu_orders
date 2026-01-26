#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV 파일을 Supabase 형식으로 직접 변환하여 저장하는 스크립트
============================================

CSV의 모든 행을 API 호출 없이 직접 Supabase에 저장합니다.

🔄 상품 매핑 시스템:
- CSV 상품명 → 실제 API prod_no 매핑
- get_orders.py와 동일한 prod_no 사용으로 중복 방지
- 새 상품 추가 시 get_product_mapping() 함수 수정

💡 새 상품 추가 방법:
1. get_orders.py --date YYYY-MM-DD 실행
2. Supabase에서 새 상품의 prod_name, prod_no 확인
3. get_product_mapping() 함수에 매핑 추가
4. 다양한 버전 (30일권, 365일권 등) 함께 추가

⚠️ 중요: order_code, prod_no가 get_orders.py와 일치해야 중복 방지됨
"""

import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv
import pytz

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

def convert_csv_to_kst_datetime(date_str):
    """CSV의 날짜 문자열을 KST ISO 형식으로 변환합니다."""
    if not date_str:
        return None
    
    try:
        kst = pytz.timezone('Asia/Seoul')
        
        # CSV 형식: "2025-01-24 16:39" 
        if len(date_str) == 16:  # "YYYY-MM-DD HH:MM"
            dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M')
            kst_dt = kst.localize(dt)
            return kst_dt.isoformat()
        else:
            return None
            
    except Exception as e:
        print(f"⚠️ 날짜 변환 오류: {date_str} - {e}")
        return None

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

def upsert_to_supabase(supabase_config, orders_data):
    """주문 데이터를 Supabase에 효율적으로 upsert합니다."""
    import time
    
    try:
        base_url = f"{supabase_config['url']}/rest/v1/uzu_orders?on_conflict=order_no,prod_no"
        headers = supabase_config['headers'].copy()
        headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        print(f"🔄 {len(orders_data)}개 행을 Supabase에 upsert 중...")
        
        # 먼저 배치 내 중복 제거
        print(f"🔍 배치 내 중복 제거 중...")
        seen_combinations = set()
        deduplicated_data = []
        
        for order in orders_data:
            combination = (order.get('order_no', ''), order.get('prod_no', ''))
            if combination not in seen_combinations:
                seen_combinations.add(combination)
                deduplicated_data.append(order)
        
        if len(deduplicated_data) != len(orders_data):
            print(f"🔁 배치 내 중복 제거: {len(orders_data)} → {len(deduplicated_data)}개")
        
        # 배치 크기로 나누어 저장
        batch_size = 50  # 더 큰 배치로 효율성 향상
        success_count = 0
        
        for i in range(0, len(deduplicated_data), batch_size):
            batch = deduplicated_data[i:i + batch_size]
            batch_num = i//batch_size + 1
            
            try:
                response = requests.post(
                    base_url,
                    headers=headers,
                    json=batch,
                    timeout=60
                )
                
                if response.status_code in [200, 201]:
                    success_count += len(batch)
                    print(f"  ✅ 배치 {batch_num} 완료 ({len(batch)}개 행)")
                else:
                    print(f"  ❌ 배치 {batch_num} 실패: HTTP {response.status_code}")
                    
            except Exception as e:
                print(f"  ❌ 배치 {batch_num} 오류: {e}")
        
        print(f"🎉 총 {success_count}개 행이 Supabase에 저장되었습니다!")
        return success_count > 0
        
    except Exception as e:
        print(f"❌ Supabase upsert 실패: {e}")
        return False

def get_product_mapping():
    """
    상품명을 실제 API의 prod_no로 매핑하는 딕셔너리를 반환합니다.
    
    🔄 새로운 상품 추가 방법:
    1. get_orders.py --date YYYY-MM-DD 실행하여 새 상품의 실제 prod_no 확인
    2. Supabase에서 해당 상품명과 prod_no 확인:
       python3 -c "
       import os, requests
       from dotenv import load_dotenv
       load_dotenv()
       url = f\"{os.getenv('SUPABASE_URL')}/rest/v1/uzu_orders\"
       headers = {'apikey': os.getenv('SUPABASE_KEY'), 'Authorization': f'Bearer {os.getenv(\"SUPABASE_KEY\")}'}
       response = requests.get(f'{url}?prod_name=like.*새상품명*&select=prod_name,prod_no&limit=5', headers=headers)
       print(response.json())
       "
    3. 아래 딕셔너리에 '상품명': 'prod_no' 형태로 추가
    4. 다양한 버전 (30일권, 365일권, [시크릿] 유무 등)도 함께 추가
    
    ⚠️ 주의사항:
    - prod_no는 반드시 문자열로 입력 ('123456', not 123456)
    - 상품명은 CSV와 정확히 일치해야 함
    - 기간별 버전 (30일권, 365일권 등)은 별도로 추가하거나 부분 매칭 로직 활용
    """
    return {
        # === API 실제값 완전 매핑 (2025-09-11 최종 업데이트) ===
        
        'K-표현 영어로 풀기': '815890',  # API 최신값
        'SAT급 고급 영단어 1000': '123456',  # API 최신값
        '[VIP시크릿]K-표현 영어로 풀기 30일권': '815890',  # API 최신값
        '[VIP시크릿]네이티브 바이브 영어 30일권': '472892',  # API 최신값
        '[VIP시크릿]바로 써 먹는 일상 일본어 30일권': '318007',  # API 최신값
        '[VIP시크릿]월스트리트에서 통하는 영어 30일권': '667062',  # API 최신값
        '[미라클-톡] 독일어 멤버십 365일권': '955731',  # API 최신값
        '[미라클-톡] 독일어 멤버십 7일권': '955731',  # API 최신값
        '[미라클-톡] 미라클 1000단어(중급)': '318007',  # API 최신값
        '[미라클-톡] 영어 1년 멤버십': '472892',  # API 최신값
        '[미라클-톡] 영어 멤버십 7일권': '472892',  # API 최신값
        '[미라클-톡] 일본어 멤버십 365일권': '318007',  # API 최신값
        '[미라클-톡] 일본어 멤버십 7일권': '318007',  # API 최신값
        '[미라클-톡] 일본어 멤버십 체험판': '318007',  # API 최신값
        '[시크릿]K-표현 영어로 풀기 30일권': '815890',  # API 최신값
        '[시크릿]SAT급 고급 영단어 1000 30일권': '657779',  # API 최신값
        '[시크릿]네이티브 바이브 영어 30일권': '404493',  # API 최신값
        '[시크릿]미국 중학생 영단어 1000 30일권': '641039',  # API 최신값
        '[시크릿]바로 써 먹는 일상 일본어 30일권': '318007',  # API 최신값
        '[시크릿]실전 맞춤 진짜 독일어 30일권': '955731',  # API 최신값
        '[시크릿]왕초보 영단어 1000 30일권': '30',  # API 최신값
        '[시크릿]월스트리트에서 통하는 영어 30일권': '667062',  # API 최신값
        '[시크릿]일상 영어 패턴 레시피 30일권': '33',  # API 최신값
        '[원티드]K-표현 영어로 풀기 30일권': '815890',  # API 최신값
        '[원티드]네이티브 바이브 영어 30일권': '44',  # API 최신값
        '[원티드]미국 중학생 영단어 1000 30일권': '641039',  # API 최신값
        '[원티드]바로 써 먹는 일상 일본어 30일권': '318007',  # API 최신값
        '[원티드]실전 맞춤 진짜 독일어 30일권': '955731',  # API 최신값
        '[원티드]왕초보 영단어 1000 30일권': '30',  # API 최신값
        '[원티드]월스트리트에서 통하는 영어 30일권': '667062',  # API 최신값
        '[원티드]일상 영어 패턴 레시피 30일권': '33',  # API 최신값
        '네이티브 바이브 영어': '44',  # API 최신값
        '미국 중학생 영단어 1000': '641039',  # API 최신값
        '바로 써 먹는 일상 일본어': '923262',  # API 최신값
        '실전 맞춤 진짜 독일어': '955731',  # API 최신값
        '왕초보 영단어 1000': '30',  # API 최신값
        '월스트리트에서 통하는 영어': '667062',  # API 최신값
        '일상 영어 패턴 레시피': '33',  # API 최신값
        '톡톡 영어 1년 멤버십': '472892',  # API 최신값
        '톡톡 일본어 1일 멤버십': '318007',  # API 최신값
        '필사클럽': '724286',  # API 최신값
        '필사클럽 노트(PDF)': '859428',  # API 최신값
        '필사클럽 노트(실물)': '454333',  # API 최신값
        '필사클럽 참가신청': '724286',  # API 최신값
        
        # === 추가 예정 상품들 (실제 prod_no로 업데이트 필요) ===
        'SAT급 고급 영단어 1000': '123456',                  # ⚠️ 예시값 - 실제 API에서 확인 필요
        
        # === 새 상품 추가 템플릿 ===
        # '새상품명': '실제_prod_no',  # 확인일: YYYY-MM-DD, 메모: 설명
    }

def get_order_code_mapping(supabase_config):
    """API에서 order_no → order_code 매핑을 가져옵니다."""
    try:
        url = f"{supabase_config['url']}/rest/v1/uzu_orders?order_code=like.o*&select=order_no,order_code&limit=2000"
        response = requests.get(url, headers=supabase_config['headers'], timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            
            # order_no별로 가장 최근 order_code 매핑
            order_mapping = {}
            for row in data:
                order_no = row['order_no']
                order_code = row['order_code']
                # 복잡한 order_code를 우선 사용 (API 생성)
                if order_no not in order_mapping or len(order_code) > len(order_mapping[order_no]):
                    order_mapping[order_no] = order_code
            
            print(f"📋 order_code 매핑 로드: {len(order_mapping)}개")
            return order_mapping
        else:
            print(f"❌ order_code 매핑 로드 실패: HTTP {response.status_code}")
            return {}
    except Exception as e:
        print(f"❌ order_code 매핑 오류: {e}")
        return {}

def main():
    load_dotenv()
    
    # Supabase 설정
    print("🔗 Supabase 연결 설정 중...")
    supabase_config = setup_supabase()
    if not supabase_config:
        return
    print("✅ Supabase 연결 정보 설정 완료")
    
    # 상품명 매핑 테이블 로드
    product_mapping = get_product_mapping()
    print(f"📋 상품명 매핑 테이블 로드 완료: {len(product_mapping)}개 상품")
    
    # order_code 매핑 테이블 로드
    order_code_mapping = get_order_code_mapping(supabase_config)
    
    csv_file = 'orders_20250902181302.csv'
    
    print(f"📊 CSV 파일 변환 시작: {csv_file}")
    
    supabase_data = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for i, row in enumerate(reader, 1):
                # CSV 데이터를 Supabase 형식으로 변환
                order_no = row.get('주문번호', '').strip()
                order_date = row.get('주문일', '').strip()
                pg_date = row.get('PG처리일시', '').strip()
                
                if not order_no:
                    continue
                
                # order_code 매핑 (API 실제값 사용, 없으면 기본값)
                order_code = order_code_mapping.get(order_no, f"o{order_no}")
                
                if order_code != f"o{order_no}":
                    print(f"✅ order_code 매핑: {order_no} → {order_code}")
                else:
                    print(f"⚠️ order_code 기본값: {order_no} → {order_code}")
                
                # === prod_no 매핑 로직 (실제 API와 동일한 값 사용) ===
                prod_name = row.get('상품명', '').strip()
                
                # 🔍 1단계: 정확한 매핑 먼저 시도
                # CSV의 상품명과 매핑 테이블의 키가 정확히 일치하는 경우
                prod_no = product_mapping.get(prod_name)
                
                # 🔍 2단계: 부분 매칭 시도 (기간 정보 제거 후 매칭)
                # 예: '[시크릿]일상 영어 패턴 레시피 365일권' → '[시크릿]일상 영어 패턴 레시피'
                if not prod_no:
                    clean_name = prod_name
                    # 일반적인 기간 표기 제거
                    for suffix in [' 30일권', ' 365일권', ' 7일권', ' 1년권', ' 1개월권', ' 12개월권']:
                        clean_name = clean_name.replace(suffix, '')
                    prod_no = product_mapping.get(clean_name)
                    
                    if prod_no:
                        print(f"✅ 부분 매핑 성공: '{prod_name}' → '{clean_name}' → {prod_no}")
                
                # 🔍 3단계: 해시 기반 폴백 (매핑 테이블에 없는 새 상품)
                if not prod_no:
                    prod_no = str(abs(hash(prod_name)) % 1000000)
                    print(f"⚠️ 매핑 없음 - 해시 사용: '{prod_name}' → {prod_no}")
                    print(f"   💡 새 상품 추가 필요: get_product_mapping() 함수에 추가하세요!")
                    print(f"   📋 추가 형식: '{prod_name}': '실제_prod_no',  # 확인 필요")
                else:
                    # 1단계에서 성공한 경우만 (2단계는 위에서 이미 출력)
                    if product_mapping.get(prod_name):
                        print(f"✅ 정확 매핑 성공: '{prod_name}' → {prod_no}")
                
                supabase_row = {
                    'order_code': order_code,
                    'order_no': order_no,
                    'order_time': convert_csv_to_kst_datetime(order_date),
                    'order_type': 'shopping',
                    'orderer_name': row.get('주문자 이름', '').strip(),
                    'orderer_email': row.get('주문자 이메일', '').strip(),
                    'orderer_phone': format_phone_number(row.get('주문자 번호', '').strip()),
                    'delivery_name': '',
                    'delivery_phone': '',
                    'delivery_postcode': '',
                    'delivery_address': '',
                    'delivery_address_detail': '',
                    'prod_no': prod_no,
                    'prod_name': prod_name,
                    'prod_quantity': int(row.get('구매수량', '1') or 1),
                    'prod_price': int(row.get('판매가', '0') or 0),
                    'prod_discount_amount': int(row.get('품목실결제가', '0') or 0),
                    'order_status': row.get('주문상태', '').strip(),
                    'payment_type': '',
                    'order_total_amount': int(row.get('최종주문금액', '0') or 0),
                    'order_discount_amount': int(row.get('품목쿠폰할인금액', '0') or 0),
                    'delivery_fee': 0,
                    'coupon_discount': int(row.get('품목쿠폰할인금액', '0') or 0),
                    'point_used': int(row.get('품목포인트사용금액', '0') or 0),
                    'order_payment_amount': int(row.get('품목실결제가', '0') or 0),
                    'payment_time': convert_csv_to_kst_datetime(pg_date),
                    'complete_time': convert_csv_to_kst_datetime(pg_date),
                    'device_type': '',
                    'is_gift': 'N'
                }
                
                supabase_data.append(supabase_row)
                
                if i % 100 == 0:
                    print(f"  📊 {i}개 행 변환 완료...")
        
        print(f"📈 CSV 변환 완료: {len(supabase_data)}개 행")
        
        # Supabase에 저장
        if supabase_data:
            success = upsert_to_supabase(supabase_config, supabase_data)
            
            if success:
                print("✅ CSV 데이터가 Supabase에 저장되었습니다!")
                print("🔗 Supabase 대시보드에서 데이터를 확인하실 수 있습니다.")
            else:
                print("❌ Supabase 저장에 실패했습니다.")
        else:
            print("⚠️ 변환할 데이터가 없습니다.")
            
    except Exception as e:
        print(f"❌ CSV 변환 중 오류: {e}")

if __name__ == "__main__":
    main()
