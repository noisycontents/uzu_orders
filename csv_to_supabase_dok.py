#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
우커머스 독일어 주문 데이터(orders_dok.csv)를 Supabase로 변환하여 저장하는 스크립트
============================================================================

우커머스 CSV 형식을 Supabase uzu_orders 테이블 형식으로 변환합니다.
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
    
    # 821로 시작하는 경우 +82로 변환
    if phone_str.startswith('821'):
        return '+82' + phone_str[3:]
    
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
        elif phone_str.startswith('491'):
            return '+49' + phone_str[3:]  # 491 → +49
        else:
            return '+' + phone_str
    
    # 기타 경우는 그대로 반환
    return phone_str

def convert_woocommerce_datetime_to_kst(date_str):
    """우커머스 날짜 문자열을 KST ISO 형식으로 변환합니다."""
    if not date_str:
        return None
    
    try:
        kst = pytz.timezone('Asia/Seoul')
        
        # 우커머스 형식: "2024-12-27 17:02" 
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
        base_url = f"{supabase_config['url']}/rest/v1/uzu_orders?on_conflict=order_code,prod_no"
        headers = supabase_config['headers'].copy()
        headers['Prefer'] = 'resolution=merge-duplicates,return=minimal'
        
        print(f"🔄 {len(orders_data)}개 행을 Supabase에 upsert 중...")
        
        # 먼저 배치 내 중복 제거
        print(f"🔍 배치 내 중복 제거 중...")
        seen_combinations = set()
        deduplicated_data = []
        
        for order in orders_data:
            combination = (order.get('order_code', ''), order.get('prod_no', ''))
            if combination not in seen_combinations:
                seen_combinations.add(combination)
                deduplicated_data.append(order)
        
        if len(deduplicated_data) != len(orders_data):
            print(f"🔁 배치 내 중복 제거: {len(orders_data)} → {len(deduplicated_data)}개")
        
        # 배치 크기로 나누어 저장
        batch_size = 50
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
                    print(f"     응답: {response.text[:200]}...")
                    
            except Exception as e:
                print(f"  ❌ 배치 {batch_num} 오류: {e}")
        
        print(f"🎉 총 {success_count}개 행이 Supabase에 저장되었습니다!")
        return success_count > 0
        
    except Exception as e:
        print(f"❌ Supabase upsert 실패: {e}")
        return False

def main():
    load_dotenv()
    
    # Supabase 설정
    print("🔗 Supabase 연결 설정 중...")
    supabase_config = setup_supabase()
    if not supabase_config:
        return
    print("✅ Supabase 연결 정보 설정 완료")
    
    csv_file = 'orders_dok.csv'
    
    print(f"📊 우커머스 독일어 CSV 파일 변환 시작: {csv_file}")
    
    supabase_data = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            
            for i, row in enumerate(reader, 1):
                # 우커머스 CSV 데이터를 Supabase 형식으로 변환
                order_number = row.get('Order Number', '').strip()
                order_status = row.get('Order Status', '').strip()
                paid_date = row.get('Paid Date', '').strip()
                full_name = row.get('Full Name (Billing)', '').strip()
                customer_email = row.get('Customer User Email', '').strip()
                phone = row.get('Phone (Billing)', '').strip()
                item_name = row.get('Item Name', '').strip()
                quantity = row.get('Quantity', '1').strip()
                item_cost = row.get('Item Cost', '0').strip()
                discount_amount = row.get('Discount Amount', '0').strip()
                
                if not order_number:
                    continue
                
                # 가격 계산: Item Cost + Discount Amount
                try:
                    item_cost_num = int(item_cost) if item_cost else 0
                    discount_num = int(discount_amount) if discount_amount else 0
                    prod_price = item_cost_num + discount_num
                except ValueError:
                    item_cost_num = 0
                    discount_num = 0
                    prod_price = 0
                
                # order_code 생성 (WordPress 주문번호 기반, get_orders_wp.py와 동일한 형식)
                order_code = f"w{order_number}"
                
                # prod_no는 실제 상품 ID 237513 사용 (CSV의 모든 상품이 동일한 상품 ID)
                prod_no = '237513'
                
                supabase_row = {
                    'order_code': order_code,
                    'order_no': order_number,
                    'order_time': convert_woocommerce_datetime_to_kst(paid_date),
                    'order_type': 'shopping',
                    'orderer_name': full_name,
                    'orderer_email': customer_email,
                    'orderer_phone': format_phone_number(phone),
                    'delivery_name': '',
                    'delivery_phone': '',
                    'delivery_postcode': '',
                    'delivery_address': '',
                    'delivery_address_detail': '',
                    'prod_no': prod_no,
                    'prod_name': item_name,
                    'prod_quantity': int(quantity) if quantity else 1,
                    'prod_price': prod_price,  # Item Cost + Discount Amount
                    'prod_discount_amount': item_cost_num,  # 실제 결제 금액
                    'order_status': order_status,
                    'payment_type': '',
                    'order_total_amount': prod_price,
                    'order_discount_amount': discount_num,
                    'delivery_fee': 0,
                    'coupon_discount': discount_num,
                    'point_used': 0,
                    'order_payment_amount': item_cost_num,  # 실제 결제 금액
                    'payment_time': convert_woocommerce_datetime_to_kst(paid_date),
                    'complete_time': convert_woocommerce_datetime_to_kst(paid_date),
                    'device_type': '',
                    'is_gift': 'N'
                }
                
                supabase_data.append(supabase_row)
                
                if i % 50 == 0:
                    print(f"  📊 {i}개 행 변환 완료...")
        
        print(f"📈 우커머스 독일어 CSV 변환 완료: {len(supabase_data)}개 행")
        
        # 변환된 데이터 샘플 출력
        if supabase_data:
            print(f"\n📋 변환된 데이터 샘플:")
            sample = supabase_data[0]
            print(f"   주문번호: {sample['order_no']}")
            print(f"   주문자: {sample['orderer_name']}")
            print(f"   전화번호: {sample['orderer_phone']}")
            print(f"   상품명: {sample['prod_name']}")
            print(f"   상품가격: {sample['prod_price']}원 (원가 {sample['order_payment_amount']} + 할인 {sample['coupon_discount']})")
            print(f"   주문일시: {sample['order_time']}")
        
        # Supabase에 저장
        if supabase_data:
            success = upsert_to_supabase(supabase_config, supabase_data)
            
            if success:
                print("✅ 우커머스 독일어 데이터가 Supabase에 저장되었습니다!")
                print("🔗 Supabase 대시보드에서 데이터를 확인하실 수 있습니다.")
                
                # 통계 출력
                print(f"\n📊 저장 통계:")
                print(f"   총 주문 수: {len(set(row['order_no'] for row in supabase_data))}개")
                print(f"   총 상품 행: {len(supabase_data)}개")
                
                # 상태별 통계
                status_counts = {}
                for row in supabase_data:
                    status = row['order_status']
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                print(f"\n📈 주문 상태별 통계:")
                for status, count in status_counts.items():
                    print(f"   {status}: {count}개")
                
            else:
                print("❌ Supabase 저장에 실패했습니다.")
        else:
            print("⚠️ 변환할 데이터가 없습니다.")
            
    except Exception as e:
        print(f"❌ CSV 변환 중 오류: {e}")

if __name__ == "__main__":
    main()
