/**
 * 우주캠퍼스 주문 데이터 Google Sheets 동기화
 * Supabase uzu_orders 테이블의 모든 데이터를 Google Sheets에 저장합니다.
 */

// Google Sheets ID (URL에서 추출)
const SHEET_ID = '190R2YQwHnl2MA3svD5Z1g_BGLtgIOmjtars18q5hFWQ';
const SHEET_NAME = 'orders'; // 실제 시트 탭 이름으로 변경하세요

/**
 * 저장된 키 가져오기
 */
function getSupabaseConfig() {
  const userProps = PropertiesService.getUserProperties();
  const supabaseUrl = userProps.getProperty("SUPABASE_URL");
  const supabaseKey = userProps.getProperty("SUPABASE_KEY");
  
  if (!supabaseUrl || !supabaseKey) {
    throw new Error('❌ Supabase 키가 설정되지 않았습니다. saveKeys() 함수를 먼저 실행하세요.');
  }
  
  return {
    url: supabaseUrl,
    key: supabaseKey
  };
}

/**
 * 메인 함수: Supabase 데이터를 Google Sheets에 동기화
 */
function syncSupabaseToSheets() {
  try {
    console.log('🚀 Supabase → Google Sheets 동기화 시작');
    
    // 1. Supabase에서 모든 데이터 가져오기
    const orders = fetchAllOrdersFromSupabase();
    console.log(`📊 Supabase에서 ${orders.length}개 행 조회 완료`);
    
    // 2. Google Sheets에 데이터 저장
    writeToGoogleSheets(orders);
    
    console.log('✅ 동기화 완료!');
    console.log(`🔗 확인: https://docs.google.com/spreadsheets/d/${SHEET_ID}`);
    
  } catch (error) {
    console.error('❌ 동기화 실패:', error);
    throw error;
  }
}

/**
 * Supabase에서 모든 주문 데이터를 가져옵니다
 */
function fetchAllOrdersFromSupabase() {
  const config = getSupabaseConfig();
  const allOrders = [];
  let offset = 0;
  const limit = 2000;
  
  while (true) {
    const url = `${config.url}/rest/v1/uzu_orders?offset=${offset}&limit=${limit}&order=id.asc`;
    
    const options = {
      method: 'GET',
      headers: {
        'apikey': config.key,
        'Authorization': `Bearer ${config.key}`,
        'Content-Type': 'application/json'
      }
    };
    
    try {
      const response = UrlFetchApp.fetch(url, options);
      
      if (response.getResponseCode() !== 200) {
        throw new Error(`Supabase API 오류: ${response.getResponseCode()}`);
      }
      
      const batch = JSON.parse(response.getContentText());
      
      if (batch.length === 0) {
        break; // 더 이상 데이터가 없음
      }
      
      allOrders.push(...batch);
      console.log(`   오프셋 ${offset}: ${batch.length}개 행, 누적: ${allOrders.length}개`);
      
      if (batch.length < limit) {
        break; // 마지막 배치
      }
      
      offset += limit;
      
    } catch (error) {
      console.error(`Supabase 조회 오류 (오프셋 ${offset}):`, error);
      throw error;
    }
  }
  
  return allOrders;
}

/**
 * Google Sheets에 데이터를 저장합니다
 */
function writeToGoogleSheets(orders) {
  try {
    // 스프레드시트 열기
    const sheet = SpreadsheetApp.openById(SHEET_ID).getSheetByName(SHEET_NAME);
    
    if (!sheet) {
      throw new Error(`시트를 찾을 수 없습니다: ${SHEET_NAME}`);
    }
    
    // 덮어쓰기 방식: 기존 데이터 삭제하지 않고 덮어씀
    console.log('🔄 덮어쓰기 방식으로 데이터 업데이트 중...');
    
    if (orders.length === 0) {
      console.log('⚠️ 저장할 데이터가 없습니다.');
      return;
    }
    
    // 헤더 확인 및 설정
    const headers = getSheetHeaders();
    const existingHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    
    // 헤더가 비어있거나 다르면 새로 설정
    if (existingHeaders.length === 0 || existingHeaders[0] === '') {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      console.log('📋 헤더 설정 완료');
    }
    
    // 데이터 변환 및 덮어쓰기
    const sheetData = convertOrdersToSheetData(orders, headers);
    
    if (sheetData.length > 0) {
      // 전체 데이터를 한 번에 덮어쓰기 (헤더 제외하고 2행부터)
      const startRow = 2;
      const numRows = sheetData.length;
      const numCols = headers.length;
      
      try {
        // 기존 데이터 영역보다 새 데이터가 적으면 남은 행 삭제
        const currentLastRow = sheet.getLastRow();
        const newLastRow = startRow + numRows - 1;
        
        if (currentLastRow > newLastRow) {
          const rowsToDelete = currentLastRow - newLastRow;
          sheet.deleteRows(newLastRow + 1, rowsToDelete);
          console.log(`🗑️ 남은 기존 행 삭제: ${rowsToDelete}개`);
        }
        
        // 새 데이터 덮어쓰기
        sheet.getRange(startRow, 1, numRows, numCols).setValues(sheetData);
        console.log(`🎉 총 ${numRows}개 행이 Google Sheets에 덮어쓰기 완료!`);
        
      } catch (error) {
        console.error('덮어쓰기 실패:', error);
        throw error;
      }
    }
    
  } catch (error) {
    console.error('Google Sheets 저장 오류:', error);
    throw error;
  }
}

/**
 * Google Sheets 헤더 정의 (Supabase 컬럼과 매핑)
 */
function getSheetHeaders() {
  return [
    'id',
    'order_code',
    'order_no',
    'order_time',
    'order_type',
    'orderer_name',
    'orderer_email',
    'orderer_phone',
    'delivery_name',
    'delivery_phone',
    'delivery_postcode',
    'delivery_address',
    'delivery_address_detail',
    'prod_no',
    'prod_name',
    'prod_quantity',
    'prod_price',
    'prod_discount_amount',
    'payment_type',
    'order_total_amount',
    'order_discount_amount',
    'delivery_fee',
    'coupon_discount',
    'point_used',
    'order_payment_amount',
    'payment_time',
    'complete_time',
    'device_type',
    'is_gift',
    'created_at',
    'updated_at',
    'order_status'
  ];
}

/**
 * Supabase 데이터를 Google Sheets 형식으로 변환
 */
function convertOrdersToSheetData(orders, headers) {
  return orders.map(order => {
    return headers.map(header => {
      const value = order[header];
      
      // null 값 처리
      if (value === null || value === undefined) {
        return '';
      }
      
      // 날짜 형식 처리
      if (header.includes('time') || header.includes('_at')) {
        if (value) {
          try {
            return new Date(value).toLocaleString('ko-KR', {
              timeZone: 'Asia/Seoul',
              year: 'numeric',
              month: '2-digit',
              day: '2-digit',
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit'
            });
          } catch (e) {
            return value;
          }
        }
        return '';
      }
      
      // 숫자 형식 처리
      if (typeof value === 'number') {
        return value;
      }
      
      // 문자열 처리
      return String(value);
    });
  });
}

/**
 * 특정 날짜 범위의 주문만 동기화 (옵션)
 */
function syncOrdersByDateRange(startDate, endDate) {
  try {
    console.log(`🗓️ 날짜 범위 동기화: ${startDate} ~ ${endDate}`);
    
    const config = getSupabaseConfig();
    const url = `${config.url}/rest/v1/uzu_orders?order_time=gte.${startDate}&order_time=lte.${endDate}&order=order_time.desc`;
    
    const options = {
      method: 'GET',
      headers: {
        'apikey': config.key,
        'Authorization': `Bearer ${config.key}`,
        'Content-Type': 'application/json'
      }
    };
    
    const response = UrlFetchApp.fetch(url, options);
    
    if (response.getResponseCode() !== 200) {
      throw new Error(`Supabase API 오류: ${response.getResponseCode()}`);
    }
    
    const orders = JSON.parse(response.getContentText());
    console.log(`📊 ${orders.length}개 주문 조회 완료`);
    
    // 기존 동기화 함수 재사용
    writeToGoogleSheets(orders);
    
  } catch (error) {
    console.error('날짜 범위 동기화 실패:', error);
    throw error;
  }
}

/**
 * 일일 업데이트: 새 주문 + 취소 상태 변경 업데이트
 * (전날 15:00 ~ 당일 15:30 범위의 updated_at 기준)
 */
function syncDailyUpdates() {
  try {
    console.log('⏰ 일일 업데이트 동기화 시작');
    
    // 전날 15:00 ~ 당일 15:30 범위 계산 (KST 기준)
    // Google Apps Script는 UTC로 동작하므로 KST로 변환 필요 (UTC+9)
    const nowUTC = new Date();
    const nowKST = new Date(nowUTC.getTime() + 9 * 60 * 60 * 1000); // UTC + 9시간 = KST
    
    const today330pmKST = new Date(nowKST.getFullYear(), nowKST.getMonth(), nowKST.getDate(), 15, 30, 0); // 당일 15:30 KST
    const yesterday3pmKST = new Date(nowKST.getFullYear(), nowKST.getMonth(), nowKST.getDate() - 1, 15, 0, 0); // 전날 15:00 KST
    
    // KST 시간을 다시 UTC로 변환 (Supabase는 UTC로 저장)
    const today330pm = new Date(today330pmKST.getTime() - 9 * 60 * 60 * 1000);
    const yesterday3pm = new Date(yesterday3pmKST.getTime() - 9 * 60 * 60 * 1000);
    const now = nowKST;
    
    // 현재 시간이 15:30 이전이면 어제 기준으로 계산
    let startTime, endTime;
    if (now < today330pm) {
      // 현재 시간이 15:30 이전 → 어제 기준
      endTime = new Date(yesterday3pm.getTime() + 24 * 60 * 60 * 1000 + 30 * 60 * 1000); // 어제 15:30
      startTime = yesterday3pm; // 그저께 15:00
    } else {
      // 현재 시간이 15:30 이후 → 오늘 기준
      endTime = today330pm; // 당일 15:30
      startTime = yesterday3pm; // 전날 15:00
    }
    
    const startISO = startTime.toISOString();
    const endISO = endTime.toISOString();
    
    console.log(`📅 업데이트 범위: ${startTime.toLocaleString('ko-KR', {timeZone: 'Asia/Seoul'})} ~ ${endTime.toLocaleString('ko-KR', {timeZone: 'Asia/Seoul'})} (KST)`);
    
    // updated_at 기준으로 최근 변경된 데이터 조회
    const config = getSupabaseConfig();
    const url = `${config.url}/rest/v1/uzu_orders?updated_at=gte.${startISO}&updated_at=lte.${endISO}&order=updated_at.desc`;
    
    const options = {
      method: 'GET',
      headers: {
        'apikey': config.key,
        'Authorization': `Bearer ${config.key}`,
        'Content-Type': 'application/json'
      }
    };
    
    const response = UrlFetchApp.fetch(url, options);
    
    if (response.getResponseCode() !== 200) {
      throw new Error(`Supabase API 오류: ${response.getResponseCode()}`);
    }
    
    const updatedOrders = JSON.parse(response.getContentText());
    console.log(`📊 ${updatedOrders.length}개 업데이트된 주문 조회 완료`);
    
    if (updatedOrders.length === 0) {
      console.log('📋 업데이트할 데이터가 없습니다.');
      return;
    }
    
    // 상태별 통계 및 상세 로그
    const statusCount = {};
    const statusDetails = {};
    
    updatedOrders.forEach(order => {
      const status = order.order_status || 'UNKNOWN';
      const orderNo = order.order_no || '';
      const ordererName = order.orderer_name || '';
      const prodName = order.prod_name || '';
      
      statusCount[status] = (statusCount[status] || 0) + 1;
      
      if (!statusDetails[status]) {
        statusDetails[status] = [];
      }
      
      statusDetails[status].push({
        orderNo: orderNo,
        ordererName: ordererName,
        prodName: prodName,
        updatedAt: order.updated_at
      });
    });
    
    console.log('📈 업데이트된 주문 상태별 통계:');
    Object.entries(statusCount).forEach(([status, count]) => {
      console.log(`   ${status}: ${count}개`);
    });
    
    console.log('\n📋 업데이트된 주문 상세 내역:');
    Object.entries(statusDetails).forEach(([status, orders]) => {
      console.log(`\n🔸 ${status} 상태 (${orders.length}개):`);
      orders.slice(0, 10).forEach((order, index) => { // 최대 10개만 표시
        console.log(`   ${index + 1}. 주문번호: ${order.orderNo} | 주문자: ${order.ordererName} | 상품: ${order.prodName}`);
      });
      if (orders.length > 10) {
        console.log(`   ... 외 ${orders.length - 10}개 더`);
      }
    });
    
    // 기존 시트 데이터와 병합하여 업데이트
    updateSheetWithChangedOrders(updatedOrders);
    
    console.log('✅ 일일 업데이트 완료!');
    
    // 자동으로 상품별 시트 동기화 실행 (업데이트된 데이터만 전달)
    console.log('\n🔗 상품별 시트 동기화 시작...');
    try {
      // Sync.gs의 함수 호출 (업데이트된 데이터를 직접 전달)
      if (typeof syncAllProductsWithData === 'function') {
        syncAllProductsWithData(updatedOrders);
        console.log('✅ 상품별 시트 동기화 완료!');
      } else if (typeof syncAllProducts === 'function') {
        // 기존 함수 사용 (전체 데이터 다시 조회)
        console.log('📋 전체 데이터 기반 상품별 동기화 실행...');
        syncAllProducts();
        console.log('✅ 상품별 시트 동기화 완료!');
      } else {
        console.warn('⚠️ Sync.gs 함수를 찾을 수 없습니다. Sync.gs 파일이 같은 프로젝트에 있는지 확인하세요.');
      }
    } catch (error) {
      console.error('❌ 상품별 시트 동기화 실패:', error);
    }
    
    // 자동으로 학습 완료된 주문 삭제 실행
    console.log('\n🗑️ 학습 완료된 주문 삭제 시작...');
    try {
      // Delete.gs의 함수 호출
      if (typeof deleteCompletedStudyOrders === 'function') {
        deleteCompletedStudyOrders();
        console.log('✅ 학습 완료 주문 삭제 완료!');
      } else {
        console.warn('⚠️ deleteCompletedStudyOrders 함수를 찾을 수 없습니다. Delete.gs 파일이 같은 프로젝트에 있는지 확인하세요.');
      }
    } catch (error) {
      console.error('❌ 학습 완료 주문 삭제 실패:', error);
    }
    
  } catch (error) {
    console.error('❌ 일일 업데이트 실패:', error);
    throw error;
  }
}

/**
 * 변경된 주문 데이터로 시트를 부분 업데이트
 */
function updateSheetWithChangedOrders(updatedOrders) {
  try {
    const sheet = SpreadsheetApp.openById(SHEET_ID).getSheetByName(SHEET_NAME);
    
    if (!sheet) {
      throw new Error(`시트를 찾을 수 없습니다: ${SHEET_NAME}`);
    }
    
    // 기존 시트 데이터 읽기
    const lastRow = sheet.getLastRow();
    if (lastRow <= 1) {
      console.log('📋 기존 데이터가 없어 전체 동기화를 실행합니다.');
      writeToGoogleSheets(updatedOrders);
      return;
    }
    
    // 헤더와 기존 데이터 읽기
    const headers = getSheetHeaders();
    const existingData = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
    
    console.log(`🔍 기존 시트 데이터: ${existingData.length}개 행`);
    
    // order_no 기준으로 기존 데이터 맵핑
    const existingOrderMap = {};
    const orderNoIndex = headers.indexOf('order_no');
    const idIndex = headers.indexOf('id');
    
    existingData.forEach((row, index) => {
      const orderNo = row[orderNoIndex];
      const id = row[idIndex];
      if (orderNo && id) {
        existingOrderMap[id] = {
          rowIndex: index + 2, // 시트 행 번호 (1-based, 헤더 제외)
          data: row
        };
      }
    });
    
    // 변경된 주문 데이터 업데이트
    const sheetData = convertOrdersToSheetData(updatedOrders, headers);
    let updatedCount = 0;
    let newCount = 0;
    
    const updatedDetails = [];
    const newDetails = [];
    
    sheetData.forEach((newRowData, index) => {
      const id = newRowData[idIndex];
      const orderNo = newRowData[headers.indexOf('order_no')] || '';
      const ordererName = newRowData[headers.indexOf('orderer_name')] || '';
      const orderStatus = newRowData[headers.indexOf('order_status')] || '';
      
      if (existingOrderMap[id]) {
        // 기존 데이터 업데이트
        const rowIndex = existingOrderMap[id].rowIndex;
        sheet.getRange(rowIndex, 1, 1, headers.length).setValues([newRowData]);
        updatedCount++;
        
        updatedDetails.push({
          orderNo: orderNo,
          ordererName: ordererName,
          status: orderStatus,
          rowIndex: rowIndex
        });
      } else {
        // 새 데이터 추가 (시트 끝에)
        sheet.getRange(lastRow + 1 + newCount, 1, 1, headers.length).setValues([newRowData]);
        newCount++;
        
        newDetails.push({
          orderNo: orderNo,
          ordererName: ordererName,
          status: orderStatus,
          rowIndex: lastRow + 1 + newCount
        });
      }
    });
    
    console.log(`🔄 시트 업데이트 완료: 수정 ${updatedCount}개, 신규 ${newCount}개`);
    
    // 업데이트된 항목 상세 로그
    if (updatedDetails.length > 0) {
      console.log('\n📝 시트에서 업데이트된 주문:');
      updatedDetails.slice(0, 10).forEach((detail, index) => {
        console.log(`   ${index + 1}. 행${detail.rowIndex}: ${detail.orderNo} | ${detail.ordererName} | ${detail.status}`);
      });
      if (updatedDetails.length > 10) {
        console.log(`   ... 외 ${updatedDetails.length - 10}개 더`);
      }
    }
    
    // 새로 추가된 항목 상세 로그
    if (newDetails.length > 0) {
      console.log('\n➕ 시트에 새로 추가된 주문:');
      newDetails.slice(0, 10).forEach((detail, index) => {
        console.log(`   ${index + 1}. 행${detail.rowIndex}: ${detail.orderNo} | ${detail.ordererName} | ${detail.status}`);
      });
      if (newDetails.length > 10) {
        console.log(`   ... 외 ${newDetails.length - 10}개 더`);
      }
    }
    
  } catch (error) {
    console.error('시트 부분 업데이트 실패:', error);
    throw error;
  }
}

/**
 * 수동 실행용 함수들
 */
function runFullSync() {
  try {
    // 1. 전체 Supabase 데이터 동기화
    syncSupabaseToSheets();
    
    // 2. 자동으로 상품별 시트 전체 동기화 실행
    console.log('\n🔗 상품별 시트 전체 동기화 시작...');
    try {
      // Sync.gs의 전체 동기화 함수 호출
      if (typeof syncAllProducts === 'function') {
        syncAllProducts();
        console.log('✅ 상품별 시트 전체 동기화 완료!');
      } else {
        console.warn('⚠️ syncAllProducts 함수를 찾을 수 없습니다. Sync.gs 파일이 같은 프로젝트에 있는지 확인하세요.');
      }
    } catch (error) {
      console.error('❌ 상품별 시트 전체 동기화 실패:', error);
    }
    
    console.log('🎉 전체 동기화 (메인 + 상품별) 완료!');
    
  } catch (error) {
    console.error('❌ 전체 동기화 실패:', error);
    throw error;
  }
}

function runDailySync() {
  syncDailyUpdates();
}

function runTodaySync() {
  const today = new Date();
  const startDate = new Date(today.getFullYear(), today.getMonth(), today.getDate()).toISOString();
  const endDate = new Date(today.getFullYear(), today.getMonth(), today.getDate() + 1).toISOString();
  
  syncOrdersByDateRange(startDate, endDate);
}

/**
 * 테스트 함수 (소량 데이터로 테스트)
 */
function testSync() {
  try {
    console.log('🧪 테스트 동기화 시작 (최근 10개 주문)');
    
    const config = getSupabaseConfig();
    const url = `${config.url}/rest/v1/uzu_orders?limit=10&order=id.desc`;
    
    const options = {
      method: 'GET',
      headers: {
        'apikey': config.key,
        'Authorization': `Bearer ${config.key}`,
        'Content-Type': 'application/json'
      }
    };
    
    const response = UrlFetchApp.fetch(url, options);
    
    if (response.getResponseCode() !== 200) {
      throw new Error(`Supabase API 오류: ${response.getResponseCode()}`);
    }
    
    const orders = JSON.parse(response.getContentText());
    console.log(`📊 테스트용 ${orders.length}개 주문 조회`);
    
    // 테스트용으로 별도 시트나 범위에 저장할 수 있음
    writeToGoogleSheets(orders);
    
    console.log('✅ 테스트 완료!');
    
  } catch (error) {
    console.error('❌ 테스트 실패:', error);
    throw error;
  }
}
