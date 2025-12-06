# load_data.py
import sqlite3
from datetime import datetime, timedelta
from kis_api import KISApi
import sys

def load_historical_data(symbol, start_date, end_date):
    """한투 API로 데이터 다운로드 및 DB 저장"""
    print(f"Loading data for {symbol} from {start_date} to {end_date}")
    
    # KIS API 초기화
    kis = KISApi()
    
    # 날짜 형식 변환
    start_yyyymmdd = start_date.replace('-', '')
    end_yyyymmdd = end_date.replace('-', '')
    
    # 데이터 다운로드
    price_data = kis.get_overseas_price_daily(
        symbol, start_yyyymmdd, end_yyyymmdd
    )
    
    if not price_data:
        print("No data received")
        return False
    
    # DB에 저장
    conn = sqlite3.connect('data/trading.db')
    cursor = conn.cursor()
    
    for data in price_data:
        cursor.execute('''
            INSERT OR REPLACE INTO prices 
            (symbol, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, data['date'], data['open'], data['high'], 
              data['low'], data['close'], data['volume']))
    
    conn.commit()
    conn.close()
    
    print(f"Saved {len(price_data)} days of data")
    return True

if __name__ == "__main__":
    # 2년치 데이터 로드
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
    
    if load_historical_data('SOXL', start_date, end_date):
        print("Data loading complete!")
    else:
        print("Data loading failed!")