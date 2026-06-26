"""종가 데이터 백필 스크립트.

evening-task가 그날 실패하면 trading.db에 종가가 비어 다음날 morning-task가
같은 계산을 중복 제출하는 문제가 있어, evening-task 2시간 뒤 최근 7일치를
다시 받아 INSERT OR REPLACE로 채워주는 안전망으로 매일 실행됨.
"""
import sqlite3
import logging
import os
from datetime import datetime, timedelta
from pytz import timezone
from kis_api import KISApi
from utils import load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def backfill(symbol: str, start_date: str, end_date: str):
    """주어진 범위의 일봉을 받아 DB에 저장.
    
    Args:
        symbol: 'SOXL'
        start_date, end_date: 'YYYYMMDD' 형식
    """
    logging.info(f'Backfill {symbol}: {start_date} ~ {end_date}')
    
    kis = KISApi()
    price_data = kis.get_overseas_price_daily(symbol, start_date, end_date)
    
    if not price_data:
        logging.error('가격 데이터 없음')
        return
    
    logging.info(f'KIS API에서 {len(price_data)}일치 받음')
    
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect('data/trading.db')
    cursor = conn.cursor()
    
    inserted = 0
    for d in price_data:
        cursor.execute('''
            INSERT OR REPLACE INTO prices
            (symbol, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (symbol, d['date'], d['open'], d['high'],
              d['low'], d['close'], d['volume']))
        inserted += 1
        logging.info(
            f"  {d['date']}: O=${d['open']:.2f} H=${d['high']:.2f} "
            f"L=${d['low']:.2f} C=${d['close']:.2f} V={d['volume']:,}"
        )
    
    conn.commit()
    conn.close()
    logging.info(f'완료: {inserted}일치 upsert')


if __name__ == '__main__':
    # 매일 실행되는 안전망: 최근 7일치를 다시 받아 누락된 종가를 채움
    # (이미 있는 날짜는 INSERT OR REPLACE라 그대로 덮어써도 안전)
    config = load_config()
    symbol = config['trading']['symbol']

    et = timezone('US/Eastern')
    today = datetime.now(et).date()
    start_date = (today - timedelta(days=7)).strftime('%Y%m%d')
    end_date = today.strftime('%Y%m%d')

    backfill(symbol, start_date, end_date)
