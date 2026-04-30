"""4월 빠진 데이터 백필 스크립트.

GitHub Actions에서 한 번 실행하고 삭제할 임시 스크립트.
INSERT OR REPLACE라 기존 데이터는 안 건드림.
"""
import sqlite3
import logging
import os
from datetime import datetime
from kis_api import KISApi

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
    # 4월 빠진 범위 백필
    # 04-09, 04-13~16, 04-20~23 (총 9 영업일)
    # 안전하게 04-09 ~ 04-29 전체 범위로 받음 (이미 있는 날은 INSERT OR REPLACE)
    backfill('SOXL', '20260409', '20260429')
