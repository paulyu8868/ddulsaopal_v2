# init_db.py
import sqlite3
import os

def create_tables():
    """DB 테이블 생성"""
    os.makedirs('data', exist_ok=True)
    conn = sqlite3.connect('data/trading.db')
    cursor = conn.cursor()
    
    # 가격 데이터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER,
            PRIMARY KEY (symbol, date)
        )
    ''')
    
    # 인덱스 생성 (조회 성능)
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_prices_date 
        ON prices(symbol, date)
    ''')
    
    conn.commit()
    conn.close()
    print("Database tables created successfully!")

if __name__ == "__main__":
    create_tables()