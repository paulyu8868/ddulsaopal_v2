import pandas as pd
import sqlite3
import yaml

def load_config():
    """설정 파일 로드"""
    config_path = 'config.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)
    
    # # 파라미터 설정
    # self.symbol = self.config['trading']['symbol']
    # self.initial_funds = self.config['trading']['initial_funds']
    # self.buy_portion = self.config['trading']['buy_portion']
    # self.fee = self.config['trading']['fee_rate']
    # self.welfare = self.config['trading']['welfare']
    # self.start_date = self.config['trading']['start_date']

# 소수 셋째자리에서 반올림
def round_half_up_to_two(num):
    try:
        if isinstance(num, (float, int)):
            num_100 = num * 100
            if num_100 - int(num_100) >= 0.5:
                return (int(num_100) + 1) / 100
            else:
                return int(num_100) / 100
        else:
            return num
    except:
        return num

# 퍼센트로 변환
def pointTopercent(num):
    return round_half_up_to_two(num*100)


def calculate_mdd(equity_curve):
    """MDD(Maximum Drawdown) 계산"""
    cummax = equity_curve.cummax()
    drawdown = (equity_curve - cummax) / cummax * 100
    mdd = drawdown.min()
    return mdd


def get_data(ticker, start, end):
    """DB에서 yfinance 형식의 DataFrame 생성"""
    conn = sqlite3.connect('data/trading.db')
    
    query = """
        SELECT date, open, high, low, close 
        FROM prices 
        WHERE symbol = ? AND date BETWEEN ? AND ?
        ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(ticker, start, end))
    conn.close()
    
    if df.empty:
        print("No data found in DB")
        return None
    
    # date를 인덱스로 설정
    df['date'] = pd.to_datetime(df['date']).dt.date
    df.set_index('date', inplace=True)
    
    # 컬럼명 대문자로 변경
    df.columns = ['Open', 'High', 'Low', 'Close']
    
    # 호가 단위 0.01$ 적용
    for col in ['Open', 'High', 'Low', 'Close']:
        df[col] = df[col].map(round_half_up_to_two)
    
    # 등락율 계산
    df['Return'] = df['Close'].pct_change()
    df['Return'] = df['Return'].map(pointTopercent)
    
    return df