# test_backtest.py
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from utils import round_half_up_to_two, pointTopercent

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

# 침몰방지법 매매로직
def prevent_drown_down_simulation(df, df_res, initial_funds, buy_portion, start_idx, simulation_period,welfare, fee):
    funds = initial_funds # 초기 자금
    one_buy_amount = initial_funds / buy_portion # 회차별 매수금액
    holdings = 0 # 보유 주식 수
    buy_records = []  # 각 매수 건을 저장하여 관리 (개별 매도 관리)
    trade_history = []  # 매도 기록 저장
    trade_id = 1  # 매수 회차별 ID (매수 구분)
    fee = fee/100
    total_fee = 0

    T=0 # T값 = 보유 회차 수

    # 시작일 - 종료일 시뮬레이션 진행
    for i in range(start_idx, start_idx + simulation_period + 1):
        # 데이터 시리즈로 가져오기
        current_date = df.index[i]
        open_price = float(df['Open'].iloc[i]) #시가
        high_price = float(df['High'].iloc[i]) #고가
        close_price = float(df['Close'].iloc[i]) #종가
        return_val = float(df['Return'].iloc[i]) #등락율
        prev_price = float(df['Close'].iloc[i-1]) if i > 0 else close_price #전날 종가


        # 오늘 데이터
        df_res.at[i, '날짜'] = current_date
        df_res.at[i, '시가'] = open_price
        df_res.at[i, '고가'] = high_price
        df_res.at[i, '종가'] = close_price
        df_res.at[i, '등락율'] = f"{round_half_up_to_two(return_val)}%"

        # 당일 종가
        price = close_price

        # 매수/매도 칼럼 초기화
        df_res.at[i, 'LOC 매수'] = 0
        df_res.at[i, '수익 실현 매도'] = 0
        df_res.at[i, 'MOC 손절'] = 0

        '''
        V2
        매매로직 (*T값 6부터 안전모드)
        매수 : (6-maximumT)%
        매도 : (12.5-2*maximumT)%
        최대보유기간 : (30-3*maximumT)일
        '''
        maximumT=min(T,6) # T값 최대 5까지 적용
        start_T=T # 매매체결전 기준 T값 (해당일에 실제 적용되는 T값)
        loc_buy = (1.06-0.01*maximumT) # 매수 기준
        loc_sell = (1.125-0.02*maximumT) # 매도 기준
        '''
        *복리*
        복리적용 회차금액 = 예수금/(분할수-T)
        '''
        one_buy_welfare = funds / (buy_portion-start_T) if buy_portion>start_T else funds # 복리투자 (조건문 = divisionByZero 방지)

        buy_order_price= prev_price*loc_buy # 매수 주문 가격


        # 매도 로직
        if holdings > 0:
            new_buy_records = []  # 매도되지 않은 매수 건을 저장할 새 리스트
            total_sell = 0  # 당일 총 매도 수량

            for record in buy_records: # 보유 주식 순회
                record['days'] += 1  # 보유일 count
                # 각 매수 건별 매도 목표가
                sell_price = record['buy_price'] * loc_sell # 매도 기준

                if price >= sell_price:  # 수익 실현 매도 조건 충족
                    funds += record['quantity'] * price
                    funds -= (record['quantity'] * price) * fee # 수수료 차감
                    total_fee += (record['quantity'] * price) * fee # 총 수수료 계산
                    total_sell += record['quantity']
                    holdings -= record['quantity']
                    T-=1 # 회차수 -=1

                    # 거래 기록 저장
                    trade_history.append({
                        '회차': record['id'],
                        '매수일': record['buy_date'],
                        '매수가': record['buy_price'],
                        '매수수량': record['quantity'],
                        '매도일': current_date,
                        '매도가': price,
                        '매도수량': record['quantity'],
                        '보유기간': record['days'],
                        '수익률(%)': round_half_up_to_two((price/record['buy_price'] - 1) * 100),
                        '적용 모드': "투자모드" if (maximumT)<6 else "회복모드",
                        '적용 T':start_T
                    })
                elif record['days'] >= (30-maximumT*3):  # 최대 보유일수 경과시 MOC 매도
                    funds += record['quantity'] * price
                    funds -= (record['quantity'] * price) * fee # 수수료 차감
                    total_fee += record['quantity'] * price * fee # 총 수수료 계산
                    df_res.at[i, 'MOC 손절'] = record['quantity']
                    holdings -= record['quantity']
                    T-=1 # 회차수 -=1
                    # 손절 거래 기록 저장
                    trade_history.append({
                        '회차': record['id'],
                        '매수일': record['buy_date'],
                        '매수가': record['buy_price'],
                        '매수수량': record['quantity'],
                        '매도일': current_date,
                        '매도가': price,
                        '매도수량': record['quantity'],
                        '보유기간': record['days'],
                        '수익률(%)': (price/record['buy_price'] - 1) * 100,
                        '적용 모드': "투자모드" if (maximumT)<6 else "회복모드",
                        '적용 T':start_T
                    })
                else:
                    new_buy_records.append(record)

            if total_sell > 0:
                df_res.at[i, '수익 실현 매도'] = total_sell

            buy_records = new_buy_records

        # 매수 로직
        if price <= buy_order_price: # 매수 기준
            if welfare: # 복리적용
                qty = int(one_buy_welfare / (prev_price * loc_buy)) # 복리 적용된 금액만큼 매수
            else: # 복리적용 X
                qty = int(one_buy_amount / (prev_price * loc_buy)) # 원금 10분할
            if funds >= (qty * price)*(1+fee):
                holdings += qty
                funds -= qty * price
                funds -= (qty * price) * fee # 수수료 차감
                total_fee += (qty * price) * fee # 총 수수료 계산
                T +=1 # 회차수 +=1
                buy_records.append({
                    'id': trade_id,
                    'buy_date': current_date,
                    'buy_price': price,
                    'quantity': qty,
                    'days': 0,
                    'type': 'LOC 매수'
                })
                df_res.at[i, 'LOC 매수'] = qty
                trade_id += 1

        # 포트폴리오 상태 저장
        df_res.at[i, '보유 주식 수'] = holdings
        df_res.at[i, '예수금'] = funds
        df_res.at[i, '총 평가액'] = funds + (price * holdings)
        df_res.at[i, '수익율(%)'] = ((funds + (price * holdings)) / initial_funds - 1) * 100
        df_res.at[i, 'T값'] = T
        df_res.at[i, '모드'] = "회복" if T>=6 else "투자"
        df_res.at[i, '총 수수료($)'] = round_half_up_to_two(total_fee)

    final_value = funds + (holdings * float(df['Close'].iloc[start_idx + simulation_period]))
    return round_half_up_to_two((final_value / initial_funds - 1) * 100), df_res, final_value, pd.DataFrame(trade_history) ,  total_fee

if __name__ == "__main__":
    start_date = '2025-01-01'
    end_date = '2025-11-12'
    initial_funds = 100000 # 초기 자금
    '''
    추천 분할수 8-10
    '''
    buy_portion = 8  # 회차 분할 수
    #welfare = False # 복리적용X
    welfare = True # 복리적용
    fee = 0.25 # 수수료(%)
    stock_name = 'SOXL' # 종목명

    # 시작일
    start_date_dt = datetime.strptime(start_date,'%Y-%m-%d')
    start_date_before_30 = start_date_dt - timedelta(days=30) # 시작일 30일 전
    start_date_before_30 = start_date_before_30.strftime('%Y-%m-%d')

    # 종료일 다음 날 계산
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    next_day = end_date_dt + timedelta(days=1)
    end_day_next = next_day.strftime('%Y-%m-%d')

    # ticker 값으로 종목 선택 가능 ex) TQQQ
    df = get_data(ticker=stock_name, start=start_date_before_30, end=end_day_next) # 시작일 30일 전 데이터부터 가져오기
    df_length = len(df) - len(get_data(ticker=stock_name, start=start_date, end=end_day_next))
    df_res = pd.DataFrame(columns=['날짜', '시가', '고가', '종가', '등락율', 'LOC 매수', '수익 실현 매도', 'MOC 손절',
                                  '보유 주식 수', '예수금', '총 평가액', '수익율(%)', 'T값',"모드",'총 수수료($)'])

    # 시뮬레이션 실행
    return_rate, df_res, final_value, df_trades , total_fee= prevent_drown_down_simulation(
        df, df_res, initial_funds, buy_portion, df_length, len(df)-1-df_length, welfare , fee)

    # 매매 통계 출력
    print('\n' + '='*80)
    print("매매 통계")
    print('='*80)
    print(f"총 매매 횟수: {len(df_trades)} 회")
    print(f"평균 보유기간: {df_trades['보유기간'].mean():.1f} 일")
    print(f"평균 수익률: {df_trades['수익률(%)'].mean():.2f}%")
    win_rate = len(df_trades[df_trades['수익률(%)'] > 0]) / len(df_trades) * 100
    print(f"승률: {win_rate:.2f}%")

    print('\n' + '='*80)
    print(f"{start_date} ~ {end_date} 동안의 자산 변동 결과")
    print('='*80)
    print(f"최초 보유 금액: ${initial_funds:,.2f}")
    print(f"최종 보유 금액: ${final_value:,.2f}")
    print(f"원금 변화율: {return_rate}%")
    print(f"총 수수료 : ${total_fee:.2f}")
    print('='*80)