import pandas as pd
from datetime import datetime, timedelta
from utils import get_data, round_half_up_to_two,  calculate_mdd
from IPython.display import display

# 떨사오팔 매매 로직

def infinite_buy_simulation(df, df_res, initial_funds, buy_portion, start_idx, simulation_period,fee,welfare):
    funds = initial_funds # 초기 자금
    one_buy_amount = initial_funds / buy_portion # 회차별 매수금액
    holdings = 0 # 보유 주식 수
    buy_records = []  # 각 매수 건을 저장하여 관리 (개별 매도 관리)
    trade_history = []  # 매도 기록 저장
    trade_id = 1  # 매수 회차별 ID (매수 구분)
    fee = (fee/100) # 수수료

    # 날짜별 거래기록 데이터 프레임
    df_res = pd.DataFrame(index=range(start_idx, start_idx + simulation_period + 1),
                         columns=['날짜', '시가', '고가', '종가', '등락율',
                                'LOC 매수', '수익 실현 매도', 'MOC 손절',
                                '보유 주식 수', '예수금', '총 평가액', '수익율(%)'])

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

        # 복리 투자 시
        T = len(buy_records)
        one_buy_welfare = funds / (buy_portion-T) if buy_portion>T else 0 # 복리투자 1회차 금액

        # 매도 로직
        if holdings > 0:
            new_buy_records = []  # 매도되지 않은 매수 건을 저장할 새 리스트
            total_sell = 0  # 당일 총 매도 수량

            for record in buy_records: # 보유 주식 순회
                record['days'] += 1  # 보유일 count
                # 각 매수 건별 매도 목표가
                #sell_price = record['buy_price'] * 1.005 # 한국투자증권 온라인 수수료 * 2
                sell_price = record['buy_price'] * (1+fee*2)  # 백테스트 기준 0.044% * 2

                if price >= sell_price:  # 수익 실현 매도 조건 충족
                    funds += record['quantity'] * price
                    funds -= (record['quantity'] * price)*fee # 수수료 차감
                    total_sell += record['quantity']
                    holdings -= record['quantity']
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
                        '수익률(%)': round_half_up_to_two((price/record['buy_price'] - 1) * 100)
                    })
                elif record['days'] >= 39:  # 40일 경과 시 손절
                    funds += record['quantity'] * price
                    funds -= (record['quantity'] * price)*fee # 수수료 차감
                    df_res.at[i, 'MOC 손절'] = record['quantity']
                    holdings -= record['quantity']
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
                        '수익률(%)': (price/record['buy_price'] - 1) * 100
                    })
                else:
                    new_buy_records.append(record)

            if total_sell > 0:
                df_res.at[i, '수익 실현 매도'] = total_sell

            buy_records = new_buy_records

        # 매수 로직
        if price <= prev_price: # 전날 종가 LOC 매수
            if welfare: # 복리 적용
                qty = int(one_buy_welfare / prev_price)
            else: # 단리 적용
                qty = int(one_buy_amount / prev_price)
            if T<buy_portion: # 수수료 적용한 금액이상이 남아있을때 매수
                holdings += qty
                funds -= (qty * price) * (1+fee) # 수수료 차감
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
        df_res.at[i, 'MDD'] = calculate_mdd(df_res['총 평가액'])

    final_value = funds + (holdings * float(df['Close'].iloc[start_idx + simulation_period]))
    final_mdd = calculate_mdd(df_res['총 평가액'])
    return round_half_up_to_two((final_value / initial_funds - 1) * 100), df_res, final_value, pd.DataFrame(trade_history), final_mdd


if __name__ == "__main__":
    start_date = '2025-03-01'
    end_date = '2025-11-29'
    initial_funds = 10000 # 초기 자금
    buy_portion = 7  # 회차 분할 수
    fee = 0.25 # 수수료(%)
    stock_item = 'SOXL'
    welfare = False
    welfare = True

    # 시작일 30일 전의 날짜
    start_date_dt = datetime.strptime(start_date,'%Y-%m-%d')
    start_date_before_30 = start_date_dt - timedelta(days=30)
    start_date_before_30 = start_date_before_30.strftime('%Y-%m-%d')

    # 종료일 다음 날 계산
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    next_day = end_date_dt + timedelta(days=1)
    end_day_next = next_day.strftime('%Y-%m-%d')

    # 30일 전 데이터부터 입력
    # ticker 값으로 종목 선택 가능 ex) TQQQ
    df = get_data(ticker=stock_item, start=start_date_before_30, end=end_day_next)
    df_length = len(df) - len(get_data(ticker=stock_item, start=start_date, end=end_day_next))

    df_res = pd.DataFrame(columns=['날짜', '시가', '고가', '종가', '등락율', 'LOC 매수', '수익 실현 매도', 'MOC 손절',
                                  '보유 주식 수', '예수금', '총 평가액', '수익율(%)', 'MDD'])

    # 시뮬레이션 실행
    return_rate, df_res, final_value, df_trades , mdd= infinite_buy_simulation(
        df, df_res, initial_funds, buy_portion, df_length, len(df)-1-df_length,fee, welfare)


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
    print(f'MDD: {mdd:.2f}%')
    print('='*80)
    
        # 날짜별 거래 현황 출력
    print("\n[일별 거래 현황]")
    df_res_style = df_res.style.format({
        '시가': '{:.2f}',
        '고가': '{:.2f}',
        '종가': '{:.2f}',
        'LOC 매수': '{:.0f}',
        '수익 실현 매도': '{:.0f}',
        'MOC 손절': '{:.0f}',
        '보유 주식 수': '{:.0f}',
        '예수금': '{:.2f}',
        '총 평가액': '{:.2f}'
    }).set_properties(**{'text-align': 'center'})
    
    df_res.to_csv(f'{start_date}_{end_date}_v2.csv', index=False, encoding='utf-8')