from utils import get_data, load_config
from datetime import timedelta, datetime


# 떨사오팔 실시간
def infinite_buy_today(df, initial_funds, buy_portion, start_idx, simulation_period,fee,welfare):
    funds = initial_funds # 초기 자금
    one_buy_amount = initial_funds / buy_portion # 회차별 매수금액
    holdings = 0 # 보유 주식 수
    buy_records = []  # 각 매수 건을 저장하여 관리 (개별 매도 관리)
    trade_id = 1  # 매수 회차별 ID (매수 구분)
    fee = (fee/100) # 수수료


    # 시작일 - 종료일 시뮬레이션 진행
    for i in range(start_idx, start_idx + simulation_period+1 ):
        # 데이터 시리즈로 가져오기
        current_date = df.index[i]
        close_price = float(df['Close'].iloc[i]) #종가
        prev_price = float(df['Close'].iloc[i-1]) if i > 0 else close_price #전날 종가

        # 당일 종가
        price = close_price

        # 복리 투자 시
        T = len(buy_records)
        one_buy_welfare = funds / (buy_portion-T) if buy_portion>T else funds # 복리투자 1회차 금액


        # 매도 로직
        if holdings > 0:
            new_buy_records = []  # 매도되지 않은 매수 건을 저장할 새 리스트
            total_sell = 0  # 당일 총 매도 수량

            for record in buy_records: # 보유 주식 순회
                record['days'] += 1  # 보유일 count
                # 각 매수 건별 매도 목표가
                sell_price = record['buy_price'] * (1+fee*2)  # 백테스트 기준 0.044% * 2

                if price >= sell_price:  # 수익 실현 매도 조건 충족
                    funds += record['quantity'] * price
                    funds -= (record['quantity'] * price)*fee # 수수료 차감
                    total_sell += record['quantity']
                    holdings -= record['quantity']
                elif record['days'] >= 40:  # 40일 경과 시 손절
                    funds += record['quantity'] * price
                    funds -= (record['quantity'] * price)*fee # 수수료 차감
                    holdings -= record['quantity']
                else:
                    new_buy_records.append(record)


            buy_records = new_buy_records

        # 매수 로직
        if price <= prev_price: # 전날 종가 LOC 매수
            if welfare: # 복리 적용
                qty = int(one_buy_welfare / prev_price)
            else: # 단리 적용
                qty = int(one_buy_amount / prev_price)
            if funds >= (qty * price) * (1+fee): # 수수료 적용한 금액이상이 남아있을때 매수
                holdings += qty
                funds -= (qty * price) * (1+fee) # 수수료 차감
                buy_records.append({
                    'id': trade_id,
                    'buy_date': current_date,
                    'buy_price': price,
                    'quantity': qty,
                    'days': 1,
                    'type': 'LOC 매수'
                })
                trade_id += 1
    # 복리 투자 시
    T = len(buy_records)
    one_buy_welfare = funds / (buy_portion-T) if buy_portion>T else funds # 복리투자 1회차 금액

    #매수주문
    buyToday= float(df['Close'].iloc[-1]) # 전일종가
    buyQty= int(one_buy_welfare / buyToday) if welfare else int(one_buy_amount / buyToday)

    #매도주문
    sellToday=[]
    for record in buy_records: # 보유 주식 순회
        if record['days']<39:
          sell_price = record['buy_price'] * (1+fee*2)
          sell_qty = record['quantity']
          sell_type = "LOC"
          sellToday.append((sell_type,sell_price, sell_qty))
        else:
          sell_price = 0
          sell_qty = record['quantity']
          sell_type = "MOC"
          sellToday.append((sell_type,sell_price, sell_qty))


    #final_value = funds + (holdings * float(df['Close'].iloc[start_idx + simulation_period])) # 평가액
    return buyToday, buyQty, funds, holdings, buy_records, sellToday



if __name__ == "__main__":
    '''
    파라미터 설정
    '''
    config = load_config()
    start_date = config['trading']['start_date']
    end_date = '2025-03-19' # 미국시간 기준으로 입력 (장중 날짜)
    initial_funds = config['trading']['initial_funds'] # 초기 자금
    buy_portion = config['trading']['buy_portion']  # 회차 분수
    fee = 0.25 # 수수료(%)
    stock_item = config['trading']['symbol']
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
    df = get_data(ticker=stock_item, start=start_date_before_30, end=end_date_dt)
    df_length = len(df) - len(get_data(ticker=stock_item, start=start_date, end=end_date_dt))

    # 오늘 투자 금액 계산
    buyToday,buyQty,funds,holdings,buy_records,sellToday = infinite_buy_today(
        df, initial_funds, buy_portion, df_length, len(df)-1-df_length,fee, welfare)

    #매매 통계 출력
    print('\n' + '='*80)
    print('='*30+f'{end_day_next}일 넣을 주문'+'='*35)
    print("<매수주문>")
    print(f'- (LOC매수):${buyToday}')
    print(f'- 수량:{buyQty}주')
    print()
    print("<매도주문>")
    if len(sellToday)>0:
      for selltype,price,qty in sellToday:
        # price=round_half_up_to_two(price)
        print(f'- ({selltype}매도):${price:.2f}')
        print(f'- 수량:{qty}주')
    else:
      print("매도할 수량이 없습니다.")

    print('='*80)
    print(f"{end_date} 기준 <포트폴리오>")
    print(f'남은예수금:${funds:.1f}')
    print(f'보유수량:{holdings}')
    print(f'초기자금:${initial_funds}')
    print(f'평가금액(전일종가 기준):${buyToday*holdings+funds:.2f}')
    print()
    print("<보유회차>")

    T=1
    for record in buy_records:
      print(f'<{T}회차>')
      date=record['buy_date']
      price=record['buy_price']
      qty=record['quantity']
      days=record['days']
      print(f'매수일:{date}')
      print(f'매수체결가:{price}')
      print(f'수량:{qty}')
      print(f'보유기간:{days}')
      T+=1