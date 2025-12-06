# daily_run.py
import sqlite3
import pandas as pd
import yaml
import pickle
import os
import logging
from datetime import datetime, timedelta
import pandas_market_calendars as mcal
from pytz import timezone
from kis_api import KISApi
from utils import round_half_up_to_two, pointTopercent, get_data
from backtest_today import infinite_buy_today


# logs 폴더 생성
os.makedirs('logs', exist_ok=True)

class DailyTrader:
    def __init__(self, config_path='config.yaml', mode='dry-run'):
        """
        Args:
            mode: 'dry-run', 'live', 'update-only'
        """
        self.mode = mode
        self.setup_logging()
        self.load_config(config_path)
        self.kis = KISApi()
        self.nyse = mcal.get_calendar('NYSE')
        
    def setup_logging(self):
        """로깅 설정"""
        log_filename = f'logs/trading_{datetime.now().strftime("%Y%m%d")}.log'
        
        # 기존 핸들러 제거
        logger = logging.getLogger()
        logger.handlers = []
        
        # 새 핸들러 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        
    def load_config(self, config_path):
        """설정 파일 로드"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.symbol = self.config['trading']['symbol']
        self.initial_funds = self.config['trading']['initial_funds']
        self.buy_portion = self.config['trading']['buy_portion']
        self.fee = self.config['trading']['fee_rate']
        self.welfare = True
        self.start_date = self.config['trading']['start_date']
    
    def get_us_date(self):
        """미국 동부시간 기준 날짜 반환"""
        et = timezone('US/Eastern')
        return datetime.now(et).date()
    
    def is_trading_day(self, date=None):
        """거래일 확인"""
        if date is None:
            date = self.get_us_date()
        
        schedule = self.nyse.schedule(start_date=date, end_date=date)
        return not schedule.empty
    
 
    
    def calculate_orders(self):
        """백테스트 로직으로 주문 계산"""
        end_date = self.get_us_date() - timedelta(days=1)
        # 시작일 30일 전의 날짜
        start_date_dt = datetime.strptime(self.start_date,'%Y-%m-%d')
        start_date_before_30 = start_date_dt - timedelta(days=30)
        start_date_before_30 = start_date_before_30.strftime('%Y-%m-%d')

        # 종료일 다음 날 계산
        #end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
        next_day = end_date + timedelta(days=1)
        real_end_day = next_day.strftime('%Y-%m-%d') # 실제 주문 넣는일

        # 30일 전 데이터부터 입력
        df = get_data(ticker=self.symbol, start=start_date_before_30, end=end_date)
        df_length = len(df) - len(get_data(ticker=self.symbol, start=self.start_date, end=end_date))

        # 오늘 투자 금액 계산
        buyToday,buyQty,funds,holdings,buy_records,sellToday = infinite_buy_today(
            df, self.initial_funds, self.buy_portion, df_length, len(df)-1-df_length, self.fee, self.welfare)
        
        # buy_records 저장
        with open('data/buy_records.pkl', 'wb') as f:
            pickle.dump(buy_records, f)
        
        logging.info(f"Order Calculation - Holdings: {holdings}, Funds: ${funds:.2f}")
        
        return buyToday, buyQty, sellToday
    
    def log_orders(self, buyPrice, buyQty, sellOrders):
        """주문 내역 파일 기록"""
        order_file = f'logs/orders_{self.get_us_date().strftime("%Y%m%d")}.txt'
        
        with open(order_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*50}\n")
            f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"US Date: {self.get_us_date()}\n")
            f.write(f"Mode: {self.mode.upper()}\n")
            f.write(f"Buy Order: {buyQty} shares @ ${buyPrice:.2f}\n")
            
            for order_type, price, qty in sellOrders:
                if order_type == 'LOC':
                    f.write(f"Sell Order (LOC): {qty} shares @ ${price:.2f}\n")
                else:
                    f.write(f"Sell Order (MOC): {qty} shares\n")
    
    def submit_orders(self, buyPrice, buyQty, sellOrders):
        """한투 API로 주문 제출"""
        results = []
        
        # 매수 주문
        if buyQty > 0:
            logging.info(f"Submitting LOC buy: {buyQty} @ ${buyPrice:.2f}")
            result = self.kis.place_order('LOC_BUY', self.symbol, buyQty, buyPrice)
            results.append(('BUY', result))
        
        # 매도 주문
        for order_type, price, qty in sellOrders:
            if order_type == 'LOC':
                logging.info(f"Submitting LOC sell: {qty} @ ${price:.2f}")
                result = self.kis.place_order('LOC_SELL', self.symbol, qty, price)
            else:
                logging.info(f"Submitting MOC sell: {qty} shares")
                result = self.kis.place_order('MOC_SELL', self.symbol, qty, 0)
            results.append(('SELL', result))
        
        # 결과 요약
        success = sum(1 for _, r in results if r.get('success'))
        logging.info(f"Order Results: {success}/{len(results)} successful")
        
    def update_price_data(self, target_date):
        """종가 데이터 업데이트"""
        logging.info(f"Updating price for {target_date}")
        
        date_str = target_date.strftime('%Y%m%d')
        
        # 한투 API로 종가 조회
        price_data = self.kis.get_overseas_price_daily(
            self.symbol, date_str, date_str
        )
        
        if not price_data:
            logging.warning(f"No price data for {target_date}")
            return False
        
        # DB 업데이트
        conn = sqlite3.connect('data/trading.db')
        cursor = conn.cursor()
        
        for data in price_data:
            cursor.execute('''
                INSERT OR REPLACE INTO prices 
                (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (self.symbol, data['date'], data['open'], 
                  data['high'], data['low'], data['close'], data['volume']))
        
        conn.commit()
        conn.close()
        
        logging.info(f"Price updated: {data['date']} - Close: ${data['close']:.2f}")
        return True
    
    def run_morning_task(self):
        """00:30 실행 - 주문 계산 및 제출"""
        logging.info("="*60)
        logging.info("MORNING TASK START")
        logging.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Mode: {self.mode.upper()}")
        logging.info("="*60)
        
        # 1. 미국 거래일 확인
        us_today = self.get_us_date()
        if not self.is_trading_day(us_today):
            logging.info(f"US Market Closed on {us_today}")
            return
        
        logging.info(f"US Market Open on {us_today}")
        
        # 2. 주문 계산
        try:
            buyPrice, buyQty, sellOrders = self.calculate_orders()
            if buyPrice is None:
                logging.error("Order calculation failed")
                return
            
            # 3. 주문 로그
            self.log_orders(buyPrice, buyQty, sellOrders)
            
            # 4. 실거래 모드일 때만 제출
            if self.mode == 'live':
                self.submit_orders(buyPrice, buyQty, sellOrders)
            else:
                logging.info(f"{self.mode.upper()} mode - Orders not submitted")
                
        except Exception as e:
            logging.error(f"Error in morning task: {e}", exc_info=True)
    
    def run_evening_task(self):
        """10:00 실행 - 종가 업데이트"""
        logging.info("="*60)
        logging.info("EVENING TASK START")
        logging.info(f"실행시간 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*60)
        
        # 미국 기준 어제 (장 마감 후)
        us_date = self.get_us_date()
        
        # 거래일인지 확인
        if not self.is_trading_day(us_date):
            logging.info(f"No trading on {us_date}")
            return
        
        # 종가 업데이트
        try:
            self.update_price_data(us_date)
        except Exception as e:
            logging.error(f"Error updating price: {e}", exc_info=True)