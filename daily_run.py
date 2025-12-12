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


class DailyTrader:
    def __init__(self, config_path='config.yaml', mode='dry-run'):
        """
        Args:
            mode: 'dry-run', 'live', 'update-only'
        """
        self.mode = mode
        self.load_config(config_path)
        self.setup_directories()
        self.setup_logging()
        self.kis = KISApi()
        self.nyse = mcal.get_calendar('NYSE')
        
    def setup_directories(self):
        """모드별 디렉토리 구조 생성"""
        # 모드별 로그 디렉토리
        self.log_base_dir = f'logs/{self.mode}'
        self.log_daily_dir = f'{self.log_base_dir}/daily'
        os.makedirs(self.log_daily_dir, exist_ok=True)
        os.makedirs('data', exist_ok=True)
        
        # 모드별 파일 경로
        self.history_log_path = f'{self.log_base_dir}/trading_history_{datetime.now().year}.log'
        self.orders_history_path = f'{self.log_base_dir}/orders_history.txt'
        self.buy_records_path = f'data/{self.mode}_buy_records.pkl'
        
    def setup_logging(self):
        """로깅 설정 - 날짜별 상세 로그"""
        log_filename = f'{self.log_daily_dir}/trading_{datetime.now().strftime("%Y%m%d")}.log'
        
        # 기존 핸들러 제거
        logger = logging.getLogger()
        logger.handlers = []
        logger.setLevel(logging.INFO)
        
        # 포매터
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # 파일 핸들러 (날짜별 상세 로그)
        file_handler = logging.FileHandler(log_filename, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
    def load_config(self, config_path):
        """설정 파일 로드"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.symbol = self.config['trading']['symbol']
        self.initial_funds = self.config['trading']['initial_funds']
        self.buy_portion = self.config['trading']['buy_portion']
        self.fee = self.config['trading']['fee_rate']
        self.welfare = self.config['trading'].get('welfare', True)
        self.start_date = self.config['trading']['start_date']
    
    def get_us_date(self):
        """미국 동부시간 기준 날짜 반환"""
        et = timezone('US/Eastern')
        return datetime.now(et).date()
    
    def get_kr_datetime(self):
        """한국시간 반환"""
        kst = timezone('Asia/Seoul')
        return datetime.now(kst)
    
    def is_trading_day(self, date=None):
        """거래일 확인"""
        if date is None:
            date = self.get_us_date()
        
        schedule = self.nyse.schedule(start_date=date, end_date=date)
        return not schedule.empty
    
    def get_weekday_kr(self, date):
        """요일 반환 (한글)"""
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        return weekdays[date.weekday()]
    
    def write_history_header(self):
        """통합 로그 헤더 작성 (파일이 없거나 비어있을 때)"""
        if os.path.exists(self.history_log_path) and os.path.getsize(self.history_log_path) > 0:
            return  # 이미 존재하면 스킵
        
        mode_display = "DRY-RUN MODE" if self.mode == 'dry-run' else "🚀 LIVE MODE"
        
        header = f"""================================================================================
🚀 떨사오팔 자동매매 시스템 ({mode_display})
================================================================================
• 종목: {self.symbol}
• 초기자금: ${self.initial_funds:,}
• 분할수: {self.buy_portion}
• 수수료: {self.fee}%
• 시작일: {self.start_date}
================================================================================

"""
        with open(self.history_log_path, 'w', encoding='utf-8') as f:
            f.write(header)
        
        logging.info(f"통합 로그 헤더 생성: {self.history_log_path}")
    
    def write_history_log(self, content):
        """통합 로그에 내용 추가"""
        with open(self.history_log_path, 'a', encoding='utf-8') as f:
            f.write(content)
    
    def write_orders_history(self, content):
        """주문 내역 통합 파일에 추가"""
        # 파일이 없으면 헤더 생성
        if not os.path.exists(self.orders_history_path):
            header = f"""================================================================================
📋 주문 내역 기록 ({self.mode.upper()})
================================================================================
• 종목: {self.symbol}
• 시작일: {self.start_date}
================================================================================

"""
            with open(self.orders_history_path, 'w', encoding='utf-8') as f:
                f.write(header)
        
        with open(self.orders_history_path, 'a', encoding='utf-8') as f:
            f.write(content)
    
    def calculate_orders(self):
        """백테스트 로직으로 주문 계산"""
        end_date = self.get_us_date() - timedelta(days=1)
        
        # 시작일 30일 전의 날짜
        start_date_dt = datetime.strptime(self.start_date, '%Y-%m-%d')
        start_date_before_30 = start_date_dt - timedelta(days=30)
        start_date_before_30 = start_date_before_30.strftime('%Y-%m-%d')

        # 30일 전 데이터부터 입력
        df = get_data(ticker=self.symbol, start=start_date_before_30, end=end_date)
        if df is None or df.empty:
            logging.error("가격 데이터 조회 실패")
            return None, None, None, None, None
            
        df_length = len(df) - len(get_data(ticker=self.symbol, start=self.start_date, end=end_date))

        # 오늘 투자 금액 계산
        buyToday, buyQty, funds, holdings, buy_records, sellToday = infinite_buy_today(
            df, self.initial_funds, self.buy_portion, df_length, len(df)-1-df_length, self.fee, self.welfare)
        
        # buy_records 저장 (모드별 분리)
        with open(self.buy_records_path, 'wb') as f:
            pickle.dump(buy_records, f)
        
        logging.info(f"Order Calculation - Holdings: {holdings}, Funds: ${funds:.2f}")
        
        return buyToday, buyQty, funds, holdings, sellToday
    
    def log_morning_history(self, is_trading_day, buyPrice=None, buyQty=None, 
                           sellOrders=None, holdings=None, funds=None, error_msg=None):
        """Morning Task 통합 로그 기록"""
        kr_now = self.get_kr_datetime()
        us_date = self.get_us_date()
        weekday = self.get_weekday_kr(us_date)
        
        content = f"""================================================================================
📅 {us_date} ({weekday}) - Morning Task ({kr_now.strftime('%H:%M')} KST)
================================================================================
"""
        
        if error_msg:
            content += f"🚨 에러 발생: {error_msg}\n"
        elif not is_trading_day:
            content += "🚫 미국 시장 휴장일\n"
        else:
            content += "✅ 미국 시장 개장일\n\n"
            content += "📊 주문 내역:\n"
            
            # 매수 주문
            if buyQty and buyQty > 0:
                content += f"  • LOC 매수: {buyQty}주 @ ${buyPrice:.2f}\n"
            else:
                content += "  • LOC 매수: 없음\n"
            
            # 매도 주문 (여러건 가능)
            if sellOrders and len(sellOrders) > 0:
                for order_type, price, qty in sellOrders:
                    if order_type == 'LOC':
                        content += f"  • LOC 매도: {qty}주 @ ${price:.2f}\n"
                    else:
                        content += f"  • MOC 매도: {qty}주 (손절)\n"
            else:
                content += "  • 매도: 없음\n"
            
            content += f"""
💼 포트폴리오:
  • 보유 주식: {holdings}주
  • 남은 잔고: ${funds:.2f}
"""
        
        content += "\n--------------------------------------------------------------------------------\n\n"
        
        self.write_history_log(content)
    
    def log_evening_history(self, is_trading_day, close_price=None, error_msg=None):
        """Evening Task 통합 로그 기록"""
        kr_now = self.get_kr_datetime()
        us_date = self.get_us_date()
        weekday = self.get_weekday_kr(us_date)
        
        content = f"""================================================================================
📅 {us_date} ({weekday}) - Evening Task ({kr_now.strftime('%H:%M')} KST)
================================================================================
"""
        
        if error_msg:
            content += f"🚨 에러 발생: {error_msg}\n"
        elif not is_trading_day:
            content += "🚫 미국 시장 휴장일 - 종가 업데이트 없음\n"
        else:
            content += f"📈 종가 업데이트 완료: ${close_price:.2f}\n"
        
        content += "\n--------------------------------------------------------------------------------\n\n"
        
        self.write_history_log(content)
    
    def log_orders_to_history(self, buyPrice, buyQty, sellOrders):
        """주문 내역 통합 파일에 기록"""
        kr_now = self.get_kr_datetime()
        us_date = self.get_us_date()
        
        content = f"""[{us_date}] {kr_now.strftime('%H:%M:%S')} KST - Mode: {self.mode.upper()}
"""
        
        if buyQty and buyQty > 0:
            content += f"  BUY (LOC): {buyQty} shares @ ${buyPrice:.2f}\n"
        
        if sellOrders:
            for order_type, price, qty in sellOrders:
                if order_type == 'LOC':
                    content += f"  SELL (LOC): {qty} shares @ ${price:.2f}\n"
                else:
                    content += f"  SELL (MOC): {qty} shares\n"
        
        content += "\n"
        
        self.write_orders_history(content)
    
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
        
        return results
    
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
            return None
        
        # DB 업데이트
        conn = sqlite3.connect('data/trading.db')
        cursor = conn.cursor()
        
        close_price = None
        for data in price_data:
            cursor.execute('''
                INSERT OR REPLACE INTO prices 
                (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (self.symbol, data['date'], data['open'], 
                  data['high'], data['low'], data['close'], data['volume']))
            close_price = data['close']
        
        conn.commit()
        conn.close()
        
        logging.info(f"Price updated: {target_date} - Close: ${close_price:.2f}")
        return close_price
    
    def run_morning_task(self):
        """00:30 실행 - 주문 계산 및 제출"""
        logging.info("="*60)
        logging.info("MORNING TASK START")
        logging.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Mode: {self.mode.upper()}")
        logging.info("="*60)
        
        # 통합 로그 헤더 확인/생성
        self.write_history_header()
        
        # 1. 미국 거래일 확인
        us_today = self.get_us_date()
        is_trading = self.is_trading_day(us_today)
        
        if not is_trading:
            logging.info(f"US Market Closed on {us_today}")
            self.log_morning_history(is_trading_day=False)
            return
        
        logging.info(f"US Market Open on {us_today}")
        
        # 2. 주문 계산
        try:
            buyPrice, buyQty, funds, holdings, sellOrders = self.calculate_orders()
            
            if buyPrice is None:
                error_msg = "Order calculation failed - no price data"
                logging.error(error_msg)
                self.log_morning_history(is_trading_day=True, error_msg=error_msg)
                return
            
            # 3. 통합 로그 기록
            self.log_morning_history(
                is_trading_day=True,
                buyPrice=buyPrice,
                buyQty=buyQty,
                sellOrders=sellOrders,
                holdings=holdings,
                funds=funds
            )
            
            # 4. 주문 내역 기록
            self.log_orders_to_history(buyPrice, buyQty, sellOrders)
            
            # 5. 실거래 모드일 때만 제출
            if self.mode == 'live':
                self.submit_orders(buyPrice, buyQty, sellOrders)
            else:
                logging.info(f"{self.mode.upper()} mode - Orders not submitted")
                
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error in morning task: {e}", exc_info=True)
            self.log_morning_history(is_trading_day=True, error_msg=error_msg)
    
    def run_evening_task(self):
        """10:00 실행 - 종가 업데이트"""
        logging.info("="*60)
        logging.info("EVENING TASK START")
        logging.info(f"실행시간 Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info("="*60)
        
        # 통합 로그 헤더 확인/생성
        self.write_history_header()
        
        # 미국 기준 오늘 (장 마감 후)
        us_date = self.get_us_date()
        is_trading = self.is_trading_day(us_date)
        
        if not is_trading:
            logging.info(f"No trading on {us_date}")
            self.log_evening_history(is_trading_day=False)
            return
        
        # 종가 업데이트
        try:
            close_price = self.update_price_data(us_date)
            if close_price:
                self.log_evening_history(is_trading_day=True, close_price=close_price)
            else:
                self.log_evening_history(is_trading_day=True, error_msg="종가 데이터 없음")
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error updating price: {e}", exc_info=True)
            self.log_evening_history(is_trading_day=True, error_msg=error_msg)
