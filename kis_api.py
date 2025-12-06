import requests
import json
import hashlib
import os
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import time
from dotenv import load_dotenv

load_dotenv()

class KISApi:
    """한국투자증권 Open API 래퍼 - 실거래 전용"""
    
    def __init__(self):
        """실거래 전용 초기화"""
        self.app_key = os.getenv('APP_KEY')
        self.app_secret = os.getenv('APP_SECRET')
        self.account_number = os.getenv('ACCOUNT_NUMBER')
        self.account_code = os.getenv('ACCOUNT_CODE', '01')
        
        # 실거래 서버만 사용
        self.base_url = "https://openapi.koreainvestment.com:9443"
        
        self.access_token = None
        self.token_expired = None
        
        # 토큰 저장 파일 경로
        self.token_file = 'data/kis_token.pkl'
        os.makedirs('data', exist_ok=True)
        
        # 저장된 토큰 로드 또는 새로 발급
        self._load_or_refresh_token()
    
    def _save_token(self):
        """토큰을 파일에 저장"""
        token_data = {
            'access_token': self.access_token,
            'token_expired': self.token_expired,
            'app_key': self.app_key
        }
        with open(self.token_file, 'wb') as f:
            pickle.dump(token_data, f)
        logging.info("Token saved to file")
    
    def _load_token(self) -> bool:
        """저장된 토큰 로드"""
        if not os.path.exists(self.token_file):
            return False
        
        try:
            with open(self.token_file, 'rb') as f:
                token_data = pickle.load(f)
            
            # 같은 API 키인지 확인
            if token_data.get('app_key') == self.app_key:
                self.access_token = token_data.get('access_token')
                self.token_expired = token_data.get('token_expired')
                
                # 토큰 만료 확인
                if self.token_expired and datetime.now() < self.token_expired:
                    logging.info(f"Loaded valid token from file (expires: {self.token_expired})")
                    return True
                else:
                    logging.info("Saved token is expired")
            else:
                logging.info("Token file exists but for different credentials")
        except Exception as e:
            logging.error(f"Failed to load token: {e}")
        
        return False
    
    def _load_or_refresh_token(self):
        """저장된 토큰 로드 또는 새로 발급"""
        if not self._load_token():
            self._get_access_token()
            self._save_token()
    
    def _get_access_token(self):
        """접근 토큰 발급"""
        path = "/oauth2/tokenP"
        url = self.base_url + path
        
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret
        }
        
        # 재시도 로직
        max_retries = 3
        for attempt in range(max_retries):
            try:
                res = requests.post(url, headers=headers, data=json.dumps(body))
                
                if res.status_code == 200:
                    data = res.json()
                    
                    # 에러 응답 체크
                    if 'error_code' in data:
                        if 'EGW00133' in data.get('error_code', ''):
                            logging.warning(f"Token rate limit. Waiting 60 seconds... (attempt {attempt+1}/{max_retries})")
                            time.sleep(60)
                            continue
                        else:
                            raise Exception(f"API Error: {data.get('error_description', 'Unknown error')}")
                    
                    self.access_token = data['access_token']
                    # 토큰 만료 시간 설정 (실제 만료 1시간 전)
                    expires_in = int(data.get('expires_in', 86400))
                    self.token_expired = datetime.now() + timedelta(seconds=expires_in - 3600)
                    logging.info(f"Access token issued successfully (expires: {self.token_expired})")
                    return
                    
                else:
                    error_data = res.json() if res.text else {}
                    if 'EGW00133' in error_data.get('error_code', ''):
                        logging.warning(f"Token rate limit. Waiting 60 seconds... (attempt {attempt+1}/{max_retries})")
                        time.sleep(60)
                        continue
                    else:
                        raise Exception(f"Failed to get access token: {res.text}")
                        
            except requests.exceptions.RequestException as e:
                logging.error(f"Network error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                raise
        
        raise Exception("Failed to get access token after all retries")
    
    def _check_token(self):
        """토큰 유효성 확인 및 갱신"""
        if not self.access_token or datetime.now() >= self.token_expired:
            logging.info("Token expired or not exists, refreshing...")
            self._get_access_token()
            self._save_token()
    
    def _make_hash(self, data: Dict) -> str:
        """해시값 생성 (실거래 주문용)"""
        data_str = json.dumps(data, ensure_ascii=False).replace(' ', '')
        hash_obj = hashlib.sha256(data_str.encode())
        return hash_obj.hexdigest()
    
    def get_overseas_price_daily(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        """해외 주식 일봉 조회"""
        self._check_token()
        
        path = "/uapi/overseas-price/v1/quotations/dailyprice"
        url = self.base_url + path
        
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS76240000"  # 해외주식 일별 시세 조회
        }
        
        all_data = []
        current_end = end_date
        
        while current_end >= start_date:
            params = {
                "AUTH": "",
                "EXCD": "AMS",  # AMEX
                "SYMB": symbol,
                "GUBN": "0",  # 일봉
                "BYMD": current_end,  # 조회 종료일
                "MODP": "0"  # 수정주가 반영
            }
            
            res = requests.get(url, headers=headers, params=params)
            
            if res.status_code == 200:
                data = res.json()
                
                if data['rt_cd'] != '0':
                    logging.error(f"API Error: {data.get('msg1', 'Unknown error')}")
                    break
                
                output2 = data.get('output2', [])
                if not output2:
                    break
                
                # 필요한 데이터만 추출
                for item in output2:
                    trade_date = item['xymd']  # YYYYMMDD
                    
                    # start_date 이후 데이터만 추가
                    if trade_date >= start_date and trade_date <= end_date:
                        all_data.append({
                            'date': datetime.strptime(trade_date, '%Y%m%d').date(),
                            'open': float(item['open']),
                            'high': float(item['high']),
                            'low': float(item['low']),
                            'close': float(item['clos']),
                            'volume': int(item.get('tvol', 0))
                        })
                
                # 가장 오래된 날짜를 다음 조회의 종료일로 설정
                if output2:
                    oldest_date = output2[-1]['xymd']
                    if oldest_date <= start_date:
                        break
                    # 하루 전날로 설정
                    oldest_dt = datetime.strptime(oldest_date, '%Y%m%d')
                    current_end = (oldest_dt - timedelta(days=1)).strftime('%Y%m%d')
                else:
                    break
                
                # API 호출 제한 방지
                time.sleep(0.1)
            else:
                logging.error(f"Failed to get price data: {res.text}")
                break
        
        # 날짜순 정렬 (오래된 날짜부터)
        all_data.sort(key=lambda x: x['date'])
        
        logging.info(f"Loaded {len(all_data)} days of price data for {symbol}")
        return all_data
    
    def get_current_price(self, symbol: str = "SOXL") -> Dict:
        """현재가 조회"""
        self._check_token()
        
        path = "/uapi/overseas-price/v1/quotations/price"
        url = self.base_url + path
        
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS00000300"  # 실거래 TR_ID
        }
        
        params = {
            "AUTH": "",
            "EXCD": "AMS",
            "SYMB": symbol
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if data['rt_cd'] == '0':
                return data.get('output', {})
            else:
                logging.error(f"API Error: {data.get('msg1')}")
                return {}
        else:
            logging.error(f"Failed to get current price: {res.text}")
            return {}
    
    def get_account_balance(self) -> Dict:
        """계좌 잔고 조회"""
        self._check_token()
        
        path = "/uapi/overseas-stock/v1/trading/inquire-balance"
        url = self.base_url + path
        
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "JTTT3012R"  # 실거래 TR_ID
        }
        
        params = {
            "CANO": self.account_number,
            "ACNT_PRDT_CD": self.account_code,
            "OVRS_EXCG_CD": "NASD",
            "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": ""
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if data['rt_cd'] == '0':
                return data.get('output2', {})
            else:
                logging.error(f"API Error: {data.get('msg1')}")
                return {}
        else:
            logging.error(f"Failed to get account balance: {res.text}")
            return {}
    
    def place_order(self, order_type: str, symbol: str, quantity: int, 
                   price: float = 0) -> Dict:
        """주문 제출"""
        self._check_token()
        
        path = "/uapi/overseas-stock/v1/trading/order"
        url = self.base_url + path
        
        # 실거래 주문 TR_ID
        if order_type == 'LOC_BUY':
            tr_id = "JTTT1002U"
            ord_dvsn = "34"  # LOC 매수
            buy_sell = "BUY"
        elif order_type == 'LOC_SELL':
            tr_id = "JTTT1006U"
            ord_dvsn = "34"  # LOC 매도
            buy_sell = "SELL"
        elif order_type == 'MOC_SELL':
            tr_id = "JTTT1006U"
            ord_dvsn = "33"  # MOC 매도
            buy_sell = "SELL"
        else:
            raise ValueError(f"Invalid order type: {order_type}")
        
        body = {
            "CANO": self.account_number,
            "ACNT_PRDT_CD": self.account_code,
            "OVRS_EXCG_CD": "AMEX",
            "PDNO": symbol,
            "ORD_QTY": str(quantity),
            "OVRS_ORD_UNPR": str(price) if price > 0 else "0",
            "SLL_BUY_DVSN_CD": buy_sell[0],  # B or S
            "ORD_DVSN": ord_dvsn,
            "ORD_SVR_DVSN_CD": "0"
        }
        
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "hashkey": self._make_hash(body)  # 실거래는 항상 해시 필요
        }
        
        res = requests.post(url, headers=headers, data=json.dumps(body))
        
        if res.status_code == 200:
            data = res.json()
            if data['rt_cd'] == '0':
                logging.info(f"Order placed successfully: {order_type} {quantity} {symbol}")
                return {'success': True, 'data': data.get('output', {})}
            else:
                logging.error(f"Order failed: {data.get('msg1')}")
                return {'success': False, 'msg': data.get('msg1')}
        else:
            logging.error(f"Order request failed: {res.text}")
            return {'success': False, 'msg': res.text}
    
    def get_orders(self) -> List[Dict]:
        """당일 주문 내역 조회"""
        self._check_token()
        
        path = "/uapi/overseas-stock/v1/trading/inquire-ccnl"
        url = self.base_url + path
        
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "JTTT3001R"  # 실거래 TR_ID
        }
        
        params = {
            "CANO": self.account_number,
            "ACNT_PRDT_CD": self.account_code,
            "PDNO": "%",  # 전체
            "ORD_STRT_DT": datetime.now().strftime('%Y%m%d'),
            "ORD_END_DT": datetime.now().strftime('%Y%m%d'),
            "SLL_BUY_DVSN": "00",  # 전체
            "CCLD_NCCS_DVSN": "00",  # 전체
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": ""
        }
        
        res = requests.get(url, headers=headers, params=params)
        
        if res.status_code == 200:
            data = res.json()
            if data['rt_cd'] == '0':
                return data.get('output', [])
            else:
                logging.error(f"Failed to get orders: {data.get('msg1')}")
                return []
        else:
            logging.error(f"Failed to get orders: {res.text}")
            return []