# evening_task.py  
from daily_run import DailyTrader
import sys

if __name__ == "__main__":
    # morning_task와 동일한 모드 사용 (기본: dry-run)
    mode = sys.argv[1] if len(sys.argv) > 1 else 'dry-run'
    trader = DailyTrader(mode=mode)
    trader.run_evening_task()
