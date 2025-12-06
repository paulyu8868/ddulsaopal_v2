# morning_task.py
from daily_run import DailyTrader
import sys

if __name__ == "__main__":
    # python morning_task.py [dry-run|live]
    mode = sys.argv[1] if len(sys.argv) > 1 else 'dry-run'
    trader = DailyTrader(mode=mode)
    trader.run_morning_task()