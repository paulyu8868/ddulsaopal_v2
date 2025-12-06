# evening_task.py  
from daily_run import DailyTrader

if __name__ == "__main__":
    trader = DailyTrader(mode='update-only')
    trader.run_evening_task()