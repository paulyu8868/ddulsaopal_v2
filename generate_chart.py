# generate_chart.py
"""README용 매매 현황 차트 생성.

SOXL 연중 가격 + 캠페인 시작일(config.yaml의 start_date)부터의 누적 수익률을
한 차트에 그려 assets/trading_chart.png로 저장한다.

분석용(backtest_all.py)만 사용 - 실거래 주문 로직에는 영향 없음.
"""
import os
import sqlite3
from datetime import datetime, timedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from backtest_all import infinite_buy_simulation
from utils import get_data, load_config

plt.rcParams['font.family'] = ['NanumGothic', 'Malgun Gothic', 'AppleGothic', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

BG_COLOR = '#0d1117'  # GitHub 다크 테마 배경색
NUMERIC_COLS = ['시가', '고가', '종가', 'LOC 매수', '수익 실현 매도', 'MOC 손절',
                 '보유 주식 수', '예수금', '총 평가액', '수익율(%)', 'MDD']


def get_latest_db_date(symbol):
    conn = sqlite3.connect('data/trading.db')
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM prices WHERE symbol = ?", (symbol,))
    latest = cursor.fetchone()[0]
    conn.close()
    return datetime.strptime(latest, '%Y-%m-%d')


def run_campaign_simulation(symbol, start_date, end_date, initial_funds, buy_portion, fee, welfare):
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    if start_date_dt > end_date:
        # 캠페인 재시작 직후 evening-task가 아직 그 날짜를 backfill하기 전이면
        # start_date가 DB 최신일보다 앞서는 경우가 생길 수 있음 -> end_date로 맞춰서 빈 차트 방지
        start_date_dt = end_date
        start_date = end_date.strftime('%Y-%m-%d')

    start_before_30 = (start_date_dt - timedelta(days=30)).strftime('%Y-%m-%d')
    df_full = get_data(ticker=symbol, start=start_before_30, end=end_date)
    df_length = len(df_full) - len(get_data(ticker=symbol, start=start_date, end=end_date))

    return_rate, df_res, final_value, df_trades, mdd = infinite_buy_simulation(
        df_full, None, initial_funds, buy_portion, df_length,
        len(df_full) - 1 - df_length, fee, welfare
    )

    df_res['날짜'] = pd.to_datetime(df_res['날짜'])
    df_res = df_res.set_index('날짜')
    for col in NUMERIC_COLS:
        df_res[col] = pd.to_numeric(df_res[col])

    return return_rate, df_res, final_value, df_trades, mdd, start_date


def build_chart(symbol, start_date, df_year, df_res, return_rate, mdd, df_trades, output_path):
    win_rate = (len(df_trades[df_trades['수익률(%)'] > 0]) / len(df_trades) * 100) if len(df_trades) > 0 else 0

    fig, ax1 = plt.subplots(figsize=(14, 7), facecolor=BG_COLOR)
    ax1.set_facecolor(BG_COLOR)

    color_price = '#4FC3F7'
    ax1.plot(df_year.index, df_year['Close'], color=color_price, linewidth=1.8, label='SOXL 종가', zorder=2)
    ax1.fill_between(df_year.index, df_year['Close'], df_year['Close'].min() * 0.97,
                      color=color_price, alpha=0.12, zorder=1)
    ax1.set_ylabel('SOXL 종가 ($)', color=color_price, fontsize=11)
    ax1.tick_params(axis='y', labelcolor=color_price)

    buy_dates = [d.date() for d in df_res[df_res['LOC 매수'] > 0].index]
    sell_dates = [d.date() for d in df_res[df_res['수익 실현 매도'] > 0].index]
    stop_dates = [d.date() for d in df_res[df_res['MOC 손절'] > 0].index]

    if buy_dates:
        ax1.scatter(buy_dates, df_year['Close'].loc[df_year.index.isin(buy_dates)],
                    marker='^', color='#4CAF50', s=70, zorder=5, label='LOC 매수',
                    edgecolors='white', linewidths=0.5)
    if sell_dates:
        ax1.scatter(sell_dates, df_year['Close'].loc[df_year.index.isin(sell_dates)],
                    marker='v', color='#EF5350', s=70, zorder=5, label='수익 실현 매도',
                    edgecolors='white', linewidths=0.5)
    if stop_dates:
        ax1.scatter(stop_dates, df_year['Close'].loc[df_year.index.isin(stop_dates)],
                    marker='x', color='#FF9800', s=90, zorder=5, label='MOC 손절', linewidths=2)

    ax2 = ax1.twinx()
    color_ret = '#FFD54F'
    ax2.plot(df_res.index, df_res['수익율(%)'], color=color_ret, linewidth=2.2,
              label='누적 수익률(%)', zorder=3)
    ax2.ticklabel_format(axis='y', style='plain', useOffset=False)
    if df_res['수익율(%)'].abs().max() < 1e-6:
        ax2.set_ylim(-1, 1)  # 캠페인 초기(거래 없음)에 부동소수점 잡음으로 축이 깨지는 것 방지
    ax2.axhline(0, color='white', linewidth=0.6, linestyle='--', alpha=0.4)
    ax2.fill_between(df_res.index, df_res['수익율(%)'], 0,
                      where=(df_res['수익율(%)'] >= 0), color='#66BB6A', alpha=0.15)
    ax2.fill_between(df_res.index, df_res['수익율(%)'], 0,
                      where=(df_res['수익율(%)'] < 0), color='#EF5350', alpha=0.15)
    ax2.set_ylabel('누적 수익률 (%)', color=color_ret, fontsize=11)
    ax2.tick_params(axis='y', labelcolor=color_ret)

    ax1.axvline(pd.to_datetime(start_date), color='white', linestyle=':', linewidth=1, alpha=0.5)
    ax1.text(pd.to_datetime(start_date), ax1.get_ylim()[1] * 0.98, ' 캠페인 시작',
              color='white', fontsize=9, alpha=0.7, va='top')

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator())
    fig.autofmt_xdate()

    title_color = '#E0E0E0'
    today_str = df_year.index.max().strftime('%Y-%m-%d')
    plt.title(f'떨사오팔 {symbol} 자동매매 현황 ({start_date} ~ {today_str})',
              color=title_color, fontsize=14, pad=15, fontweight='bold')

    stats_text = (f"수익률 {return_rate:+.2f}%   |   MDD {mdd:.2f}%   |   "
                  f"거래 {len(df_trades)}회   |   승률 {win_rate:.1f}%")
    fig.text(0.5, 0.93, stats_text, ha='center', color=title_color, fontsize=10, alpha=0.85)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', ncol=1,
              facecolor='#1e1e1e', edgecolor='none', labelcolor=title_color,
              fontsize=9, framealpha=0.7)

    for spine in ax1.spines.values():
        spine.set_color('#555555')
    ax1.tick_params(colors='#AAAAAA')
    ax2.tick_params(colors='#AAAAAA')

    plt.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, dpi=150, facecolor=BG_COLOR, bbox_inches='tight')
    plt.close(fig)


def main():
    config = load_config()
    symbol = config['trading']['symbol']
    initial_funds = config['trading']['initial_funds']
    buy_portion = config['trading']['buy_portion']
    fee = config['trading']['fee_rate']
    welfare = config['trading'].get('welfare', True)

    # 차트 표시용 시작일 - chart.start_date가 있으면 그걸 쓰고, 없으면 실거래 start_date를 따라감
    chart_start_date = (config.get('chart') or {}).get('start_date') or config['trading']['start_date']

    end_date = get_latest_db_date(symbol)
    year_start = f'{end_date.year}-01-01'

    df_year = get_data(ticker=symbol, start=year_start, end=end_date)
    if df_year is None or df_year.empty:
        print('SOXL 연간 데이터 없음 - 차트 생성 중단')
        return

    return_rate, df_res, final_value, df_trades, mdd, effective_start_date = run_campaign_simulation(
        symbol, chart_start_date, end_date, initial_funds, buy_portion, fee, welfare
    )

    build_chart(symbol, effective_start_date, df_year, df_res, return_rate, mdd, df_trades,
                'assets/trading_chart.png')

    print(f'차트 생성 완료: 수익률 {return_rate:.2f}%, MDD {mdd:.2f}%, '
          f'거래 {len(df_trades)}회')


if __name__ == '__main__':
    main()
