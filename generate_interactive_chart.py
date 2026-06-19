# generate_interactive_chart.py
"""GitHub Pages용 인터랙티브 차트 생성 (docs/index.html).

기간 슬라이더/버튼으로 줌·패닝, 커서 호버로 정확한 수치 확인 가능.
정적 페이지라 차트 시작일을 입력하면 그 자리에서 수익률을 재계산하는 건 아니고,
config.yaml의 chart.start_date 기준으로 미리 계산된 곡선을 보여준다 (날짜를 바꾸려면
config.yaml을 수정하고 재배포해야 함 - generate_chart.py와 동일한 데이터 소스 사용).

분석용(backtest_all.py)만 사용 - 실거래 주문 로직에는 영향 없음.
"""
import os
import sqlite3

import plotly.graph_objects as go

from generate_chart import get_latest_db_date, run_campaign_simulation
from utils import get_data, load_config

BG_COLOR = '#0d1117'
GRID_COLOR = '#30363d'


def get_earliest_db_date(symbol):
    conn = sqlite3.connect('data/trading.db')
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(date) FROM prices WHERE symbol = ?", (symbol,))
    earliest = cursor.fetchone()[0]
    conn.close()
    return earliest


def build_interactive_chart(symbol, start_date, df_all, df_res, return_rate, mdd, df_trades, output_path):
    win_rate = (len(df_trades[df_trades['수익률(%)'] > 0]) / len(df_trades) * 100) if len(df_trades) > 0 else 0

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_all.index, y=df_all['Close'], name='SOXL 종가',
        mode='lines', line=dict(color='#4FC3F7', width=1.5),
        fill='tozeroy', fillcolor='rgba(79,195,247,0.08)',
        hovertemplate='%{x|%Y-%m-%d}<br>종가 $%{y:.2f}<extra></extra>',
    ))

    buy = df_res[df_res['LOC 매수'] > 0]
    sell = df_res[df_res['수익 실현 매도'] > 0]
    stop = df_res[df_res['MOC 손절'] > 0]

    for trace_df, name, symbol_marker, color in [
        (buy, 'LOC 매수', 'triangle-up', '#4CAF50'),
        (sell, '수익 실현 매도', 'triangle-down', '#EF5350'),
        (stop, 'MOC 손절', 'x', '#FF9800'),
    ]:
        if len(trace_df) == 0:
            continue
        prices = df_all['Close'].reindex(trace_df.index)
        fig.add_trace(go.Scatter(
            x=trace_df.index, y=prices, name=name, mode='markers',
            marker=dict(symbol=symbol_marker, size=11, color=color,
                        line=dict(width=1, color='white')),
            hovertemplate=f'%{{x|%Y-%m-%d}}<br>{name} $%{{y:.2f}}<extra></extra>',
        ))

    fig.add_trace(go.Scatter(
        x=df_res.index, y=df_res['수익율(%)'], name='누적 수익률(%)',
        mode='lines', line=dict(color='#FFD54F', width=2.5), yaxis='y2',
        hovertemplate='%{x|%Y-%m-%d}<br>수익률 %{y:.2f}%<extra></extra>',
    ))

    today_str = df_all.index.max().strftime('%Y-%m-%d')
    stats = (f"수익률 {return_rate:+.2f}%  |  MDD {mdd:.2f}%  |  "
             f"거래 {len(df_trades)}회  |  승률 {win_rate:.1f}%")

    fig.update_layout(
        title=dict(text=f'떨사오팔 {symbol} 자동매매 현황 ({start_date} ~ {today_str})<br>'
                         f'<span style="font-size:13px;color:#AAAAAA">{stats}</span>',
                   x=0.5, xanchor='center'),
        template='plotly_dark',
        paper_bgcolor=BG_COLOR, plot_bgcolor=BG_COLOR,
        hovermode='x unified',
        legend=dict(orientation='h', y=1.1, x=0, bgcolor='rgba(0,0,0,0)'),
        margin=dict(t=110, b=40, l=60, r=60),
        xaxis=dict(
            gridcolor=GRID_COLOR,
            rangeslider=dict(visible=True, bgcolor='#161b22', thickness=0.08),
            rangeselector=dict(
                buttons=[
                    dict(count=1, label='1개월', step='month', stepmode='backward'),
                    dict(count=3, label='3개월', step='month', stepmode='backward'),
                    dict(count=6, label='6개월', step='month', stepmode='backward'),
                    dict(count=1, label='올해', step='year', stepmode='todate'),
                    dict(step='all', label='전체'),
                ],
                bgcolor='#21262d', activecolor='#388bfd', font=dict(color='#E0E0E0'),
            ),
        ),
        yaxis=dict(title='SOXL 종가 ($)', gridcolor=GRID_COLOR, color='#4FC3F7'),
        yaxis2=dict(title='누적 수익률 (%)', overlaying='y', side='right',
                     color='#FFD54F', showgrid=False),
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.write_html(output_path, include_plotlyjs='cdn', full_html=True,
                    config={'displayModeBar': True, 'scrollZoom': True})


def main():
    config = load_config()
    symbol = config['trading']['symbol']
    initial_funds = config['trading']['initial_funds']
    buy_portion = config['trading']['buy_portion']
    fee = config['trading']['fee_rate']
    welfare = config['trading'].get('welfare', True)
    chart_start_date = (config.get('chart') or {}).get('start_date') or config['trading']['start_date']

    end_date = get_latest_db_date(symbol)
    earliest = get_earliest_db_date(symbol)

    df_all = get_data(ticker=symbol, start=earliest, end=end_date)
    if df_all is None or df_all.empty:
        print('SOXL 데이터 없음 - 인터랙티브 차트 생성 중단')
        return

    return_rate, df_res, final_value, df_trades, mdd, effective_start_date = run_campaign_simulation(
        symbol, chart_start_date, end_date, initial_funds, buy_portion, fee, welfare
    )

    build_interactive_chart(symbol, effective_start_date, df_all, df_res, return_rate, mdd,
                             df_trades, 'docs/index.html')

    print('인터랙티브 차트 생성 완료: docs/index.html')


if __name__ == '__main__':
    main()
