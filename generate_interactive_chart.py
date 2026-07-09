# generate_interactive_chart.py
"""GitHub Pages용 인터랙티브 대시보드 생성 (docs/index.html).

v2 (2026-07): 시뮬레이션 재현이 아니라 **실제 live 주문·포트폴리오 기록** 기반.
- 종가: data/trading.db
- 주문: logs/live/orders_history.txt (실제 제출된 주문)
- 평가액·수익률: run_campaign_simulation(generate_chart.py)로 **현재 config 기준
  start_date부터 매번 재계산** — trading_history 로그의 포트폴리오 스냅샷은 기록 당시
  config가 섞여 있어 쓰지 않는다 (실거래 주문 계산과 동일한 재계산 경로라 장부와 일치)
- 체결 판정(LOC 규칙): 매수 = 종가 <= 지정가, 매도 = 종가 >= 지정가, MOC = 항상 체결
  · 당일 종가가 아직 DB에 없으면 "체결 대기"

config 변경 시 동작: 평가액·수익률 곡선은 전체 재계산되고, 주문 마커는 실제 제출
기록(로그 사실)이라 불변.

분석·표시 전용 - 실거래 주문 로직에는 영향 없음.
"""
import re
import sqlite3

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from generate_chart import get_latest_db_date, run_campaign_simulation
from utils import load_config

BG_COLOR = '#0d1117'
GRID_COLOR = '#30363d'
C_BUY = '#ff6b6b'      # 매수 (적색 - 국내 관례)
C_SELL = '#5b9ce8'     # 매도 (청색)
C_MISS = '#8b949e'     # 미체결
C_PEND = '#3fb98f'     # 종가 대기
C_PRICE = '#4FC3F7'
C_RET = '#FFD54F'

ORDERS_PATH = 'logs/live/orders_history.txt'


def load_prices(symbol, start_date):
    conn = sqlite3.connect('data/trading.db')
    rows = conn.execute(
        "SELECT date, close FROM prices WHERE symbol = ? AND date >= ? ORDER BY date",
        (symbol, start_date),
    ).fetchall()
    conn.close()
    return [r[0] for r in rows], [r[1] for r in rows]


def parse_orders(start_date):
    """orders_history.txt -> {미국거래일: [주문]}. 같은 날 재기록 시 마지막 항목 사용."""
    orders = {}
    cur = None
    for line in open(ORDERS_PATH, encoding='utf-8'):
        m = re.match(r'\[(\d{4}-\d{2}-\d{2})\] [\d:]+ KST - Mode: LIVE', line)
        if m:
            cur = m.group(1) if m.group(1) >= start_date else None
            if cur:
                orders[cur] = []
            continue
        m = re.match(r'\s+(BUY|SELL) \((LOC|MOC)\): (\d+) shares @ \$([\d.]+)', line)
        if m and cur:
            orders[cur].append({
                'side': m.group(1), 'type': m.group(2),
                'qty': int(m.group(3)), 'price': float(m.group(4)),
            })
    return orders


def judge_fills(orders, close_map):
    """LOC 체결 판정. filled: True/False/None(종가 대기)."""
    for d, lst in orders.items():
        c = close_map.get(d)
        for o in lst:
            if c is None:
                o['filled'] = None
            elif o['type'] == 'MOC':
                o['filled'] = True
            elif o['side'] == 'BUY':
                o['filled'] = c <= o['price'] + 1e-9
            else:
                o['filled'] = c >= o['price'] - 1e-9
            o['fill_price'] = c if o['filled'] else None


def build_dashboard(symbol, start_date, initial_funds, config, output_path):
    dates, closes = load_prices(symbol, start_date)
    if not dates:
        print('가격 데이터 없음 - 대시보드 생성 중단')
        return
    close_map = dict(zip(dates, closes))

    orders = parse_orders(start_date)
    judge_fills(orders, close_map)

    # 평가액·수익률: 현재 config로 start_date부터 재계산 (실거래와 동일한 계산 경로)
    end_date = get_latest_db_date(symbol)
    return_rate, df_res, final_value, df_trades, mdd, start_date = run_campaign_simulation(
        symbol, start_date, end_date, initial_funds,
        config['buy_portion'], config['fee_rate'], config.get('welfare', True),
    )
    eq_dates = [d.strftime('%Y-%m-%d') for d in df_res.index]
    equity = df_res['총 평가액'].tolist()
    rets = df_res['수익율(%)'].tolist()

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.52, 0.22, 0.26], vertical_spacing=0.05,
        subplot_titles=('SOXL 종가와 주문·체결', '일별 주문 금액 (진한색=체결)', '수익률 (원금 대비)'),
    )

    # --- 1행: 종가 + 주문 마커 ---
    fig.add_trace(go.Scatter(
        x=dates, y=closes, name='SOXL 종가', mode='lines',
        line=dict(color=C_PRICE, width=1.6),
        hovertemplate='%{x|%Y-%m-%d}<br>종가 $%{y:.2f}<extra></extra>',
    ), row=1, col=1)

    groups = {
        'buy':  dict(name='매수 체결', symbol='triangle-up', color=C_BUY, x=[], y=[], t=[]),
        'sell': dict(name='매도 체결', symbol='triangle-down', color=C_SELL, x=[], y=[], t=[]),
        'miss': dict(name='미체결 주문', symbol='circle-open', color=C_MISS, x=[], y=[], t=[]),
        'pend': dict(name='체결 대기', symbol='circle-open', color=C_PEND, x=[], y=[], t=[]),
    }
    for d in sorted(orders):
        for o in orders[d]:
            nm = '매수' if o['side'] == 'BUY' else '매도'
            if o['filled'] is True:
                g = groups['buy' if o['side'] == 'BUY' else 'sell']
                g['x'].append(d); g['y'].append(o['fill_price'])
                g['t'].append(f"{nm} {o['type']} {o['qty']}주<br>지정가 ${o['price']:.2f} → 체결가 ${o['fill_price']:.2f}")
            else:
                g = groups['miss' if o['filled'] is False else 'pend']
                g['x'].append(d); g['y'].append(o['price'])
                st = '미체결' if o['filled'] is False else '종가 대기'
                g['t'].append(f"{nm} {o['type']} {o['qty']}주<br>지정가 ${o['price']:.2f} ({st})")
    for g in groups.values():
        if not g['x']:
            continue
        fig.add_trace(go.Scatter(
            x=g['x'], y=g['y'], name=g['name'], mode='markers',
            marker=dict(symbol=g['symbol'], size=11, color=g['color'],
                        line=dict(width=1.2, color='white' if 'open' not in g['symbol'] else g['color'])),
            text=g['t'], hovertemplate='%{x|%Y-%m-%d}<br>%{text}<extra></extra>',
        ), row=1, col=1)

    # --- 2행: 일별 주문 금액 ---
    bars = {
        ('BUY', True):  dict(name='매수 체결', color=C_BUY, opacity=1.0, sign=1, x=[], y=[], t=[]),
        ('BUY', False): dict(name='매수 미체결·대기', color=C_BUY, opacity=0.35, sign=1, x=[], y=[], t=[]),
        ('SELL', True):  dict(name='매도 체결', color=C_SELL, opacity=1.0, sign=-1, x=[], y=[], t=[]),
        ('SELL', False): dict(name='매도 미체결·대기', color=C_SELL, opacity=0.35, sign=-1, x=[], y=[], t=[]),
    }
    for d in sorted(orders):
        for o in orders[d]:
            b = bars[(o['side'], o['filled'] is True)]
            amt = o['qty'] * o['price']
            b['x'].append(d); b['y'].append(b['sign'] * amt)
            b['t'].append(f"{o['qty']}주 @ ${o['price']:.2f} = ${amt:,.0f}")
    for b in bars.values():
        if not b['x']:
            continue
        fig.add_trace(go.Bar(
            x=b['x'], y=b['y'], name=b['name'], showlegend=False,
            marker=dict(color=b['color'], opacity=b['opacity']),
            text=b['t'], hovertemplate='%{x|%Y-%m-%d}<br>%{text}<extra>' + b['name'] + '</extra>',
        ), row=2, col=1)

    # --- 3행: 수익률 ---
    fig.add_trace(go.Scatter(
        x=eq_dates, y=rets, name='수익률(%)', mode='lines+markers', showlegend=False,
        line=dict(color=C_RET, width=2), marker=dict(size=5),
        customdata=equity,
        hovertemplate='%{x|%Y-%m-%d}<br>수익률 %{y:.2f}%<br>평가금액 $%{customdata:,.0f}<extra></extra>',
    ), row=3, col=1)
    fig.add_hline(y=0, line=dict(color=GRID_COLOR, width=1), row=3, col=1)

    # --- 헤더 통계 ---
    n_buy = sum(1 for lst in orders.values() for o in lst if o['side'] == 'BUY' and o['filled'] is True)
    n_sell = sum(1 for lst in orders.values() for o in lst if o['side'] == 'SELL' and o['filled'] is True)
    if equity:
        last = df_res.iloc[-1]
        stats = (f"평가금액 ${equity[-1]:,.0f} ({rets[-1]:+.2f}%)  |  "
                 f"보유 {int(last['보유 주식 수'])}주 · 예수금 ${last['예수금']:,.0f}  |  "
                 f"MDD {mdd:.1f}%  |  체결 매수 {n_buy}회 · 매도 {n_sell}회")
    else:
        stats = '포트폴리오 기록 없음'
    today_str = dates[-1]

    fig.update_layout(
        title=dict(text=f'떨사오팔 {symbol} 실거래 현황 ({start_date} ~ {today_str})<br>'
                        f'<span style="font-size:13px;color:#AAAAAA">{stats}  ·  원금 ${initial_funds:,.0f} 기준</span>',
                   x=0.5, xanchor='center', y=0.985, yanchor='top'),
        template='plotly_dark',
        paper_bgcolor=BG_COLOR, plot_bgcolor=BG_COLOR,
        barmode='relative',
        hovermode='closest',
        legend=dict(orientation='h', y=1.065, x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
        margin=dict(t=185, b=40, l=60, r=30),
        height=920,
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label='1개월', step='month', stepmode='backward'),
                    dict(count=3, label='3개월', step='month', stepmode='backward'),
                    dict(count=6, label='6개월', step='month', stepmode='backward'),
                    dict(step='all', label='전체'),
                ],
                bgcolor='#21262d', activecolor='#388bfd', font=dict(color='#E0E0E0'),
            ),
        ),
    )
    for ax in ('xaxis', 'xaxis2', 'xaxis3'):
        fig.update_layout({ax: dict(gridcolor=GRID_COLOR)})
    fig.update_xaxes(rangebreaks=[dict(bounds=['sat', 'mon'])])
    fig.update_yaxes(title_text='종가 ($)', gridcolor=GRID_COLOR, row=1, col=1)
    fig.update_yaxes(title_text='주문 금액 ($)', gridcolor=GRID_COLOR, row=2, col=1)
    fig.update_yaxes(title_text='수익률 (%)', gridcolor=GRID_COLOR, row=3, col=1)

    import os
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.write_html(output_path, include_plotlyjs='cdn', full_html=True,
                   config={'displayModeBar': True, 'scrollZoom': True})


def main():
    config = load_config()
    symbol = config['trading']['symbol']
    initial_funds = float(config['trading']['initial_funds'])
    start_date = (config.get('chart') or {}).get('start_date') or config['trading']['start_date']

    build_dashboard(symbol, start_date, initial_funds, config['trading'], 'docs/index.html')
    print('인터랙티브 대시보드 생성 완료: docs/index.html')


if __name__ == '__main__':
    main()
