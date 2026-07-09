# generate_interactive_chart.py
"""GitHub Pages용 인터랙티브 대시보드 생성 (docs/index.html).

v2 (2026-07): 시뮬레이션 재현이 아니라 **실제 live 주문·포트폴리오 기록** 기반.
- 종가: data/trading.db
- 주문: logs/live/orders_history.txt (실제 제출된 주문)
- 보유주식·예수금: logs/live/trading_history_*.log 의 Morning Task 블록
- 체결 판정(LOC 규칙): 매수 = 종가 <= 지정가, 매도 = 종가 >= 지정가, MOC = 항상 체결
  · 당일 종가가 아직 DB에 없으면 "체결 대기"
- 수익률 = (보유주식 x 종가 + 예수금) / config initial_funds - 1  ← 봇 장부 기준

config 변경 시 동작: start_date는 표시 구간 필터, initial_funds는 수익률 분모로만
쓰인다. 과거 주문·체결 기록 자체는 로그 사실이므로 재계산되지 않는다.

분석·표시 전용 - 실거래 주문 로직에는 영향 없음.
"""
import glob
import re
import sqlite3

import plotly.graph_objects as go
from plotly.subplots import make_subplots

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
HISTORY_GLOB = 'logs/live/trading_history_*.log'


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


def parse_portfolio(start_date):
    """trading_history 로그의 Morning Task 블록 -> {미국거래일: (보유주식, 예수금)}."""
    port = {}
    for path in sorted(glob.glob(HISTORY_GLOB)):
        text = open(path, encoding='utf-8').read()
        headers = list(re.finditer(r'📅 (\d{4}-\d{2}-\d{2}) \(.\) - (Morning|Evening) Task', text))
        for i, m in enumerate(headers):
            if m.group(2) != 'Morning':
                continue
            block = text[m.end(): headers[i + 1].start() if i + 1 < len(headers) else len(text)]
            hm = re.search(r'보유 주식: ([\d,]+)주', block)
            fm = re.search(r'남은 잔고: \$([\d,]+\.?\d*)', block)
            if hm and fm and m.group(1) >= start_date:
                port[m.group(1)] = (
                    int(hm.group(1).replace(',', '')),
                    float(fm.group(1).replace(',', '')),
                )
    return port


def build_dashboard(symbol, start_date, initial_funds, output_path):
    dates, closes = load_prices(symbol, start_date)
    if not dates:
        print('가격 데이터 없음 - 대시보드 생성 중단')
        return
    close_map = dict(zip(dates, closes))

    orders = parse_orders(start_date)
    judge_fills(orders, close_map)
    port = parse_portfolio(start_date)

    eq_dates, equity, rets = [], [], []
    for d in sorted(port):
        c = close_map.get(d)
        if c is None:
            continue
        h, f = port[d]
        e = h * c + f
        eq_dates.append(d)
        equity.append(e)
        rets.append((e / initial_funds - 1) * 100)

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
        last_h, last_f = port[eq_dates[-1]]
        stats = (f"평가금액 ${equity[-1]:,.0f} ({rets[-1]:+.2f}%)  |  "
                 f"보유 {last_h}주 · 예수금 ${last_f:,.0f}  |  "
                 f"체결 매수 {n_buy}회 · 매도 {n_sell}회")
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

    build_dashboard(symbol, start_date, initial_funds, 'docs/index.html')
    print('인터랙티브 대시보드 생성 완료: docs/index.html')


if __name__ == '__main__':
    main()
