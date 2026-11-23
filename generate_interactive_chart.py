# generate_interactive_chart.py
"""GitHub Pages용 인터랙티브 대시보드 생성 (docs/index.html).

v3 (2026-11): 연간 전략 성과표.
config.yaml의 chart.start_date가 'ytd'면 올해 첫 거래일부터, 원금(trading.initial_funds)으로
떨사오팔을 운용했다고 가정한 시뮬레이션(run_campaign_simulation)을 보여준다.
해가 바뀌면 새해 첫 종가가 DB에 들어온 날 자동으로 새해 기준으로 리셋된다
(그 전까지는 전년도 결산이 유지됨). x축은 해당 연도 1/1~12/31로 고정.

- 1단: 종가 + 시뮬레이션 매매 마커 (LOC 매수 / 익절 매도 / MOC 손절)
- 2단: 일별 매매 금액 (위 매수 / 아래 매도)
- 3단: 전략 수익률 vs 같은 기간 SOXL 단순 보유 수익률

실계좌 잔고가 아니라 전략 시뮬레이션이다. 분석·표시 전용 - 실거래 주문 로직에는 영향 없음.
"""
import os

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from generate_chart import get_latest_db_date, resolve_chart_start, run_campaign_simulation
from utils import load_config

BG_COLOR = '#0d1117'
GRID_COLOR = '#30363d'
C_BUY = '#ff6b6b'      # 매수 (적색 - 국내 관례)
C_SELL = '#5b9ce8'     # 익절 매도 (청색)
C_STOP = '#FF9800'     # 손절
C_PRICE = '#4FC3F7'
C_RET = '#FFD54F'
C_HOLD = '#8b949e'     # 단순 보유 비교선


def build_dashboard(symbol, start_date, cfg, output_path):
    initial_funds = float(cfg['initial_funds'])
    end_date = get_latest_db_date(symbol)
    return_rate, df_res, final_value, df_trades, mdd, start_date = run_campaign_simulation(
        symbol, start_date, end_date, initial_funds,
        cfg['buy_portion'], cfg['fee_rate'], cfg.get('welfare', True),
    )
    if df_res.empty:
        print('시뮬레이션 결과 없음 - 대시보드 생성 중단')
        return

    year = int(start_date[:4])
    dates = df_res.index
    close = df_res['종가']
    hold_ret = (close / close.iloc[0] - 1) * 100   # 첫 거래일 종가에 전액 매수 후 보유

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.5, 0.2, 0.3], vertical_spacing=0.05,
        subplot_titles=('SOXL 종가와 매매', '일별 매매 금액 (위 매수 / 아래 매도)',
                        '수익률: 떨사오팔 vs SOXL 단순 보유'),
    )

    # --- 1행: 종가 + 매매 마커 ---
    fig.add_trace(go.Scatter(
        x=dates, y=close, name='SOXL 종가', mode='lines',
        line=dict(color=C_PRICE, width=1.6),
        hovertemplate='%{x|%Y-%m-%d}<br>종가 $%{y:.2f}<extra></extra>',
    ), row=1, col=1)

    def state_text(row):
        return (f"보유 {int(row['보유 주식 수'])}주 · 예수금 ${row['예수금']:,.0f}<br>"
                f"평가금액 ${row['총 평가액']:,.0f} ({row['수익율(%)']:+.2f}%)")

    for col, name, marker, color in [
        ('LOC 매수', 'LOC 매수', 'triangle-up', C_BUY),
        ('수익 실현 매도', '익절 매도', 'triangle-down', C_SELL),
        ('MOC 손절', 'MOC 손절', 'x', C_STOP),
    ]:
        hit = df_res[df_res[col] > 0]
        if hit.empty:
            continue
        fig.add_trace(go.Scatter(
            x=hit.index, y=hit['종가'], name=name, mode='markers',
            marker=dict(symbol=marker, size=11, color=color, line=dict(width=1, color='white')),
            text=[f"{name} {int(r[col])}주 @ ${r['종가']:.2f}<br>{state_text(r)}" for _, r in hit.iterrows()],
            hovertemplate='%{x|%Y-%m-%d}<br>%{text}<extra></extra>',
        ), row=1, col=1)

    # --- 2행: 일별 매매 금액 ---
    for col, name, color, sign in [
        ('LOC 매수', '매수 금액', C_BUY, 1),
        ('수익 실현 매도', '익절 금액', C_SELL, -1),
        ('MOC 손절', '손절 금액', C_STOP, -1),
    ]:
        hit = df_res[df_res[col] > 0]
        if hit.empty:
            continue
        amt = hit[col] * hit['종가']
        fig.add_trace(go.Bar(
            x=hit.index, y=sign * amt, name=name, showlegend=False,
            marker=dict(color=color),
            text=[f"{int(q)}주 x ${p:.2f} = ${a:,.0f}" for q, p, a in zip(hit[col], hit['종가'], amt)],
            hovertemplate='%{x|%Y-%m-%d}<br>%{text}<extra>' + name + '</extra>',
        ), row=2, col=1)

    # --- 3행: 수익률 비교 ---
    fig.add_trace(go.Scatter(
        x=dates, y=df_res['수익율(%)'], name='떨사오팔 수익률', mode='lines',
        line=dict(color=C_RET, width=2.2),
        customdata=df_res['총 평가액'],
        hovertemplate='%{x|%Y-%m-%d}<br>떨사오팔 %{y:+.2f}%<br>평가금액 $%{customdata:,.0f}<extra></extra>',
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=dates, y=hold_ret, name='SOXL 단순 보유', mode='lines',
        line=dict(color=C_HOLD, width=1.4, dash='dot'),
        hovertemplate='%{x|%Y-%m-%d}<br>단순 보유 %{y:+.2f}%<extra></extra>',
    ), row=3, col=1)
    fig.add_hline(y=0, line=dict(color=GRID_COLOR, width=1), row=3, col=1)

    # --- 헤더 ---
    n_trades = len(df_trades)
    win_rate = (len(df_trades[df_trades['수익률(%)'] > 0]) / n_trades * 100) if n_trades else 0
    last = df_res.iloc[-1]
    stats = (f"수익률 {return_rate:+.2f}% (SOXL 단순 보유 {hold_ret.iloc[-1]:+.1f}%)  |  "
             f"평가금액 ${final_value:,.0f}  |  MDD {mdd:.1f}%  |  "
             f"청산 {n_trades}회 · 승률 {win_rate:.0f}%  |  현재 보유 {int(last['보유 주식 수'])}주")
    today_str = dates[-1].strftime('%Y-%m-%d')

    fig.update_layout(
        title=dict(text=f'떨사오팔 {symbol} {year}년 전략 성과 ({start_date} ~ {today_str})<br>'
                        f'<span style="font-size:13px;color:#AAAAAA">{stats}</span><br>'
                        f'<span style="font-size:12px;color:#8b949e">연초 원금 ${initial_funds:,.0f}로 '
                        f'운용했다고 가정한 시뮬레이션 · 실계좌 잔고 아님 · 매년 1월 자동 리셋</span>',
                   x=0.5, xanchor='center', y=0.985, yanchor='top'),
        template='plotly_dark',
        paper_bgcolor=BG_COLOR, plot_bgcolor=BG_COLOR,
        barmode='relative',
        hovermode='closest',
        legend=dict(orientation='h', y=-0.04, yanchor='top', x=0.5, xanchor='center', bgcolor='rgba(0,0,0,0)'),
        margin=dict(t=170, b=80, l=60, r=30),
        height=960,
        xaxis=dict(
            rangeselector=dict(
                buttons=[
                    dict(count=1, label='1개월', step='month', stepmode='backward'),
                    dict(count=3, label='3개월', step='month', stepmode='backward'),
                    dict(count=6, label='6개월', step='month', stepmode='backward'),
                    dict(step='all', label='올해 전체'),
                ],
                bgcolor='#21262d', activecolor='#388bfd', font=dict(color='#E0E0E0'),
            ),
        ),
    )
    fig.update_xaxes(gridcolor=GRID_COLOR, range=[f'{year}-01-01', f'{year}-12-31'],
                     rangebreaks=[dict(bounds=['sat', 'mon'])])
    fig.update_yaxes(title_text='종가 ($)', gridcolor=GRID_COLOR, row=1, col=1)
    fig.update_yaxes(title_text='금액 ($)', gridcolor=GRID_COLOR, row=2, col=1)
    fig.update_yaxes(title_text='수익률 (%)', gridcolor=GRID_COLOR, row=3, col=1)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fig.write_html(output_path, include_plotlyjs='cdn', full_html=True,
                   config={'displayModeBar': True, 'scrollZoom': True})
    print(f'{year}년 대시보드: 수익률 {return_rate:+.2f}% / 단순 보유 {hold_ret.iloc[-1]:+.1f}% / MDD {mdd:.1f}%')


def main():
    config = load_config()
    symbol = config['trading']['symbol']
    start_date = resolve_chart_start(config, symbol)
    build_dashboard(symbol, start_date, config['trading'], 'docs/index.html')
    print('인터랙티브 대시보드 생성 완료: docs/index.html')


if __name__ == '__main__':
    main()
