# CLAUDE.md — ddulsaopal_v2 자동매매 봇 작업 지침

> 이 파일은 Claude Code가 이 레포에서 작업할 때 반드시 따라야 하는 운영 매뉴얼이다.
> **실제 자금이 투입된 실거래 봇**이므로, 모든 작업은 아래 규칙과 주의사항을 전제로 한다.
> ⚠️ 이 레포는 **public**이다. 이 파일·커밋·PR에 비밀키, 토큰, 계좌번호, 이메일 등 개인정보를 쓰지 말 것.
>
> 최종 현행화: 2026-11-23 (AWS 이관·캠페인 리셋·주문 정지·대시보드 v3 반영, 코드와 대조 검증)

---

## 0. 최우선 원칙 (작업 전 항상 확인)

1. **실거래 봇이다.** 코드 한 줄이 실제 돈의 매수/매도로 이어진다. 추측으로 고치지 말 것.
2. **모든 변경은 PR로.** `main` 브랜치에 직접 push 금지. 소유자가 리뷰·머지한다. (봇의 데이터·로그 자동 커밋은 예외)
3. **dry-run 먼저, live 나중.** 매매 동작에 영향을 주는 변경은 dry-run 검증 후에만 live 반영 (방법은 7번).
4. **실패는 시끄럽게.** 중요 실패가 조용히 넘어가면 안 된다. 단, **현재 코드는 이 원칙을 완전히 지키지 못한다** (5번 "아직 없는 안전장치" 참조).
5. **최소 수정 원칙.** 넓은 리팩토링 금지. 문제 지점만 surgical하게 고친다. 새 변수를 함부로 도입하지 않는다.
6. **실행은 AWS, 코드·리뷰·데이터는 GitHub.** 스케줄 실행은 AWS Lambda(서울 리전)가 하고, 레포는 코드·PR·데이터 저장소(레포 = DB) 역할이다.
7. **불확실하면 멈추고 물어본다.** 특히 KIS API 필드, 거래소 코드, 날짜 처리, 수수료/매도 기준은 추정 금지 — 확실치 않으면 변경하지 말고 보고한다.
8. **로그는 사실의 기록이지 장부가 아니다.** "Holdings/Funds" 로그는 시뮬레이션 리플레이 값이다. 실계좌 확인은 소유자의 KIS 앱이 유일한 근거다 (5번-10).

---

## 1. 프로젝트 개요

SOXL(반도체 3배 레버리지 ETF)에 **떨사오팔(떨어지면 사고 오르면 판다)** 평균회귀 전략을 적용하는 개인 자동매매 시스템.

- **브로커/API:** 한국투자증권(KIS) Open API (실거래 전용)
- **주문 방식:** LOC(Limit on Close) / MOC(Market on Close)
- **거래소:** SOXL은 AMEX 상장 (가격조회 EXCD=`AMS`, 주문 `OVRS_EXCG_CD=AMEX`)
- **실행:** AWS EventBridge Scheduler → Lambda `ddulsa-trading-bot` (ap-northeast-2). 상세는 [aws/README.md](aws/README.md)
- **데이터:** SQLite `data/trading.db` (가격 이력) — 실행 결과를 레포에 커밋해서 보존
- **대시보드:** GitHub Pages (`docs/index.html`) — 연간 전략 성과표 (8번)
- **보조 전략:** 침몰방지법(prevent_drown) — 백테스트(`침몰방지법.py`)에서만 사용
- **관련 레포:** Streamlit 백테스트 앱(별도 레포)이 이 레포의 `trading.db`를 raw GitHub URL로 가져다 씀

---

## 2. 매매 전략 로직 (떨사오팔)

실거래 주문 계산의 실제 경로는 **`backtest_today.py`의 `infinite_buy_today()`**다. 매일 `config.yaml`의 `start_date`부터 DB 종가로 전 구간을 다시 리플레이해서 오늘 주문을 낸다. (`backtest_all.py`는 분석·차트용 — 혼동 금지)

### 매수 (LOC)
- **트리거:** 당일 종가 ≤ 전날 종가 → LOC 매수
- **주문가:** 전날 종가
- **수량(복리, welfare=true):** `int( (예수금 / (buy_portion - T)) / 전날종가 )`, `T = 보유 회차 수`
- **수량(단리, welfare=false):** `int( (initial_funds / buy_portion) / 전날종가 )`
- **조건:** 수수료 포함 금액이 예수금 이하일 때만
- **시작일 규칙:** `start_date` 당일은 기준일이라 매수하지 않는다 — 매매는 다음 거래일부터 (`i > start_idx`, PR #14)

### 매도
- **수익 실현(LOC):** 지정가 = `매수체결가 × (1 + (fee_rate/100) × 2)`. **이 기준가는 `fee_rate`에 직접 연동된다** (4번)
- **MOC 손절:** 보유일 기준. 일수 처리가 파일마다 미세하게 다르니 건드릴 땐 의도적으로 맞출 것:
  - `backtest_today.py` 리플레이: `days >= 40`이면 청산 처리
  - `backtest_today.py` 주문 생성: `days < 39`면 LOC 매도, 그 외(39일차부터)는 **MOC 주문 제출**
  - `backtest_all.py`(분석용): `days >= 39`
- 회차(`buy_records`)별 개별 관리, `data/{mode}_buy_records.pkl`에 저장. **매 실행마다 start_date부터 재계산해 덮어쓰는 파생 상태** — 수동 편집·초기화 불필요

### 침몰방지법(보조, 백테스트 전용)
- 매수: `(1.06 - 0.01×min(T,6))` 기준 / 매도: `(1.125 - 0.02×min(T,6))` / 최대보유 `(30 - 3×min(T,6))`일, T≥6 "회복모드"
- 실거래에 끌어오려면 별도 검증 필요

### 전략 특성 (실전에서 확인됨)
- 지속 상승장에서는 +0.5%마다 익절해 상승분을 거의 못 탄다 (언더퍼폼)
- 진짜 리스크는 **장기 하락장**: 고점 부근에서 회차를 다 채우고 폭락을 그대로 맞는다
- 2026년 실측(1/2~11/20 시뮬레이션): 떨사오팔 **-66.4%**, 승률 94% vs SOXL 단순 보유 -34.6%. 작은 이익을 자주 내고 큰 손실 한 번으로 잃는 구조. 전략 변경 논의 시 이 근거를 반영할 것

---

## 3. 파일 구조 및 역할

| 파일 | 역할 | 실거래 영향 |
|---|---|---|
| `config.yaml` | 핵심 파라미터 | **직접 영향** (4번) |
| `morning_task.py` | 주문 계산·제출 진입점 (`dry-run`/`live` 인자) | 높음 |
| `evening_task.py` | 종가 업데이트 진입점 | 중 (DB) |
| `backfill_prices.py` | 최근 7일 종가 재수집 안전망 (INSERT OR REPLACE) | 중 (DB) |
| `daily_run.py` | `DailyTrader` — 거래일 판정·주문 계산·제출·종가 업데이트 | **높음** |
| `kis_api.py` | KIS Open API 래퍼 (토큰/시세/주문/잔고) | **매우 높음** |
| `backtest_today.py` | 오늘 넣을 주문 계산 (실거래가 이 로직 사용) | **높음** |
| `backtest_all.py` | 전체 구간 시뮬레이션 (차트·분석용) | 없음 |
| `generate_chart.py` | README 정적 차트 + `resolve_chart_start()` | 없음 |
| `generate_interactive_chart.py` | GitHub Pages 대시보드 | 없음 |
| `침몰방지법.py` | 보조 전략 백테스트 | 없음 |
| `utils.py` | 반올림/등락율/MDD/DB 조회 | 간접 |
| `init_db.py` / `load_data.py` | DB 테이블 생성 / 과거 데이터 적재 | 초기화용 |
| `aws/handler.py` | Lambda 진입점: main clone → 태스크 실행(3회 재시도) → 결과 commit/push | **높음** |
| `aws/Dockerfile` | Lambda 이미지 (의존성만, matplotlib/plotly 제외) | 배포 |
| `.github/workflows/deploy_lambda.yml` | `aws/**`·`requirements.txt` 머지 시 ECR 빌드 → Lambda 갱신 | 배포 |
| `.github/workflows/update_charts.yml` | `data/trading.db` push 시 차트·대시보드 재생성 | 없음 |
| `.github/workflows/daily_trading.yml` | **수동 백업 전용** — 실행 시 live 주문 제출 (7번 경고) | **매우 높음** |

- **저장소:** `data/trading.db`, `data/{mode}_buy_records.pkl` (둘 다 커밋됨), `data/kis_token.pkl` (토큰 캐시, gitignore)
- **로그:** `logs/dry-run/`, `logs/live/` — 일별 로그, 연간 통합 로그(`trading_history_{year}.log`), 주문 이력(`orders_history.txt`). 커밋됨
- **.gitignore:** `.env`, `__pycache__/`, `*.pyc`, `data/kis_token.pkl` 뿐이다. **비밀키·토큰을 커밋하지 말 것.**

---

## 4. config.yaml 설정 및 주의사항 ⚠️ (가장 사고 잦은 영역)

```yaml
trading:
  symbol: 'SOXL'
  initial_funds: 20500       # 원금 (2026-08-03 리셋)
  buy_portion: 7             # 분할 회차 수
  fee_rate: 0.25             # 매도 기준가에 직접 연동 (아래 주의)
  welfare: true              # 복리 재투자 (daily_run.py가 config 값을 그대로 사용)
  start_date: '2026-07-31'   # 반드시 과거 거래일

chart:
  start_date: 'ytd'          # 차트·대시보드 표시 전용 (실거래 무관)
```

### `trading.start_date` — 가장 중요
- **반드시 "과거의 실제 거래일".** 오늘·미래 날짜 금지 — 데이터 구간이 역전되어 `TypeError: object of type 'NoneType' has no len()` (과거 실제 사고)
- "오늘부터 새로 시작" = **직전 거래일**을 넣는다 (시작일 당일은 매매하지 않으므로 다음 거래일부터 매매)
- 변경 시 ① 과거인지 ② NYSE 거래일인지 `pandas_market_calendars`로 검증 후 PR
- 캠페인 리셋 절차(2026-08-03 선례): 기존 보유를 소유자가 수동 정리 → 실제 예수금을 `initial_funds`로 → `start_date` = 직전 거래일. pkl·로그·DB는 손댈 필요 없음

### `fee_rate`
- 매도 기준가 = `매수체결가 × (1 + (fee_rate/100) × 2)`. 현재 0.25 → 약 **+0.5%** 상승 시 매도
- 실제 KIS 수수료는 약 0.044%/leg — 의미·목적이 다를 수 있으니, 변경 요청 시 "매도 기준가가 바뀐다"를 명시하고 의도 재확인

### `initial_funds` / `buy_portion` / `welfare`
- `initial_funds`: 복리 회차 금액의 기준. 운영 중 변경 시 실계좌 예수금과의 정합성 확인. 리플레이 장부는 이 값으로 시작하므로 실잔고보다 크면 주문이 잔고 부족으로 거부됨
- `buy_portion`: 추천 7~10. 매수 단위 금액을 바꾸므로 신중히
- `welfare`: `true`(복리) 권장

### `chart.start_date` (표시 전용)
- `'ytd'`: DB 최신 종가가 속한 해의 첫 거래일 → 새해 첫 종가가 들어오는 날 자동 리셋 (그 전까지 전년도 결산 유지)
- `'YYYY-MM-DD'`: 지정 날짜 / `null`: `trading.start_date`를 따라감

---

## 5. 과거 발생 오류 및 교훈 (절대 재발 금지)

1. **cron 불일치 버그 (silent skip, GH Actions 시절)** — 잡의 `if: github.event.schedule == '...'` 문자열이 schedule cron과 달라 스케줄 실행이 전부 조용히 스킵됨. 현재 `daily_trading.yml`은 schedule 트리거가 없어 해당 없지만, 되살릴 경우 cron과 `if` 조건을 반드시 같은 문자열로.
2. **GitHub Actions 크론 지연 + 미국 러너 → KIS 타임아웃** — 크론이 +1~4시간 늦고, 최근 50회 중 20%가 `openapi.koreainvestment.com:9443` 접속 타임아웃으로 실패, 2026-07-01엔 주문 미제출. → **실행을 서울 리전 Lambda + EventBridge로 이관** (PR #7~#10, 2026-07-09).
3. **evening 날짜 버그 ①** — KST 자정 이후 실행 시 미국 "오늘" 종가를 조회해 실패. → 항상 하루를 빼고 최대 7일 거슬러 최근 유효 거래일을 찾도록 수정 (DB에 있으면 스킵).
4. **evening 날짜 버그 ② (지연이 버그를 가린 사례)** — ③의 "미국 오늘 − 1" 로직은 **ET 자정 이후 실행이 전제**. GH 크론의 만성 지연 덕에 우연히 맞았고, Lambda 정시 실행(ET 전날 21:00) 첫날 7/8 종가가 누락됨. → evening/backfill 스케줄을 **ET 00:30 / 02:30**으로 고정 (코드 무변경). **절대 ET 자정 이전으로 되돌리지 말 것.**
5. **LOC/MOC 주문 시간 제약** — KIS는 대략 장 시작~마감 20분 전(ET)에만 접수, 밖이면 `[API MOC 또는 LOC 주문불가 시간]` 거부. 현재 morning은 ET 09:31 정시 실행이라 윈도우 안에 들어간다.
6. **첫날 가상 매수 (PR #14)** — 리플레이가 `start_date` 당일도 매매해, 그날 종가 ≤ 전일 종가면 실제로 산 적 없는 회차가 장부에 생기고 다음 날 **없는 주식의 매도 주문**이 나감. → 시작일은 기준일만 담당하도록 수정.
7. **SOXL + SOXS 동시 운용 = 헷지 아님** — 추세장에서 한쪽이 계속 매수 신호를 띄워 손실 증폭. 헷지 목적 SOXS 동시 적용은 거부됨.
8. **yfinance 사용 금지** — 데이터 품질 문제. KIS API 데이터만 사용.
9. **Lambda 이미지 빌드 실패** — pillow 12.x가 Lambda 베이스(AL2, glibc 2.26)용 휠 미제공. → 이미지에서 차트 전용 의존성(matplotlib/plotly) 제외 (PR #9). 이미지에 의존성 추가 시 AL2 휠 제공 여부 확인.
10. **로그 스냅샷 ≠ 장부** — 로그의 보유·잔고는 기록 당시 config로 계산된 값이 섞여 쌓인다. 일관된 수치가 필요하면 현재 config로 재계산(`run_campaign_simulation`). 또 "Order placed successfully"는 **접수**일 뿐 체결이 아니다. `kis_api.py`의 `get_account_balance()`·`get_orders()`는 구현돼 있으나 어디서도 호출하지 않는다 — **장부와 실계좌를 대사하는 코드는 없다.**
11. **주문 정지 중 리플레이 착시** — 주문을 멈춰도 리플레이는 "계속 매매했다고 가정"하고 계산한다. 2026-09-04 정지 후 대시보드가 -74%(가상)를 표시했지만 실계좌는 약 -6.5%. → 대시보드를 YTD 전략 성과표로 전환하고 "실계좌 아님"을 명시 (PR #15).
12. **데이터 검증 원칙** — 거래 로직의 실제 구현은 채팅 요약이 아니라 **프로젝트 파일을 직접 확인**해 검증. 추정으로 단정하지 말 것.

### 아직 없는 안전장치 (문서상 있다고 오해하기 쉬움)
- **`sys.exit(1)` 없음** — `run_morning_task`/`run_evening_task`는 내부 예외를 잡아 로그만 남기고 정상 종료한다. Lambda 실행 성공 ≠ 주문 제출 성공. CloudWatch 경보는 프로세스 크래시(토큰·네트워크·clone 실패 등)만 잡는다.
- **주문 시간 윈도우 가드 없음** — 시간 제약은 스케줄 정시성(ET 09:31)에만 의존한다.
- **일별 중복 실행 방지(dedup 플래그) 없음** — Lambda 비동기 재시도 0회 + 스케줄러 재시도 0회로 중복 호출을 막고 있을 뿐이다. 같은 날 morning을 두 번 실행하면 주문이 두 번 나간다.
- 이것들을 추가하자는 제안은 매매 동작 변경이므로 소유자 승인 후 PR로.

---

## 6. KIS API 세부 주의사항 ⚠️ (주문 실패 시 우선 점검)

`kis_api.py` 현재 상태 (2026-11-23 코드 확인):

- **주문 TR_ID:** 매수 `TTTT1002U`, 매도 `TTTT1006U` (미국 주식 실거래용 — 일본용 `JTTT` 아님, 수정 완료)
- **해시키:** `/uapi/hashkey` 엔드포인트에서 발급 (`_get_hashkey`, 수정 완료)
- **주문 body:** `CANO`, `ACNT_PRDT_CD`, `OVRS_EXCG_CD=AMEX`, `PDNO`, `ORD_QTY`, `OVRS_ORD_UNPR`(소수점 2자리), `ORD_DVSN`(LOC=`34`, MOC=`33`), `ORD_SVR_DVSN_CD=0`. `SLL_BUY_DVSN_CD`는 없음. 거부 시 `CTAC_TLNO`, `MGCO_APTM_ODNO` 필요 여부 검토
- **거래소 코드 불일치 주의:** 가격 조회 `EXCD=AMS` / 주문 `OVRS_EXCG_CD=AMEX` / **잔고 조회(`JTTT3012R`)는 `OVRS_EXCG_CD=NASD`** ← 의도된 것인지 미확인
- **토큰:** `data/kis_token.pkl` 캐시 (Lambda에선 매 실행 새 clone이라 매번 신규 발급). rate limit(`EGW00133`) 대기·재시도 로직 있음
- **자격증명:** `APP_KEY`, `APP_SECRET`, `ACCOUNT_NUMBER`, `ACCOUNT_CODE`
  - Lambda: AWS SSM Parameter Store `/ddulsa/*` (SecureString) + 결과 push용 `GITHUB_TOKEN`(fine-grained PAT, 이 레포 Contents 읽기/쓰기만)
  - 수동 백업 워크플로우: GitHub Secrets
  - 코드·로그·PR에 노출 금지. KIS 앱키를 재발급하면 **SSM과 GitHub Secrets 양쪽을 동시에** 교체

---

## 7. 개발·운영 워크플로우 규칙

### 실행 구조
```
EventBridge (America/New_York 기준 정시, 서머타임 자동)
  → Lambda ddulsa-trading-bot (ap-northeast-2, 컨테이너 이미지, timeout 840s)
      ① main shallow clone  ② python {task} 실행 (3회 재시도)  ③ data/·logs/ commit → main push
  실패(크래시) 시 → CloudWatch 경보 ddulsa-trading-bot-errors → SNS ddulsa-alerts (이메일)
```
- 매매 코드는 이미지에 포함되지 않는다. **config·전략 변경은 PR 머지만으로 다음 실행부터 반영**되고 재배포가 필요 없다.
- `aws/**`·`requirements.txt` 변경 머지 시에만 `deploy_lambda.yml`이 이미지 재빌드(OIDC, main 한정).
- 롤백: EventBridge 스케줄 비활성화 → 필요 시 `daily_trading.yml`로 수동 대응.

### dry-run 검증 방법
- payload의 `mode`만 바꿔 Lambda를 직접 호출:
  ```
  aws lambda invoke --region ap-northeast-2 --function-name ddulsa-trading-bot \
    --cli-binary-format raw-in-base64-out --payload '{"task":"morning","mode":"dry-run"}' out.json
  ```
  dry-run도 결과(`logs/dry-run/`, `data/dry-run_buy_records.pkl`)를 main에 push한다. 주문은 제출하지 않는다.
- 또는 로컬에서 `infinite_buy_today()`를 직접 호출해 오늘 주문을 재현 (KIS 호출 없이 계산만). 회귀 검증은 과거 실제 주문(`logs/live/orders_history.txt`)을 가격·수량까지 재현하는지로 확인한다 (PR #14 선례).

### ⚠️ 수동 실행 금지 사항
- **`daily_trading.yml`의 `workflow_dispatch`는 morning·evening·backfill 세 잡을 모두 실행하고, morning은 `live`로 하드코딩**되어 있다 (모드 선택 입력 없음). 상태 확인용으로 절대 실행하지 말 것 — 실주문이 중복 제출된다.
- 상태 확인은 읽기 전용으로만: `gh run view`, 레포의 로그 파일, CloudWatch 로그, `aws scheduler get-schedule`.

### 기타
- 동시성: Lambda 비동기 재시도 0회, 스케줄러 재시도 0회. push 충돌은 `git reset --soft` 재시도(최대 3회).
- 로깅: mode 분리 폴더에 일별/연간/주문 이력 기록.

---

## 8. 스케줄 (EventBridge, 모두 America/New_York 기준)

| 스케줄 | cron | KST (서머타임 / 겨울) | payload | 비고 |
|---|---|---|---|---|
| `ddulsa-morning` | `31 9 * * ? *` | 22:31 / 23:31 | `{"task":"morning","mode":"live"}` | 개장 1분 후. 휴장일은 코드가 스킵 |
| `ddulsa-evening` | `30 0 * * ? *` | 13:30 / 14:30 | `{"task":"evening","mode":"live"}` | **반드시 ET 자정 이후** (5번-4) |
| `ddulsa-backfill` | `30 2 * * ? *` | 15:30 / 16:30 | `{"task":"backfill",...}` | 최근 7일 종가 안전망 |

- 종가가 DB에 push되면 `update_charts.yml`이 README 차트와 대시보드를 재생성한다.
- 대시보드(`docs/index.html`)는 **연초 원금 운용 가정 YTD 시뮬레이션**이다 — 실계좌 잔고가 아니다.

---

## 9. 현재 운영 상태 (2026-11-23 기준 — 상태가 바뀌면 이 절을 갱신할 것)

- **주문 일시정지 중 (2026-09-04~):** `ddulsa-morning` = **DISABLED**. evening/backfill은 가동 중(종가·차트 갱신 유지).
- 정지 시점 장부: 보유 19주 @ $129.10 (2026-08-18 매수 회차), 예수금 약 $18,587. 정지 중엔 이 회차의 익절·손절 주문도 나가지 않는다. 그 사이 소유자가 계좌에서 직접 거래했을 수 있으니 실제 보유는 소유자에게 확인할 것.
- **재개 시 그냥 켜면 안 된다.** 리플레이 장부는 정지 기간에도 매매했다고 가정하므로 현재 장부는 실계좌와 크게 다르다(2026-11 기준 장부 154주·예수금 $560 vs 실제 19주). 이 상태로 켜면 없는 주식의 매도 주문과 엉뚱한 수량의 매수 주문이 나간다.
  - 재개 절차: ① 소유자가 실계좌 보유 정리(또는 현황 확인) ② 실제 예수금으로 `initial_funds`, 직전 거래일로 `start_date` 설정(4번 리셋 절차) ③ 로컬 재현으로 오늘 주문이 실계좌와 맞는지 확인 ④ PR 머지 후 `ddulsa-morning` 활성화.

---

## 10. 작업 시 체크리스트 (매매 영향 변경 전 필수)

매매 동작·주문·config·KIS API·스케줄에 영향을 주는 변경이라면 PR 설명에 아래를 포함한다.

- [ ] 변경 전/후 값과 변경 이유 명시
- [ ] `start_date` 변경이면: 과거의 실제 NYSE 거래일인지 검증했는가 (오늘 날짜 금지)
- [ ] `fee_rate`/매도 기준 변경이면: 매도 트리거 변화량을 명시했는가
- [ ] KIS API 필드/TR_ID/거래소 코드 변경이면: 공식 문서로 재확인했는가
- [ ] 스케줄 변경이면: evening/backfill이 ET 자정 이후인지, morning이 LOC/MOC 접수 윈도우 안인지 확인했는가
- [ ] dry-run(Lambda payload 또는 로컬 재현)으로 검증했는가, 회귀(과거 실제 주문 재현)를 확인했는가
- [ ] `main` 직접 push가 아니라 PR로 제출하는가
- [ ] 비밀키·토큰·계좌정보·이메일이 코드/로그/PR에 노출되지 않는가 (public 레포)

> 위 항목 중 하나라도 불확실하면 **변경을 멈추고 소유자에게 보고**한다.

---

## 11. 새 PC에서 작업 이어가기

1. `gh auth login` → `gh repo clone paulyu8868/ddulsaopal_v2`
2. `aws configure` (리전 `ap-northeast-2`) — 소유자의 IAM 사용자 액세스 키
3. 로컬 시뮬레이션이 필요하면 Python + `pip install -r requirements.txt` (스크립트 실행 시 레포 루트를 `PYTHONPATH`에)
4. Docker 불필요 — 이미지 빌드는 GitHub Actions가 한다
5. 이 파일이 레포 루트에 있으므로 Claude Code를 레포에서 열면 자동으로 로드된다
