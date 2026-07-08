# AWS Lambda 이관 (1안: 실행만 AWS, 저장은 git 유지)

GitHub Actions 스케줄 실행의 두 가지 고질 문제(크론 지연 +1~4시간, 미국 러너 IP → KIS API 간헐 타임아웃)를
해결하기 위해 **실행만** 서울 리전 Lambda로 옮긴다. 코드·PR 리뷰·데이터(trading.db, 로그)는 지금처럼 GitHub에 남는다.

## 구조

```
EventBridge Scheduler (정시 cron 3개)
  → Lambda ddulsa-trading-bot (ap-northeast-2, 컨테이너 이미지)
      ① main 브랜치 shallow clone   ← config.yaml 변경은 머지만 하면 자동 반영
      ② python {morning|evening|backfill}_task 실행 (3회 재시도)
      ③ data/·logs/ 변경분 commit → main push   ← 지금과 동일하게 커밋이 쌓임
  실패 시 → CloudWatch 경보 → SNS 이메일
```

- 자격증명: SSM Parameter Store `/ddulsa/*` (APP_KEY, APP_SECRET, ACCOUNT_NUMBER, ACCOUNT_CODE, GITHUB_TOKEN)
- 배포: `aws/` 또는 `requirements.txt` 변경이 main에 머지되면 `deploy_lambda.yml`이 ECR 빌드 → Lambda 업데이트
- 매매 코드는 이미지에 포함되지 않으므로 전략·config 변경에는 재배포가 필요 없다
- 차트 생성(generate_chart.py 등)은 미사용 중이라 이관에서 제외됨

## 스케줄 (EventBridge)

| 이름 | cron | 기존 대비 |
|---|---|---|
| ddulsa-morning | `31 9 ? * * *` America/New_York | UTC 14:31 절충 불필요 — 연중 개장 1분 후 정시 |
| ddulsa-evening | `0 1 * * ? *` UTC | 동일 시각, 정시 보장 |
| ddulsa-backfill | `0 3 * * ? *` UTC | 동일 시각, 정시 보장 |

payload 예: `{"task": "morning", "mode": "dry-run"}` — live 전환은 payload의 mode만 변경.

## 수동 실행

```
aws lambda invoke --function-name ddulsa-trading-bot \
  --payload '{"task":"morning","mode":"dry-run"}' --cli-binary-format raw-in-base64-out /dev/stdout
```

## 롤백

EventBridge 스케줄 3개를 비활성화하고 `.github/workflows/daily_trading.yml`의 스케줄 크론을 되살리면 끝.
