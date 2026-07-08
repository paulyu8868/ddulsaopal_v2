# aws/handler.py — Lambda 진입점
# GitHub Actions 러너가 하던 일(clone → 태스크 실행 → 결과 commit/push)을 그대로 수행한다.
# 매매 로직은 이미지에 굽지 않고 매 실행마다 main을 clone하므로,
# config.yaml·전략 코드 변경은 PR 머지만으로 다음 실행에 반영된다.
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("SSM_REGION", "ap-northeast-2")
SSM_PREFIX = os.environ.get("SSM_PREFIX", "/ddulsa")
GITHUB_REPO = os.environ.get("GITHUB_REPO", "paulyu8868/ddulsaopal_v2")
WORK_DIR = "/tmp/repo"

TASKS = {
    "morning": {
        "cmd": ["python", "morning_task.py"],
        "mode_arg": True,
        "commit_paths": ["logs/", "data/"],
        "commit_msg": "📊 Morning task logs",
    },
    "evening": {
        "cmd": ["python", "evening_task.py"],
        "mode_arg": True,
        "commit_paths": ["data/trading.db", "logs/"],
        "commit_msg": "📈 Evening update",
    },
    "backfill": {
        "cmd": ["python", "backfill_prices.py"],
        "mode_arg": False,
        "commit_paths": ["data/trading.db"],
        "commit_msg": "🔄 Backfill recent prices",
    },
}

# 기존 워크플로우의 nick-invision/retry(3회, 30초 간격)와 동일한 재시도 정책.
# 시도당 240초 제한 → 최악의 경우에도 Lambda 타임아웃(840초) 안에 끝난다.
MAX_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 30
ATTEMPT_TIMEOUT_SECONDS = 240

REQUIRED_PARAMS = ["APP_KEY", "APP_SECRET", "ACCOUNT_NUMBER", "ACCOUNT_CODE", "GITHUB_TOKEN"]


def _sanitize(text, secret):
    return (text or "").replace(secret, "***")


def _load_params():
    ssm = boto3.client("ssm", region_name=REGION)
    params = {}
    next_token = None
    while True:
        kwargs = {"Path": SSM_PREFIX, "WithDecryption": True}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = ssm.get_parameters_by_path(**kwargs)
        for p in resp["Parameters"]:
            params[p["Name"].rsplit("/", 1)[-1]] = p["Value"]
        next_token = resp.get("NextToken")
        if not next_token:
            break
    missing = [k for k in REQUIRED_PARAMS if not params.get(k) or params[k] == "CHANGE_ME"]
    if missing:
        raise RuntimeError(f"SSM parameters missing or unset: {missing}")
    return params


def _git(args, check=True):
    result = subprocess.run(
        ["git"] + args, cwd=WORK_DIR, capture_output=True, text=True
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"git {args[0]} failed: {result.stderr}")
    return result


def _clone(token):
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    url = f"https://x-access-token:{token}@github.com/{GITHUB_REPO}.git"
    result = subprocess.run(
        ["git", "clone", "--depth", "5", url, WORK_DIR],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git clone failed: {_sanitize(result.stderr, token)}")
    _git(["config", "user.email", "bot@github.com"])
    _git(["config", "user.name", "Trading Bot (Lambda)"])


def _run_task(task_cfg, mode, env):
    cmd = list(task_cfg["cmd"])
    if task_cfg["mode_arg"]:
        cmd.append(mode)
    last_rc = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info("Attempt %d/%d: %s", attempt, MAX_ATTEMPTS, " ".join(cmd))
        try:
            # stdout/stderr를 그대로 흘려보내 CloudWatch Logs에 남긴다
            proc = subprocess.run(cmd, cwd=WORK_DIR, env=env, timeout=ATTEMPT_TIMEOUT_SECONDS)
            last_rc = proc.returncode
        except subprocess.TimeoutExpired:
            last_rc = "timeout"
            logger.error("Attempt %d timed out after %ds", attempt, ATTEMPT_TIMEOUT_SECONDS)
        if last_rc == 0:
            return
        logger.warning("Attempt %d failed (exit=%s)", attempt, last_rc)
        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_WAIT_SECONDS)
    raise RuntimeError(f"Task failed after {MAX_ATTEMPTS} attempts (last exit={last_rc})")


def _commit_and_push(task_cfg, token):
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    msg = f"{task_cfg['commit_msg']} {date_str}"
    for attempt in range(1, 4):
        _git(["add"] + task_cfg["commit_paths"], check=False)
        if _git(["diff", "--staged", "--quiet"], check=False).returncode == 0:
            logger.info("No changes to commit")
            return
        _git(["commit", "-m", msg])
        push = _git(["push", "origin", "HEAD:main"], check=False)
        if push.returncode == 0:
            logger.info("Pushed results to main")
            return
        logger.warning("Push failed (attempt %d): %s", attempt, _sanitize(push.stderr, token))
        _git(["fetch", "origin", "main"], check=False)
        _git(["reset", "--soft", "origin/main"], check=False)
    raise RuntimeError("git push failed after 3 attempts")


def lambda_handler(event, context):
    event = event or {}
    task = event.get("task")
    mode = event.get("mode", "dry-run")
    if task not in TASKS:
        raise ValueError(f"Unknown task: {task!r} (expected one of {sorted(TASKS)})")
    if mode not in ("dry-run", "live"):
        raise ValueError(f"Unknown mode: {mode!r} (expected dry-run or live)")

    task_cfg = TASKS[task]
    params = _load_params()
    token = params["GITHUB_TOKEN"]

    os.environ["HOME"] = "/tmp"
    _clone(token)

    env = {
        **os.environ,
        "APP_KEY": params["APP_KEY"],
        "APP_SECRET": params["APP_SECRET"],
        "ACCOUNT_NUMBER": params["ACCOUNT_NUMBER"],
        "ACCOUNT_CODE": params["ACCOUNT_CODE"],
    }
    env.pop("GITHUB_TOKEN", None)

    logger.info("Running task=%s mode=%s", task, mode)
    _run_task(task_cfg, mode, env)
    _commit_and_push(task_cfg, token)
    return {"task": task, "mode": mode, "status": "ok"}
