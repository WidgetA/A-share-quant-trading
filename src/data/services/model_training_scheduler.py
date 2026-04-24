# === MODULE PURPOSE ===
# Background scheduler for ML model training and fine-tuning.
# Runs every 20 trading days at 3am Beijing time: triggers FC serverless
# training, receives callback with trained model, saves to disk.

from __future__ import annotations

import asyncio
import logging
import secrets
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BEIJING_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULE_HOUR = 3  # 3am Beijing time
MAX_DATA_FIX_ATTEMPTS = 3
FULL_MODEL_NAME = "full_latest"
MODEL_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "models"
LAST_TRAIN_FILE = MODEL_DIR / ".last_finetune_date"
LAST_SUCCESS_FILE = MODEL_DIR / ".last_success_time"
MODEL_META_FILE = MODEL_DIR / ".model_meta.json"


async def _notify_feishu(message: str) -> None:
    """Best-effort Feishu notification, never raises."""
    try:
        from src.common.feishu_bot import FeishuBot

        bot = FeishuBot()
        if bot.is_configured():
            await bot.send_message(message)
    except Exception:
        logger.warning("Failed to send Feishu notification", exc_info=True)


async def _get_trading_calendar(start_date: date, end_date: date) -> list[date]:
    """Get trading days via Tushare trade_cal, fallback to weekdays."""
    try:
        from src.data.clients.tushare_realtime import get_tushare_trade_calendar

        sd = start_date.strftime("%Y-%m-%d")
        ed = end_date.strftime("%Y-%m-%d")
        date_strs = await get_tushare_trade_calendar(sd, ed)
        return sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in date_strs)
    except Exception as e:
        logger.warning(f"Tushare trade_cal failed: {e}, using weekday fallback")
        days = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                days.append(current)
            current += timedelta(days=1)
        return days


def _get_last_finetune_date() -> date | None:
    """Read the last finetune date from marker file."""
    if LAST_TRAIN_FILE.exists():
        try:
            text = LAST_TRAIN_FILE.read_text(encoding="utf-8").strip()
            return datetime.strptime(text, "%Y-%m-%d").date()
        except (ValueError, OSError):
            pass
    return None


def _set_last_finetune_date(d: date) -> None:
    """Write the last finetune date to marker file."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    LAST_TRAIN_FILE.write_text(d.strftime("%Y-%m-%d"), encoding="utf-8")


def _get_last_success_time() -> str | None:
    """Read the last successful training time from marker file."""
    if LAST_SUCCESS_FILE.exists():
        try:
            return LAST_SUCCESS_FILE.read_text(encoding="utf-8").strip()
        except OSError:
            pass
    return None


def _set_last_success_time(mode: str, model_name: str) -> None:
    """Write the last successful training time to marker file."""
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    now = datetime.now(BEIJING_TZ)
    label = "全量" if mode == "full" else "微调"
    text = f"{now.strftime('%Y-%m-%d %H:%M')}|{label}|{model_name}"
    LAST_SUCCESS_FILE.write_text(text, encoding="utf-8")


def _write_model_meta(s3_key: str, s3_last_modified: str) -> None:
    """Write model metadata after downloading from S3."""
    import json
    import re

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    # Extract training date from S3 key, e.g. "models/full_20260406.lgb" → "2026-04-06"
    training_date: str | None = None
    m = re.search(r"(\d{4})(\d{2})(\d{2})\.lgb$", s3_key)
    if m:
        training_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    meta = {
        "s3_key": s3_key,
        "s3_last_modified": s3_last_modified,
        "training_date": training_date,
        "downloaded_at": datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }
    MODEL_META_FILE.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")


def get_model_meta() -> dict | None:
    """Read model metadata. Returns None if not available."""
    import json

    if not MODEL_META_FILE.exists():
        return None
    try:
        return json.loads(MODEL_META_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


class ModelTrainingScheduler:
    """Background scheduler for auto fine-tuning every 20 trading days at 3am.

    Usage:
        scheduler = ModelTrainingScheduler(app.state)
        task = asyncio.create_task(scheduler.run())
        # On shutdown:
        task.cancel()
    """

    def __init__(self, app_state) -> None:
        self._app_state = app_state
        self.next_run_time: str | None = None
        self.last_run_time: str | None = None
        self.last_run_result: str | None = None
        self.last_run_message: str | None = None
        self.training_in_progress: bool = False
        self.training_log: list[str] = []
        self._training_tokens: dict[str, dict] = {}
        self._pending_results: dict[str, dict] = {}  # token -> {"event", "result"}
        self.s3_has_full_model: bool = False  # updated by _ensure_local_model_from_s3
        self._ensure_local_model_from_s3()

    def _ensure_local_model_from_s3(self) -> None:
        """On startup, always sync latest model from S3.

        S3 is the single source of truth. Local is just a disk cache
        required by LightGBM (which only loads from file paths).
        Always downloads the latest model to ensure freshness.
        """
        try:
            from src.common.s3_client import create_s3_client_from_config

            s3 = create_s3_client_from_config()
            if not s3:
                return

            import asyncio

            async def _check_and_download():
                models = await s3.list_models(prefix="models/")
                full_models = [m for m in models if "full_" in m["key"]]
                if not full_models:
                    logger.info("No model in S3 yet")
                    return
                self.s3_has_full_model = True
                latest = sorted(full_models, key=lambda m: m["last_modified"])[-1]
                full_model_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
                MODEL_DIR.mkdir(parents=True, exist_ok=True)
                await s3.download_file(latest["key"], full_model_path)
                _write_model_meta(latest["key"], latest["last_modified"])
                logger.info("Synced model from S3: %s", latest["key"])

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(_check_and_download())
            except RuntimeError:
                asyncio.run(_check_and_download())
        except Exception as e:
            logger.warning("S3 model check on startup failed (non-fatal): %s", e)

    async def _refresh_s3_flag(self) -> None:
        """Query S3 to update s3_has_full_model. S3 is single source of truth."""
        try:
            from src.common.s3_client import create_s3_client_from_config

            s3 = create_s3_client_from_config()
            if not s3:
                return
            models = await s3.list_models(prefix="models/")
            self.s3_has_full_model = any("full_" in m["key"] for m in models)
        except Exception as e:
            logger.warning("S3 flag refresh failed: %s", e)

    def _generate_training_token(self, mode: str) -> str:
        """Generate a one-time token for FC callback authentication."""
        token = secrets.token_urlsafe(32)
        now = datetime.now(BEIJING_TZ)
        self._training_tokens[token] = {"mode": mode, "created_at": now}
        # Clean up tokens older than 1 hour
        cutoff = now - timedelta(hours=1)
        self._training_tokens = {
            k: v for k, v in self._training_tokens.items() if v["created_at"] > cutoff
        }
        return token

    def validate_and_consume_token(self, token: str) -> str | None:
        """Validate a training token. Returns mode if valid, None otherwise.

        Data tokens are NOT consumed (FC async may retry the entire function).
        They are protected by 1-hour expiry instead.
        """
        info = self._training_tokens.get(token)
        if not info:
            logger.warning("validate_token: token %s NOT FOUND", token[:8])
            return None
        if datetime.now(BEIJING_TZ) - info["created_at"] > timedelta(hours=1):
            del self._training_tokens[token]
            logger.warning("validate_token: token %s EXPIRED", token[:8])
            return None
        return info["mode"]

    def receive_training_result(self, token: str, result: dict) -> bool:
        """Receive async training result from FC callback. Returns True if accepted."""
        pending = self._pending_results.get(token)
        if not pending:
            logger.warning("Unknown result token: %s", token[:8])
            return False
        pending["result"] = result
        pending["event"].set()
        logger.info("Training result received for token %s", token[:8])
        return True

    def get_status(self) -> dict:
        """Return scheduler status for dashboard display."""
        full_model_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
        # Parse last success info from file: "2026-04-24 03:15|微调|finetune_20260424"
        last_success = _get_last_success_time()
        last_success_time = None
        last_success_label = None
        if last_success:
            parts = last_success.split("|", 2)
            last_success_time = parts[0]
            last_success_label = parts[1] if len(parts) > 1 else None
        meta = get_model_meta()
        return {
            "next_run_time": self.next_run_time,
            "last_run_time": self.last_run_time,
            "last_run_result": self.last_run_result,
            "last_run_message": self.last_run_message,
            "training_in_progress": self.training_in_progress,
            "has_full_model": full_model_path.exists(),
            "s3_has_full_model": self.s3_has_full_model,
            "current_model": self._get_current_model_name(),
            "model_training_date": meta.get("training_date") if meta else None,
            "model_s3_key": meta.get("s3_key") if meta else None,
            "log_lines": self.training_log[-50:],
            "last_success_time": last_success_time,
            "last_success_label": last_success_label,
        }

    def _get_current_model_name(self) -> str | None:
        """Find the most recent model file."""
        if not MODEL_DIR.exists():
            return None
        lgb_files = sorted(MODEL_DIR.glob("*.lgb"), key=lambda p: p.stat().st_mtime, reverse=True)
        return lgb_files[0].stem if lgb_files else None

    def _append_log(self, msg: str) -> None:
        """Add timestamped entry to training_log (keep last 100 lines)."""
        ts = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self.training_log.append(line)
        if len(self.training_log) > 100:
            self.training_log = self.training_log[-100:]
        logger.info("ModelTraining: %s", msg)

    def _get_storage(self):
        return getattr(self._app_state, "storage", None)

    async def run(self) -> None:
        """Main loop: sleep until 3am, check if 20 trading days elapsed, run finetune."""
        from src.strategy.strategies.ml_scanner import _RETRAIN_INTERVAL_DAYS

        logger.info("ModelTrainingScheduler started")
        try:
            while True:
                now = datetime.now(BEIJING_TZ)
                target = datetime.combine(now.date(), time(SCHEDULE_HOUR, 0), tzinfo=BEIJING_TZ)
                if now >= target:
                    target += timedelta(days=1)

                self.next_run_time = target.strftime("%Y-%m-%d %H:%M")
                wait_secs = (target - now).total_seconds()
                logger.info(
                    "ModelTrainingScheduler: next check at %s (%.1fh)",
                    target.strftime("%Y-%m-%d %H:%M"),
                    wait_secs / 3600,
                )
                await asyncio.sleep(wait_secs)

                run_time = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M")

                # Check if 20 trading days have passed since last finetune
                last_date = _get_last_finetune_date()
                if last_date:
                    today = datetime.now(BEIJING_TZ).date()
                    trading_days = await _get_trading_calendar(last_date, today)
                    elapsed = len([d for d in trading_days if d > last_date])
                    if elapsed < _RETRAIN_INTERVAL_DAYS:
                        self.last_run_time = run_time
                        self.last_run_result = "skipped"
                        self.last_run_message = f"距上次{elapsed}天 < {_RETRAIN_INTERVAL_DAYS}天"
                        self.next_run_time = target.strftime("%Y-%m-%d %H:%M")
                        continue

                # Check if full model exists
                full_model_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
                if not full_model_path.exists():
                    self.last_run_time = run_time
                    self.last_run_result = "no_model"
                    self.last_run_message = "无全量模型，跳过微调"
                    await _notify_feishu("[模型微调] 无全量模型，跳过微调")
                    continue

                try:
                    result = await self.run_finetune()
                    self.last_run_time = run_time
                    if result.get("error"):
                        self.last_run_result = "failed"
                        self.last_run_message = result["error"][:100]
                    else:
                        self.last_run_result = "success"
                        self.last_run_message = f"微调完成: {result.get('model_name', '')}"
                except Exception as e:
                    logger.error("ModelTrainingScheduler finetune failed: %s", e, exc_info=True)
                    self.last_run_time = run_time
                    self.last_run_result = "failed"
                    self.last_run_message = str(e)[:100]
                    await _notify_feishu(f"[模型微调] 微调失败\n{e}")

        except asyncio.CancelledError:
            logger.info("ModelTrainingScheduler cancelled")

    async def run_full_training(self, progress_cb=None) -> dict:
        """Execute full training from scratch.

        Args:
            progress_cb: Optional async callable(msg: str) for progress updates.

        Returns:
            Dict with keys: model_name, model_path, s3_uri, error.
        """
        if self.training_in_progress:
            return {"error": "训练正在进行中"}

        self.training_in_progress = True
        self.training_log.clear()
        try:
            return await self._train(mode="full", progress_cb=progress_cb)
        finally:
            self.training_in_progress = False

    async def run_finetune(self, progress_cb=None) -> dict:
        """Execute fine-tune from existing full model.

        Args:
            progress_cb: Optional async callable(msg: str) for progress updates.

        Returns:
            Dict with keys: model_name, model_path, s3_uri, error.
        """
        if self.training_in_progress:
            return {"error": "训练正在进行中"}

        full_model_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
        if not full_model_path.exists():
            return {"error": "无全量训练模型，请先执行全量训练"}

        self.training_in_progress = True
        self.training_log.clear()
        try:
            return await self._train(
                mode="finetune",
                init_model_path=full_model_path,
                progress_cb=progress_cb,
            )
        finally:
            self.training_in_progress = False

    async def _train(
        self,
        mode: str,
        init_model_path: Path | None = None,
        progress_cb=None,
    ) -> dict:
        """Core training logic — delegates to FC serverless."""
        from src.common.config import get_fc_url

        fc_url = get_fc_url()
        if not fc_url:
            return {"error": "FC 训练端点未配置，请在设置页面配置 FC URL"}
        return await self._train_remote(mode, init_model_path, progress_cb)

    async def _train_remote(
        self,
        mode: str,
        init_model_path: Path | None = None,
        progress_cb=None,
    ) -> dict:
        """Train via FC serverless endpoint."""
        import httpx

        from src.common.config import get_fc_url, get_s3_config

        label = "全量训练" if mode == "full" else "微调"

        async def _log(msg: str):
            self._append_log(msg)
            if progress_cb:
                ts = datetime.now(BEIJING_TZ).strftime("%H:%M:%S")
                await progress_cb(f"[{ts}] {msg}")

        await _log(f"{label}开始 (远程FC)")
        await _notify_feishu(f"[模型训练] {label}开始 (远程FC)")

        # Step 1: Data completeness check
        await _log("检查数据完整性...")
        data_ok = await self._check_data_completeness(progress_cb=_log)
        if not data_ok:
            error = "数据不完整，已尝试补全3次仍失败"
            await _log(f"中止: {error}")
            await _notify_feishu(f"[模型{label}] {error}，跳过本次{label}")
            return {"error": error}

        # Step 2: Generate callback URLs for FC
        base_url = getattr(self._app_state, "web_base_url", "http://localhost:8000")
        data_token = self._generate_training_token(mode)
        callback_url = f"{base_url}/api/model/training-data?token={data_token}"
        result_token = self._generate_training_token(mode)
        result_callback_url = f"{base_url}/api/model/training-result?token={result_token}"
        await _log(f"回调URL已生成 ({base_url})")

        # Prepare async event for result delivery
        event = asyncio.Event()
        self._pending_results[result_token] = {"event": event, "result": None}

        # Step 3: Build lightweight payload (no daily_data, no model bytes!)
        # FC downloads init model from S3 directly for finetune
        payload: dict = {
            "mode": mode,
            "callback_url": callback_url,
            "result_callback_url": result_callback_url,
        }

        s3_config = get_s3_config()
        if s3_config:
            # FC runs on public internet, convert internal VPC endpoint to public
            fc_s3 = dict(s3_config)
            ep = fc_s3.get("endpoint_url", "")
            if "-internal." in ep:
                fc_s3["endpoint_url"] = ep.replace("-internal.", ".")
            payload["s3_config"] = fc_s3

        # Step 4: POST async trigger to FC
        fc_url = get_fc_url()
        assert fc_url, "FC URL not configured"  # guaranteed by _train() caller
        await _log("发送异步训练请求到 FC...")
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                resp = await client.post(
                    fc_url,
                    json=payload,
                    headers={"X-Fc-Invocation-Type": "Async"},
                )
                if resp.status_code not in (200, 202):
                    self._pending_results.pop(result_token, None)
                    error = f"FC 触发失败: HTTP {resp.status_code}"
                    await _log(error)
                    await _notify_feishu(f"[模型{label}] {error}")
                    return {"error": error}
        except Exception as e:
            self._pending_results.pop(result_token, None)
            error = f"FC 请求失败: {e}"
            await _log(error)
            await _notify_feishu(f"[模型{label}] {error}")
            return {"error": error}

        await _log("FC 已接受训练任务，等待训练完成...")

        # Wait for FC to call back with result
        try:
            await asyncio.wait_for(event.wait(), timeout=3600)
        except asyncio.TimeoutError:
            error = "FC 训练超时 (>3600s 未收到回调)"
            await _log(error)
            await _notify_feishu(f"[模型{label}] {error}")
            return {"error": error}
        finally:
            result = self._pending_results.pop(result_token, {}).get("result")

        if not result:
            error = "FC 回调结果为空"
            await _log(error)
            return {"error": error}

        if not result.get("success"):
            error = f"FC 训练失败: {result.get('error', '未知错误')}"
            await _log(error)
            await _notify_feishu(f"[模型{label}] {error}")
            return {"error": error}

        # Step 5: Download model from S3 (source of truth)
        # FC uploads model to S3 after training. We download from there.
        # model_b64 in callback is only a fallback if S3 download fails.
        fallback_name = f"{mode}_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}"
        model_name = result.get("model_name", fallback_name)
        rounds = result.get("rounds", 0)
        s3_uri = result.get("s3_uri")

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model_path = MODEL_DIR / f"{model_name}.lgb"
        full_model_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"

        downloaded_from_s3 = False
        s3_key = None
        if s3_uri:
            # Parse s3_key from s3_uri (format: s3://bucket/key)
            s3_key = "/".join(s3_uri.split("/")[3:]) if s3_uri.startswith("s3://") else None
            if s3_key:
                try:
                    from src.common.s3_client import create_s3_client_from_config

                    s3 = create_s3_client_from_config()
                    if s3:
                        await s3.download_file(s3_key, model_path)
                        if mode == "full":
                            await s3.download_file(s3_key, full_model_path)
                        downloaded_from_s3 = True
                        await _log(f"模型已从 S3 下载: {model_path.name}")
                except Exception as e:
                    await _log(f"S3 下载失败，尝试 base64 fallback: {e}")

        if not downloaded_from_s3:
            # Fallback: use base64 from callback (legacy FC versions)
            import base64

            model_b64 = result.get("model_b64", "")
            if not model_b64:
                error = "FC 返回无模型数据且 S3 下载失败"
                await _log(error)
                return {"error": error}
            model_bytes = base64.b64decode(model_b64)
            model_path.write_bytes(model_bytes)
            size = len(model_bytes)
            await _log(f"模型已保存 (base64 fallback): {model_path.name} ({size}B)")
            if mode == "full":
                full_model_path.write_bytes(model_bytes)

        # Write model metadata so dashboard shows training date
        now_str = datetime.now(BEIJING_TZ).isoformat()
        _write_model_meta(
            s3_key=s3_key or f"models/{model_name}.lgb",
            s3_last_modified=now_str,
        )

        # Refresh s3_has_full_model from S3 (S3 is single source of truth)
        await self._refresh_s3_flag()

        elapsed = result.get("elapsed_seconds", 0)
        n_samples = result.get("n_samples", 0)
        n_days = result.get("n_days", 0)

        await _notify_feishu(
            f"[模型训练] {label}完成 (远程FC)\n"
            f"模型: {model_name}.lgb\n迭代轮数: {rounds}\n"
            f"样本数: {n_samples}, 天数: {n_days}\n"
            f"耗时: {elapsed}s"
        )

        # Step 6: Update last finetune date and last success time
        _set_last_finetune_date(datetime.now(BEIJING_TZ).date())
        _set_last_success_time(mode, model_name)

        await _log(f"{label}全部完成 ({rounds}轮, {elapsed}s)")
        return {
            "model_name": model_name,
            "model_path": str(model_path),
            "s3_uri": s3_uri,
            "rounds": rounds,
        }

    async def _check_data_completeness(self, progress_cb=None) -> bool:
        """Check GreptimeDB data integrity, try to fill gaps up to 3 times.

        Returns True if data is good enough to train.
        """
        storage = self._get_storage()
        if storage is None or not storage.is_ready:
            if progress_cb:
                await progress_cb("GreptimeDB storage 不可用")
            return False

        for attempt in range(1, MAX_DATA_FIX_ATTEMPTS + 1):
            issues = await storage.check_data_integrity()
            error_issues = [i for i in issues if i["level"] == "error"]

            if not error_issues:
                if progress_cb:
                    await progress_cb(f"数据完整性检查通过 (第{attempt}次)")
                return True

            if progress_cb:
                n = len(error_issues)
                await progress_cb(
                    f"数据完整性问题: {n} 个 (第{attempt}/{MAX_DATA_FIX_ATTEMPTS}次尝试修复)"
                )

            # Try to fill gaps
            try:
                from src.data.services.cache_scheduler import CacheScheduler

                temp_scheduler = CacheScheduler(self._app_state)
                await temp_scheduler.check_and_fill_gaps()
            except Exception as e:
                if progress_cb:
                    await progress_cb(f"数据修复失败: {e}")

        return False
