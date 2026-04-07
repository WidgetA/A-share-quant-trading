# === MODULE PURPOSE ===
# Background scheduler for ML model training and fine-tuning.
# Runs every 20 trading days at 3am Beijing time: checks data completeness,
# builds training data from GreptimeDB, trains/fine-tunes LightGBM model,
# saves to disk, uploads to S3, and sends Feishu notifications.

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

    def _generate_training_token(self, mode: str) -> str:
        """Generate a one-time token for FC callback authentication."""
        token = secrets.token_urlsafe(32)
        now = datetime.now(BEIJING_TZ)
        self._training_tokens[token] = {"mode": mode, "created_at": now, "used": False}
        # Clean up tokens older than 1 hour
        cutoff = now - timedelta(hours=1)
        self._training_tokens = {
            k: v for k, v in self._training_tokens.items() if v["created_at"] > cutoff
        }
        return token

    def validate_and_consume_token(self, token: str) -> str | None:
        """Validate a training token. Returns mode if valid, None otherwise."""
        info = self._training_tokens.get(token)
        if not info or info["used"]:
            return None
        if datetime.now(BEIJING_TZ) - info["created_at"] > timedelta(hours=1):
            del self._training_tokens[token]
            return None
        info["used"] = True
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
        return {
            "next_run_time": self.next_run_time,
            "last_run_time": self.last_run_time,
            "last_run_result": self.last_run_result,
            "last_run_message": self.last_run_message,
            "training_in_progress": self.training_in_progress,
            "has_full_model": full_model_path.exists(),
            "current_model": self._get_current_model_name(),
            "log_lines": self.training_log[-50:],
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

    def _get_cache(self):
        return getattr(self._app_state, "backtest_cache", None)

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
        """Core training logic shared by full training and fine-tuning.

        If FC endpoint is configured, trains remotely via serverless.
        Otherwise falls back to local training.
        """
        from src.common.config import get_fc_url

        fc_url = get_fc_url()
        if fc_url:
            return await self._train_remote(mode, init_model_path, progress_cb)
        return await self._train_local(mode, init_model_path, progress_cb)

    async def _train_remote(
        self,
        mode: str,
        init_model_path: Path | None = None,
        progress_cb=None,
    ) -> dict:
        """Train via FC serverless endpoint."""
        import base64

        import httpx

        from src.common.config import get_fc_url, get_s3_config

        label = "全量训练" if mode == "full" else "微调"

        async def _log(msg: str):
            self._append_log(msg)
            if progress_cb:
                await progress_cb(msg)

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

        # Step 3: Build lightweight payload (no daily_data!)
        payload: dict = {
            "mode": mode,
            "callback_url": callback_url,
            "result_callback_url": result_callback_url,
        }

        if init_model_path and init_model_path.exists():
            model_bytes = init_model_path.read_bytes()
            payload["init_model_b64"] = base64.b64encode(model_bytes).decode("ascii")
            await _log(f"附加基准模型: {init_model_path.name} ({len(model_bytes)} bytes)")

        s3_config = get_s3_config()
        if s3_config:
            payload["s3_config"] = s3_config

        # Step 4: POST async trigger to FC
        fc_url = get_fc_url()
        assert fc_url, "FC URL not configured"  # guaranteed by _train() caller
        await _log("发送异步训练请求到 FC...")
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
                resp = await client.post(
                    fc_url,
                    json=payload,
                    headers={"X-Fc-Async": "true"},
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
            await asyncio.wait_for(event.wait(), timeout=900)
        except asyncio.TimeoutError:
            error = "FC 训练超时 (>900s 未收到回调)"
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

        # Step 5: Save returned model locally
        fallback_name = f"{mode}_{datetime.now(BEIJING_TZ).strftime('%Y%m%d')}"
        model_name = result.get("model_name", fallback_name)
        rounds = result.get("rounds", 0)
        model_b64 = result.get("model_b64", "")

        if not model_b64:
            error = "FC 返回无模型数据"
            await _log(error)
            return {"error": error}

        model_bytes = base64.b64decode(model_b64)
        MODEL_DIR.mkdir(parents=True, exist_ok=True)

        model_path = MODEL_DIR / f"{model_name}.lgb"
        model_path.write_bytes(model_bytes)
        await _log(f"模型已保存: {model_path.name} ({len(model_bytes)} bytes)")

        if mode == "full":
            latest_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
            latest_path.write_bytes(model_bytes)
            await _log(f"基准模型已保存: {latest_path.name}")

        s3_uri = result.get("s3_uri")
        elapsed = result.get("elapsed_seconds", 0)
        n_samples = result.get("n_samples", 0)
        n_days = result.get("n_days", 0)

        await _notify_feishu(
            f"[模型训练] {label}完成 (远程FC)\n"
            f"模型: {model_name}.lgb\n迭代轮数: {rounds}\n"
            f"样本数: {n_samples}, 天数: {n_days}\n"
            f"耗时: {elapsed}s"
        )

        # Step 6: Update last finetune date
        _set_last_finetune_date(datetime.now(BEIJING_TZ).date())

        await _log(f"{label}全部完成 ({rounds}轮, {elapsed}s)")
        return {
            "model_name": model_name,
            "model_path": str(model_path),
            "s3_uri": s3_uri,
            "rounds": rounds,
        }

    async def _train_local(
        self,
        mode: str,
        init_model_path: Path | None = None,
        progress_cb=None,
    ) -> dict:
        """Train locally (fallback when FC URL not configured)."""
        label = "全量训练" if mode == "full" else "微调"

        async def _log(msg: str):
            self._append_log(msg)
            if progress_cb:
                await progress_cb(msg)

        await _log(f"{label}开始")
        await _notify_feishu(f"[模型训练] {label}开始")

        # Step 1: Data completeness check with retries
        await _log("检查数据完整性...")
        data_ok = await self._check_data_completeness(progress_cb=_log)
        if not data_ok:
            error = "数据不完整，已尝试补全3次仍失败"
            await _log(f"中止: {error}")
            await _notify_feishu(f"[模型{label}] {error}，跳过本次{label}")
            return {"error": error}

        # Step 2: Build training data from GreptimeDB
        await _log("构建训练数据...")
        try:
            train_result = await self._build_training_data(mode=mode, progress_cb=_log)
        except Exception as e:
            error = f"构建训练数据失败: {e}"
            await _log(error)
            await _notify_feishu(f"[模型{label}] {error}")
            return {"error": error}

        features = train_result["features"]
        labels = train_result["labels"]
        groups = train_result["groups"]
        val_features = train_result.get("val_features")
        val_labels = train_result.get("val_labels")
        val_groups = train_result.get("val_groups")

        await _log(
            f"训练数据就绪: {len(labels)} 条样本, {len(groups)} 天, "
            f"验证集 {len(val_labels or [])} 条"
        )

        # Step 3: Train model
        await _log("开始训练 LightGBM LambdaRank...")
        try:
            from src.strategy.strategies.ml_scanner import MLScanner

            # Run CPU-bound training in thread pool
            booster = await asyncio.to_thread(
                MLScanner.train_model,
                features,
                labels,
                groups,
                val_features,
                val_labels,
                val_groups,
                str(init_model_path) if init_model_path else None,
            )
        except Exception as e:
            error = f"训练失败: {e}"
            await _log(error)
            await _notify_feishu(f"[模型{label}] {error}")
            return {"error": error}

        rounds = booster.current_iteration()
        await _log(f"训练完成: {rounds} 轮迭代")

        # Step 4: Save model locally
        today_str = datetime.now(BEIJING_TZ).strftime("%Y%m%d")
        if mode == "full":
            model_name = f"full_{today_str}"
            # Also save as full_latest for finetune base
            latest_path = MODEL_DIR / f"{FULL_MODEL_NAME}.lgb"
            MODEL_DIR.mkdir(parents=True, exist_ok=True)
            booster.save_model(str(latest_path))
            await _log(f"基准模型已保存: {latest_path.name}")
        else:
            model_name = f"finetune_{today_str}"

        from src.strategy.strategies.ml_scanner import MLScanner

        model_path = MLScanner.save_model(booster, model_name)
        await _log(f"模型已保存: {model_path.name}")
        await _notify_feishu(f"[模型训练] {label}完成\n模型: {model_name}.lgb\n迭代轮数: {rounds}")

        # Step 5: Upload to S3
        s3_uri = await self._upload_to_s3(model_path, f"models/{model_name}.lgb", _log)
        if mode == "full":
            await self._upload_to_s3(latest_path, f"models/{FULL_MODEL_NAME}.lgb", _log)

        # Step 6: Update last finetune date
        _set_last_finetune_date(datetime.now(BEIJING_TZ).date())

        await _log(f"{label}全部完成")
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
        cache = self._get_cache()
        if cache is None or not cache.is_ready:
            if progress_cb:
                await progress_cb("backtest cache 不可用")
            return False

        for attempt in range(1, MAX_DATA_FIX_ATTEMPTS + 1):
            issues = await cache.check_data_integrity()
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

    async def _build_training_data(self, mode: str, progress_cb=None) -> dict:
        """Build feature matrix + labels from GreptimeDB cache.

        Returns dict with: features, labels, groups, val_features, val_labels, val_groups.
        """
        from src.strategy.strategies.ml_scanner import (
            _MIN_TRAIN_DAYS,
            _TRAIN_VAL_SPLIT,
        )

        cache = self._get_cache()
        if cache is None or not cache.is_ready:
            raise RuntimeError("backtest cache not available")

        # Get available date range
        existing_dates = sorted(await cache._get_existing_daily_dates())
        if len(existing_dates) < _MIN_TRAIN_DAYS:
            raise RuntimeError(
                f"训练数据不足: {len(existing_dates)} 天 < 最少 {_MIN_TRAIN_DAYS} 天"
            )

        if progress_cb:
            n = len(existing_dates)
            await progress_cb(f"可用日期: {n} 天 ({existing_dates[0]}~{existing_dates[-1]})")

        # For finetune, use recent data; for full, use all
        if mode == "finetune":
            # Use the most recent 60-120 trading days
            train_dates = existing_dates[-120:]
        else:
            train_dates = existing_dates

        # Split into train/val by time
        split_idx = int(len(train_dates) * _TRAIN_VAL_SPLIT)
        train_set = train_dates[:split_idx]
        val_set = train_dates[split_idx:]

        if progress_cb:
            await progress_cb(f"训练集: {len(train_set)} 天, 验证集: {len(val_set)} 天")

        # Build feature vectors and labels for each day
        # This is a placeholder that collects data from GreptimeDB.
        # The actual implementation requires querying daily OHLCV data,
        # computing features, and calculating forward returns.
        # For now, we prepare the data structure that MLScanner.train_model expects.

        all_features: list[list[float]] = []
        all_labels: list[int] = []
        all_groups: list[int] = []
        val_features: list[list[float]] = []
        val_labels_list: list[int] = []
        val_groups: list[int] = []

        # Process each training date (with retry on DB timeout)
        skipped = 0
        for i, trade_date in enumerate(train_dates):
            if progress_cb and i % 20 == 0:
                await progress_cb(f"处理训练数据: {i}/{len(train_dates)} 天")

            day_data = await self._fetch_day_with_retry(cache, trade_date, existing_dates)
            if day_data is None:
                skipped += 1
                continue

            all_features.extend(day_data["features"])
            all_labels.extend(day_data["labels"])
            all_groups.append(day_data["group_size"])

        if skipped > 0:
            logger.warning("训练数据: 跳过 %d/%d 天 (无数据或查询超时)", skipped, len(train_dates))

        skipped = 0
        for i, trade_date in enumerate(val_set):
            if progress_cb and i % 10 == 0:
                await progress_cb(f"处理验证数据: {i}/{len(val_set)} 天")

            day_data = await self._fetch_day_with_retry(cache, trade_date, existing_dates)
            if day_data is None:
                skipped += 1
                continue

            val_features.extend(day_data["features"])
            val_labels_list.extend(day_data["labels"])
            val_groups.append(day_data["group_size"])

        if skipped > 0:
            logger.warning("验证数据: 跳过 %d/%d 天 (无数据或查询超时)", skipped, len(val_set))

        if not all_features:
            raise RuntimeError("无法构建训练数据: 所有日期的特征提取失败")

        if progress_cb:
            await progress_cb(
                f"数据构建完成: 训练 {len(all_features)} 条/{len(all_groups)} 天, "
                f"验证 {len(val_features)} 条/{len(val_groups)} 天"
            )

        return {
            "features": all_features,
            "labels": all_labels,
            "groups": all_groups,
            "val_features": val_features if val_features else None,
            "val_labels": val_labels_list if val_labels_list else None,
            "val_groups": val_groups if val_groups else None,
        }

    async def _fetch_day_with_retry(
        self, cache, trade_date: date, all_dates: list[date], retries: int = 2
    ) -> dict | None:
        """Fetch day data with retry on timeout. Returns None after all retries fail."""
        for attempt in range(1 + retries):
            try:
                return await self._get_day_features_and_labels(cache, trade_date, all_dates)
            except (asyncio.TimeoutError, TimeoutError):
                if attempt < retries:
                    logger.warning("查询超时 %s (第%d次重试)...", trade_date, attempt + 1)
                    await asyncio.sleep(1)
                else:
                    logger.error("查询超时 %s, 已重试%d次, 跳过该天", trade_date, retries)
                    return None
            except Exception:
                logger.error("获取 %s 数据异常, 跳过", trade_date, exc_info=True)
                return None
        return None  # unreachable, but makes type checker happy

    async def _get_day_features_and_labels(
        self, cache, trade_date: date, all_dates: list[date]
    ) -> dict | None:
        """Get features and labels for a single trading day.

        Returns dict with: features (list of 76-dim vectors), labels (list of ints),
        group_size (int). Returns None if insufficient data.
        """
        from src.strategy.strategies.ml_scanner import (
            _FORWARD_DAYS,
            _LABEL_BINS,
            _TOTAL_FEE_PCT,
        )

        date_str = trade_date.strftime("%Y-%m-%d")

        # Get daily OHLCV data: dict[stock_code, DailyBar]
        daily_map = await cache.get_all_codes_with_daily(date_str)
        if not daily_map or len(daily_map) < 50:
            return None

        # Need forward return: find the close price _FORWARD_DAYS later
        date_idx = None
        for i, d in enumerate(all_dates):
            if d == trade_date:
                date_idx = i
                break
        if date_idx is None or date_idx + _FORWARD_DAYS >= len(all_dates):
            return None  # Can't compute forward return

        future_date = all_dates[date_idx + _FORWARD_DAYS]
        future_str = future_date.strftime("%Y-%m-%d")
        future_map = await cache.get_all_codes_with_daily(future_str)
        if not future_map:
            return None

        # Build {code: close} maps, skip suspended stocks
        today_close: dict[str, float] = {}
        for code, bar in daily_map.items():
            if bar.is_suspended:
                continue
            if bar.close > 0:
                today_close[code] = bar.close

        future_close: dict[str, float] = {}
        for code, bar in future_map.items():
            if bar.close > 0:
                future_close[code] = bar.close

        # Compute forward returns and labels
        codes_with_return = []
        returns = []
        for code in today_close:
            if code in future_close:
                ret = (future_close[code] / today_close[code] - 1) * 100 - _TOTAL_FEE_PCT
                codes_with_return.append(code)
                returns.append(ret)

        if len(codes_with_return) < 20:
            return None

        # Bucket into quintiles
        sorted_returns = sorted(returns)
        bin_size = len(sorted_returns) / _LABEL_BINS
        labels = []
        for ret in returns:
            for b in range(_LABEL_BINS):
                threshold_idx = min(int((b + 1) * bin_size), len(sorted_returns) - 1)
                if ret <= sorted_returns[threshold_idx]:
                    labels.append(b)
                    break
            else:
                labels.append(_LABEL_BINS - 1)

        # Build feature vectors from DailyBar objects
        features = []
        valid_labels = []
        for i, code in enumerate(codes_with_return):
            bar = daily_map.get(code)
            if not bar:
                continue
            fv = self._extract_features_from_bar(bar)
            if fv is not None:
                features.append(fv)
                valid_labels.append(labels[i])

        if not features:
            return None

        return {
            "features": features,
            "labels": valid_labels,
            "group_size": len(features),
        }

    def _extract_features_from_bar(self, bar) -> list[float] | None:
        """Extract 76 features from a DailyBar NamedTuple.

        Returns 76-dimensional feature vector or None if data insufficient.
        """
        from src.strategy.strategies.ml_scanner import FEATURE_NAMES_RAW

        o = bar.open
        h = bar.high
        lo = bar.low
        c = bar.close
        vol = bar.volume

        if not all([o > 0, h > 0, lo > 0, c > 0]):
            return None

        # Compute basic features from OHLCV
        raw = {}
        raw["open_gain"] = (o / c - 1) * 100
        raw["volume_amp"] = vol / 1e6 if vol else 0
        raw["consecutive_up_days"] = 0  # Requires history
        raw["trend_5d"] = 0
        raw["trend_10d"] = 0
        raw["avg_return_20d"] = 0
        raw["volatility_20d"] = 0
        raw["early_price_range"] = (h - lo) / o * 100
        raw["market_open_gain"] = 0
        raw["trend_consistency"] = 0
        raw["gap"] = 0
        raw["upper_shadow_ratio"] = (h - max(o, c)) / (h - lo) if h > lo else 0
        raw["volume_ratio"] = 1.0

        # Advanced features (defaults — require more history)
        raw["open_position_consistency"] = 0
        raw["volume_price_divergence"] = 0
        raw["intraday_momentum_cont"] = (c - o) / (h - lo) if h > lo else 0
        raw["volume_concentration"] = 0
        raw["relative_strength"] = 0
        raw["return_consistency"] = 0
        raw["amplitude_decay"] = 0
        raw["volume_stability"] = 0
        raw["close_vs_vwap"] = 0
        raw["volume_weighted_return"] = 0
        raw["price_channel_position"] = 0
        raw["up_day_ratio_20d"] = 0
        raw["amplitude_20d"] = (h - lo) / o * 100
        raw["volume_ratio_5d_20d"] = 1.0

        # Cross features
        raw["momentum_x_mean_reversion"] = raw["open_gain"] * raw["avg_return_20d"]
        raw["trend_acceleration"] = raw["trend_5d"] - raw["trend_10d"]
        raw["momentum_quality"] = raw["open_gain"] * raw["volume_amp"]
        raw["volume_trend_interaction"] = raw["volume_ratio"] * raw["trend_5d"]
        raw["gap_volume_interaction"] = raw["gap"] * raw["volume_amp"]
        raw["strength_persistence"] = raw["relative_strength"] * raw["consecutive_up_days"]
        raw["volatility_adj_return"] = (
            raw["open_gain"] / raw["volatility_20d"] if raw["volatility_20d"] > 0 else 0
        )
        raw["volume_price_momentum"] = raw["volume_amp"] * raw["intraday_momentum_cont"]
        raw["gap_reversion"] = raw["gap"] * raw["open_gain"]
        raw["trend_volume_divergence"] = raw["trend_5d"] * (1 - raw["volume_ratio"])
        raw["momentum_stability"] = raw["return_consistency"] * raw["trend_5d"]

        # Build 38 raw + 38 z-scored (z-scores computed as 0 for single stock)
        feature_vec = [raw.get(name, 0.0) for name in FEATURE_NAMES_RAW]
        # Z-score placeholder (will be 0 when computed across full candidate pool)
        z_scored = [0.0] * len(FEATURE_NAMES_RAW)
        return feature_vec + z_scored

    async def _upload_to_s3(self, local_path: Path, s3_key: str, log_fn) -> str | None:
        """Upload model file to S3. Returns S3 URI or None."""
        try:
            from src.common.s3_client import create_s3_client_from_config

            client = create_s3_client_from_config()
            if client is None:
                await log_fn("S3 未配置，跳过上传")
                return None

            uri = await client.upload_file(local_path, s3_key)
            await log_fn(f"已上传到 S3: {s3_key}")
            await _notify_feishu(f"[模型训练] 模型已上传S3\n{s3_key}")
            return uri
        except Exception as e:
            await log_fn(f"S3 上传失败: {e}")
            await _notify_feishu(f"[模型训练] S3 上传失败\n{e}")
            return None
