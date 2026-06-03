# === MODULE PURPOSE ===
# Audit-related endpoints under /api/audit/*:
#   - Truth-table (trading_calendar) rebuild / status / problems
#   - Index-driven daily backfill
#   - Path-B listing-info verification (kimi) trigger + observability
#     (status / kimi-raw / findings)
#   - stock_snapshot / listing-info coverage summaries
#
# kimi AUTHENTICATION is NOT handled here anymore: it uses a static Kimi-Code
# API key generated into ~/.kimi/config.toml at startup from the KIMI_API_KEY
# env var (see src/data/services/kimi_config.py). The old OAuth
# credentials-upload + device-id endpoints were removed once the static key
# replaced them (no more 15-min token expiry / device-bound refresh).

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from src.web.routes import verify_trading_api_key

logger = logging.getLogger(__name__)


def create_audit_router() -> APIRouter:
    """Routes under /api/audit/* for the truth-table pipeline + path-B verify."""
    router = APIRouter(prefix="/api/audit", tags=["audit"])

    # ------------------------------------------------------------------
    # Listing info upload (kimi verification result)
    # ------------------------------------------------------------------

    @router.get("/listing-info/status")
    async def listing_info_status(request: Request) -> JSONResponse:
        """Coverage stats + path-B scheduler status for the settings card."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            return JSONResponse({"ready": False})
        try:
            info = await storage.get_listing_info_all()
        except Exception as e:
            logger.warning("listing_info_status 读取失败: %s", e, exc_info=True)
            return JSONResponse({"ready": False, "error": str(e)})

        verified = sum(1 for v in info.values() if v.get("verified"))
        payload: dict[str, Any] = {
            "ready": True,
            "total_codes": len(info),
            "verified_codes": verified,
            "unverified_codes": len(info) - verified,
        }
        scheduler = getattr(request.app.state, "listing_verify_scheduler", None)
        if scheduler is not None:
            payload["scheduler"] = scheduler.get_status()
        return JSONResponse(payload)

    @router.post(
        "/listing-info/verify",
        dependencies=[Depends(verify_trading_api_key)],
    )
    async def trigger_listing_verify(request: Request) -> JSONResponse:
        """Trigger an immediate path-B verify run in the background.

        Does NOT receive offline verification results — verification is owned
        by the server-side kimi-cli; this only kicks the scheduler. Pass
        ``?include_failed=1`` to also re-verify existing verified=false rows.
        """
        scheduler = getattr(request.app.state, "listing_verify_scheduler", None)
        if scheduler is None:
            raise HTTPException(status_code=503, detail="验证调度器未就绪")
        if scheduler.in_progress:
            return JSONResponse({"success": False, "message": "验证正在进行中，请稍后"})
        include_failed = request.query_params.get("include_failed") in ("1", "true", "yes")
        asyncio.create_task(scheduler.trigger_manual(include_failed=include_failed))
        return JSONResponse(
            {"success": True, "message": "已触发后台验证", "include_failed": include_failed}
        )

    @router.post("/listing-info/verify-problems")
    async def trigger_verify_problems(request: Request) -> JSONResponse:
        """Run kimi on the truth-table's listing-date-ambiguous codes — the backstop
        for cases Tushare stock_basic can't resolve (DAT-006).

        These are codes the calendar marks ``source_none`` (在册却源头无数据 → list_date
        suspect, e.g. 920 迁移代码) or ``orphan`` (有数据却不在册 → 退市/迁移代码). kimi
        web-searches the real listing/delisting and writes it back to stock_listing_info;
        the next calendar rebuild then reclassifies them.

        ``?states=source_none,orphan`` (default), ``?max=`` per-run cap.
        """
        scheduler = getattr(request.app.state, "listing_verify_scheduler", None)
        storage = getattr(request.app.state, "storage", None)
        if scheduler is None or storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="验证调度器/存储未就绪")
        if scheduler.in_progress:
            return JSONResponse({"success": False, "message": "验证正在进行中，请稍后"})
        states_raw = request.query_params.get("states", "source_none,orphan")
        states = {s.strip() for s in states_raw.split(",") if s.strip()}
        max_codes: int | None
        try:
            max_codes = int(request.query_params["max"]) if "max" in request.query_params else None
        except ValueError:
            max_codes = None
        try:
            codes = await storage.get_calendar_problem_codes(states)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not codes:
            return JSONResponse(
                {"success": True, "message": f"真值表里没有 {states_raw} 代码,无需验证"}
            )
        asyncio.create_task(scheduler.trigger_manual(codes=codes, max_codes=max_codes))
        return JSONResponse(
            {
                "success": True,
                "message": f"已触发 kimi 兜底验证 {len(codes)} 只({states_raw}),结果发飞书",
            }
        )

    @router.get("/listing-info/kimi-raw")
    async def kimi_raw(request: Request) -> JSONResponse:
        """Observability: return the FULL raw kimi output (the tool trace —
        SearchWeb/FetchURL/Shell calls + results) from a code's last verify run,
        so a "查不到" can be INSPECTED instead of guessed at. ?code=XXXXXX."""
        from src.data.services.listing_verify_scheduler import KIMI_RAW_DIR

        code = request.query_params.get("code", "").strip()
        if not (len(code) == 6 and code.isdigit()):
            raise HTTPException(status_code=400, detail="code 必须是 6 位数字")
        path = KIMI_RAW_DIR / f"{code}.txt"
        if not path.exists():
            return JSONResponse(
                {"code": code, "found": False, "message": "暂无 kimi 原始记录(尚未验证过该代码)"}
            )
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"读取失败: {e}")
        return JSONResponse({"code": code, "found": True, "chars": len(text), "raw": text})

    @router.get("/listing-info/findings")
    async def kimi_findings(request: Request) -> JSONResponse:
        """kimi 逐只查到的"怎么回事"(名字 / 状态 / 一句话说明 / 上市退市日)。

        这是放 kimi 上去的目的——它自己查清每个代码现在到底是什么情况,写成
        人能看懂的清单。?code=XXXXXX 看单只;不带 code 返回全部(按状态归类)。
        """
        import json as _json

        from src.data.services.listing_verify_scheduler import KIMI_FINDINGS_DIR

        code = request.query_params.get("code", "").strip()
        if code:
            if not (len(code) == 6 and code.isdigit()):
                raise HTTPException(status_code=400, detail="code 必须是 6 位数字")
            path = KIMI_FINDINGS_DIR / f"{code}.json"
            if not path.exists():
                return JSONResponse(
                    {"code": code, "found": False, "message": "kimi 尚未查过这个代码"}
                )
            return JSONResponse(
                {"code": code, "found": True, "finding": _json.loads(path.read_text("utf-8"))}
            )

        if not KIMI_FINDINGS_DIR.exists():
            return JSONResponse({"total": 0, "by_status": {}, "findings": []})
        items: list[dict] = []
        for p in sorted(KIMI_FINDINGS_DIR.glob("*.json")):
            try:
                items.append(_json.loads(p.read_text("utf-8")))
            except Exception:
                continue
        by_status: dict[str, int] = {}
        for it in items:
            by_status[it.get("status", "?")] = by_status.get(it.get("status", "?"), 0) + 1
        return JSONResponse({"total": len(items), "by_status": by_status, "findings": items})

    # ------------------------------------------------------------------
    # TEMPORARY: 新旧索引对照验证 (POST /api/audit/index-compare)
    # 触发后台任务,把新索引(三合一-kimi)逐日跟旧索引(Tushare bak_basic)对照,
    # 结果发飞书。用来在「索引驱动修复」上线前确认新索引可信。确认完即可删此段
    # + scripts/compare_index_old_new.py。
    # ------------------------------------------------------------------

    @router.post("/index-compare")
    async def trigger_index_compare(request: Request) -> JSONResponse:
        """Kick a background old-vs-new index comparison; result → Feishu.

        TEMPORARY tool — intentionally un-gated (read-only compare + Feishu),
        no X-API-Key needed, so it can be triggered freely while we validate
        the new index. Delete with the rest of the index-compare code.

        Query params: start / end (YYYY-MM-DD, optional), max_days (default 30).
        """
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "index_compare_running", False):
            return JSONResponse({"success": False, "message": "对照任务正在进行中，请稍后"})

        qp = request.query_params
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = qp.get("start") or (today - timedelta(days=45)).isoformat()
        end = qp.get("end") or today.isoformat()
        try:
            max_days = int(qp.get("max_days", "30"))
        except ValueError:
            max_days = 30

        async def _run() -> None:
            from scripts.compare_index_old_new import (
                _notify_feishu,
                compare_index_range,
                format_feishu_summary,
            )
            from src.common.config import get_tushare_token
            from src.data.clients.tushare_realtime import (
                TushareRealtimeClient,
                get_tushare_trade_calendar,
            )

            request.app.state.index_compare_running = True
            client = TushareRealtimeClient(token=get_tushare_token())
            try:
                cal = await get_tushare_trade_calendar(start, end)
                dates = sorted(datetime.strptime(s, "%Y-%m-%d").date() for s in cal)
                if len(dates) > max_days:
                    logger.warning(
                        "index-compare: %d 交易日超过 max_days=%d, 只对照最近 %d 天",
                        len(dates),
                        max_days,
                        max_days,
                    )
                    dates = dates[-max_days:]
                if not dates:
                    await _notify_feishu("[新旧索引对照] 无可对照的交易日")
                    return
                await client.start()
                result = await compare_index_range(storage, client, dates)
                await _notify_feishu(format_feishu_summary(result))
            except Exception as e:
                logger.error("index-compare 失败: %s", e, exc_info=True)
                await _notify_feishu(f"[新旧索引对照] 执行异常\n{e}")
            finally:
                await client.stop()
                request.app.state.index_compare_running = False

        asyncio.create_task(_run())
        return JSONResponse(
            {"success": True, "message": "已触发后台对照", "start": start, "end": end}
        )

    # ------------------------------------------------------------------
    # stock_snapshot 历史回填 (POST /api/audit/snapshot-backfill)
    # 把索引基表 stock_snapshot 回填到历史区间(默认 2023-01-01~昨天)。
    # 只跑 snapshot(B∪D∪S 三源并集),不碰分钟线;resume-safe(跳已存在日期);
    # 撞 Tushare 限频自动 sleep 后靠 resume 续跑。后台执行,起止发飞书。
    # ------------------------------------------------------------------

    @router.post("/snapshot-backfill")
    async def trigger_snapshot_backfill(request: Request) -> JSONResponse:
        """Backfill stock_snapshot over a historical range (snapshot-only)."""
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo

        storage = getattr(request.app.state, "storage", None)
        pipeline = getattr(request.app.state, "pipeline", None)
        if storage is None or pipeline is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="storage/pipeline 未就绪")
        if getattr(request.app.state, "snapshot_backfill_running", False):
            return JSONResponse({"success": False, "message": "snapshot 回填正在进行中，请稍后"})
        if getattr(request.app.state, "cache_fill_running", False):
            return JSONResponse(
                {"success": False, "message": "缓存补全正在运行，待其结束后再试(避免抢内存)"}
            )

        qp = request.query_params
        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = qp.get("start") or "2023-01-01"
        end = qp.get("end") or (today - timedelta(days=1)).isoformat()

        async def _run() -> None:
            from scripts.compare_index_old_new import _notify_feishu
            from src.data.clients.tushare_realtime import get_tushare_trade_calendar

            request.app.state.snapshot_backfill_running = True
            try:
                cal = await get_tushare_trade_calendar(start, end)
                dates = sorted(datetime.strptime(s, "%Y-%m-%d").date() for s in cal)
                if not dates:
                    await _notify_feishu("[snapshot 回填] 无交易日,跳过")
                    return

                async def _missing() -> int:
                    existing = await storage.get_existing_snapshot_dates()
                    return sum(1 for d in dates if d not in existing)

                miss0 = await _missing()
                await _notify_feishu(
                    f"[snapshot 回填] 开始 {start}~{end}: 目标 {len(dates)} 天, "
                    f"待回填 {miss0} 天(只跑 snapshot,resume-safe)"
                )

                # daily_source + metadata_source must be entered (httpx clients);
                # minute_source is NOT needed for snapshot.
                stale = 0
                async with pipeline.daily_source, pipeline.metadata_source:
                    while True:
                        prev = await _missing()
                        if prev == 0:
                            break
                        try:
                            # resume-safe: re-running skips already-synced dates,
                            # so a rate-limit mid-run just continues next round.
                            await pipeline._sync_stock_snapshot(dates, None)
                        except Exception as e:  # noqa: BLE001 — backfill, keep going
                            logger.warning("snapshot-backfill 中断(resume 续跑): %s", e)
                            await asyncio.sleep(20)
                        now_missing = await _missing()
                        if now_missing >= prev:  # no progress this round
                            stale += 1
                            if stale >= 5:
                                logger.error(
                                    "snapshot-backfill 连续 5 轮无进展,停止 (还缺 %d 天)",
                                    now_missing,
                                )
                                break
                            await asyncio.sleep(30)
                        else:
                            stale = 0

                done = len(dates) - await _missing()
                await _notify_feishu(
                    f"[snapshot 回填] 完成 {start}~{end}: 目标 {len(dates)} 天, "
                    f"已入库 {done} 天, 仍缺 {len(dates) - done} 天"
                )
            except Exception as e:
                logger.error("snapshot-backfill 失败: %s", e, exc_info=True)
                await _notify_feishu(f"[snapshot 回填] 异常\n{e}")
            finally:
                request.app.state.snapshot_backfill_running = False

        asyncio.create_task(_run())
        return JSONResponse(
            {"success": True, "message": "已触发 snapshot 回填", "start": start, "end": end}
        )

    # ------------------------------------------------------------------
    # 数据缺口诊断报告 (POST /api/audit/diagnose-gaps)
    # 把每日补全报的三类问题逐条核查,产出"问题→根因→该多少→怎么修"报告,发飞书。
    # ------------------------------------------------------------------

    @router.post("/diagnose-gaps")
    async def trigger_diagnose_gaps(request: Request) -> JSONResponse:
        """Run the gap-diagnosis report in the background; result → Feishu."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "diagnose_gaps_running", False):
            return JSONResponse({"success": False, "message": "诊断正在进行中，请稍后"})

        # Bounded by default so a manual trigger can't blow Tushare's 500/min
        # limit (suspend_d per daily-gap day + stk_mins per missing minute stock).
        # Override via query params for a deliberate bigger run.
        def _qint(name: str, default: int) -> int:
            try:
                return int(request.query_params.get(name, str(default)))
            except ValueError:
                return default

        daily_detail_days = _qint("daily_detail_days", 30)
        minute_detail_days = _qint("minute_detail_days", 30)
        max_minute_source_checks = _qint("max_minute_source_checks", 60)

        async def _run() -> None:
            from scripts.diagnose_gaps import _notify_feishu, run_diagnosis_report

            request.app.state.diagnose_gaps_running = True
            try:
                await run_diagnosis_report(
                    storage,
                    feishu=True,
                    daily_detail_days=daily_detail_days,
                    minute_detail_days=minute_detail_days,
                    max_minute_source_checks=max_minute_source_checks,
                )
            except Exception as e:
                logger.error("diagnose-gaps 失败: %s", e, exc_info=True)
                await _notify_feishu(f"[数据诊断报告] 执行异常\n{e}")
            finally:
                request.app.state.diagnose_gaps_running = False

        asyncio.create_task(_run())
        return JSONResponse({"success": True, "message": "已触发数据诊断,结果发飞书"})

    # ------------------------------------------------------------------
    # 灌权威上市索引 (POST /api/audit/listing-info/load-tushare)
    # 用 Tushare stock_basic(在市+已退市 2 次调用)把每只票的上市/退市日灌进
    # stock_listing_info,建成「一查即知」的权威 list。无需 kimi。
    # ------------------------------------------------------------------

    @router.post("/listing-info/load-tushare")
    async def trigger_load_listing(request: Request) -> JSONResponse:
        """Build the queryable listing index from Tushare stock_basic (background)."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "load_listing_running", False):
            return JSONResponse({"success": False, "message": "正在灌入中,请稍后"})

        async def _run() -> None:
            from scripts.load_listing_from_tushare import run_load_listing

            request.app.state.load_listing_running = True
            try:
                await run_load_listing(storage, feishu=True)
            except Exception as e:
                logger.error("load-tushare listing 失败: %s", e, exc_info=True)
                from scripts.load_listing_from_tushare import _notify_feishu

                await _notify_feishu(f"[上市索引] 灌入失败\n{e}")
            finally:
                request.app.state.load_listing_running = False

        asyncio.create_task(_run())
        return JSONResponse({"success": True, "message": "已触发:从 Tushare 灌权威上市索引"})

    # ------------------------------------------------------------------
    # 只补日线的历史回填 (POST /api/audit/backfill-daily)
    # 日线按交易日整批拉(自带主板/创业板/科创板/北交所),所以重下即可补全
    # 那些"只存了主板"的历史日子。分钟线 NOT 触碰(单独跑,免得拖垮小机器)。
    # 范围 ?start=YYYY-MM-DD&end=YYYY-MM-DD,默认 2024-01-01 ~ 昨天。
    # ------------------------------------------------------------------

    @router.post("/backfill-daily")
    async def trigger_backfill_daily(request: Request) -> JSONResponse:
        """Daily backfill (DAT-006 阶段2). Minute untouched.

        Default = INDEX-DRIVEN: fill only trading_calendar ``daily_state=missing``
        (run ``/calendar/rebuild`` 阶段1 first); ``ok`` + ``source_none`` skipped,
        so 源头不可抗 is never retried.

        ``?mode=full&start=&end=`` = audit-based full-range re-download (bootstrap
        or extend to a new range; default range = CACHE_START_DATE~today).
        """
        storage = getattr(request.app.state, "storage", None)
        pipeline = getattr(request.app.state, "pipeline", None)
        if storage is None or pipeline is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage/pipeline 未就绪")
        if getattr(request.app.state, "backfill_daily_running", False):
            return JSONResponse({"success": False, "message": "日线补全正在进行中,请稍后"})

        mode = request.query_params.get("mode", "index")

        async def _run_index() -> str:
            result = await pipeline.fill_daily_from_calendar(quiet=True)
            return (
                "【阶段二·补日线】✅ 完成\n"
                "任务: 照索引补「源头有、库里没有」的日线\n"
                "(已经齐的、以及「源头本就没有」的都跳过)\n"
                f"结果: 补回 {result['filled']:,} 行 / 跨 {result['dates']} 个有缺口的交易日\n"
                "(随后重建一次索引即可确认这些缺口归零)"
            )

        async def _run_full() -> str:
            from datetime import date, datetime, timedelta
            from zoneinfo import ZoneInfo

            from src.data.services.cache_scheduler import CACHE_START_DATE

            def _qdate(name: str, default: date) -> date:
                v = request.query_params.get(name)
                if not v:
                    return default
                try:
                    return datetime.strptime(v, "%Y-%m-%d").date()
                except ValueError:
                    return default

            today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
            start = _qdate("start", CACHE_START_DATE)
            # daily interface only goes to T-1 (today's daily isn't published yet);
            # ending at "today" would falsely mark all of today's roster as missing.
            end = _qdate("end", today - timedelta(days=1))
            result = await pipeline.download_prices(start, end, skip_minute=True, quiet=True)
            return (
                "【阶段二·补日线·全量】✅ 完成\n"
                f"范围: {start} ~ {end}(全量重下,用于建底/扩大范围)\n"
                f"{result.get('verify_msg')}"
            )

        async def _run() -> None:
            request.app.state.backfill_daily_running = True
            try:
                msg = await (_run_full() if mode == "full" else _run_index())
            except Exception as e:
                logger.error("backfill-daily 失败: %s", e, exc_info=True)
                msg = f"【阶段二·补日线】❌ 失败\n{e}"
            finally:
                request.app.state.backfill_daily_running = False
            try:
                from src.common.feishu_bot import FeishuBot

                bot = FeishuBot()
                if bot.is_configured():
                    await bot.send_message(msg)
            except Exception as e:  # noqa: BLE001
                logger.warning("backfill-daily 飞书通知失败: %s", e)

        asyncio.create_task(_run())
        label = "全量" if mode == "full" else "索引驱动"
        return JSONResponse({"success": True, "message": f"已触发日线补全({label}),结果发飞书"})

    # ------------------------------------------------------------------
    # 重建交易日历真值表 (POST /api/audit/calendar/rebuild) — DAT-006
    # 每(交易日 × 股票)复合一行真值:roster(Tushare 上市日) ∩ suspend_d ∩
    # Tushare daily ∩ backtest_daily → 状态。范围 ?start=&end=,默认 2024-01-01~昨天。
    # ------------------------------------------------------------------

    @router.post("/calendar/rebuild")
    async def trigger_calendar_rebuild(request: Request) -> JSONResponse:
        """Rebuild the trading_calendar truth table over a range (DAT-006)."""
        from datetime import date, datetime, timedelta
        from zoneinfo import ZoneInfo

        from src.data.services.cache_scheduler import CACHE_START_DATE

        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        if getattr(request.app.state, "calendar_rebuild_running", False):
            return JSONResponse({"success": False, "message": "真值表重建正在进行中,请稍后"})

        def _qdate(name: str, default: date) -> date:
            v = request.query_params.get(name)
            if not v:
                return default
            try:
                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                return default

        today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
        start = _qdate("start", CACHE_START_DATE)
        # daily interface only goes to T-1; ending at "today" would falsely mark
        # all of today's roster as missing (today's daily isn't published yet).
        end = _qdate("end", today - timedelta(days=1))

        async def _run() -> None:
            from src.common.config import get_tushare_token
            from src.data.clients.tushare_realtime import (
                TushareRealtimeClient,
                get_tushare_trade_calendar,
            )
            from src.data.services.trading_calendar import build_calendar

            request.app.state.calendar_rebuild_running = True
            client = TushareRealtimeClient(token=get_tushare_token())
            await client.start()
            try:
                cal = await get_tushare_trade_calendar(
                    start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), token=get_tushare_token()
                )
                # get_tushare_trade_calendar returns YYYY-MM-DD; tolerate YYYYMMDD too.
                trading_days = [datetime.strptime(d.replace("-", ""), "%Y%m%d").date() for d in cal]

                async def fetch_suspended(day: date) -> set[str]:
                    return await client.fetch_suspended_stocks(day.strftime("%Y%m%d"))

                async def fetch_traded(day: date) -> set[str]:
                    recs = await client.fetch_daily(day.strftime("%Y%m%d"))
                    return {r["ticker"] for r in recs if (r.get("volume") or 0) > 0}

                result = await build_calendar(
                    storage,
                    trading_days=trading_days,
                    fetch_suspended=fetch_suspended,
                    fetch_traded=fetch_traded,
                )
                bd = result.get("by_state", {})
                _labels = {
                    "ok": "正常",
                    "missing": "真缺待补(源头有·库没有)",
                    "wrong_suspended": "标错停牌(库标停牌·实际有成交)",
                    "wrong_traded": "标错交易",
                    "orphan": "有数据却不在册",
                    "source_none": "源头也无(接口数据不全·已标记,无需修)",
                }
                lines = "\n".join(
                    f"  · {_labels.get(k, k)}: {v:,}" for k, v in sorted(bd.items()) if v
                )
                msg = (
                    "【阶段一·索引建设】✅ 重建完成\n"
                    "任务: 把每天每只票汇总成一行真值(是否在册 / 是否停牌 / 数据对不对)\n"
                    f"范围: {start} ~ {end}\n"
                    f"规模: {result['days']} 个交易日, 写入 {result['rows']:,} 行\n"
                    f"本次范围内分类:\n{lines}\n"
                    f"(有问题的合计 {result['problem_rows']:,};除「源头也无」外都待修)"
                )
            except Exception as e:
                logger.error("calendar rebuild 失败: %s", e, exc_info=True)
                msg = f"【阶段一·索引建设】❌ 重建失败 {start}~{end}\n{e}"
            finally:
                await client.stop()
                request.app.state.calendar_rebuild_running = False
            try:
                from src.common.feishu_bot import FeishuBot

                bot = FeishuBot()
                if bot.is_configured():
                    await bot.send_message(msg)
            except Exception as e:  # noqa: BLE001
                logger.warning("calendar rebuild 飞书通知失败: %s", e)

        asyncio.create_task(_run())
        return JSONResponse(
            {"success": True, "message": f"已触发交易日历真值表重建 {start}~{end},结果发飞书"}
        )

    @router.post("/calendar/purge-codes-data")
    async def purge_codes_data(request: Request) -> JSONResponse:
        """DESTRUCTIVE: delete *all* backtest_daily rows for the given codes.

        For purging dead-alias codes — 北交所 老代码 (43x/83x/87x) globally migrated
        to 920x on 2025-10-09. Tushare serves their full history under the 920 code,
        so the old-code rows are redundant orphans (left over after load-tushare
        drops them from the roster). Their data is re-derivable under the 920 code,
        so this is recoverable in practice — but still requires an EXPLICIT code
        list (no auto-purge) and is capped + logged + Feishu'd.

        Body: ``{"codes": ["430198", "830964", ...]}``.
        """
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="非法 JSON body")
        raw = body.get("codes") or []
        codes = [s for c in raw if (s := str(c).strip()).isdigit() and len(s) == 6]
        if not codes:
            raise HTTPException(status_code=400, detail="需提供 6 位数字代码清单 (codes)")
        if len(codes) > 500:
            raise HTTPException(status_code=400, detail="一次最多清理 500 只,请分批")
        result = await storage.delete_daily_by_codes(codes)
        msg = (
            "【数据清理】已删除迁移老代码的冗余日线\n"
            f"删除 {result['codes']} 只代码 / 共 {result['deleted']:,} 行\n"
            "(这些是北交所改代码后的废弃老代码,数据已在 920 新代码下,删的是冗余副本)\n"
            "下一步: 补全 920 新代码全历史 → 重建真值表"
        )
        logger.info("purge-codes-data: %s codes, %s rows", result["codes"], result["deleted"])
        try:
            from src.common.feishu_bot import FeishuBot

            bot = FeishuBot()
            if bot.is_configured():
                await bot.send_message(msg)
        except Exception:  # noqa: BLE001
            logger.warning("purge-codes-data 飞书通知失败", exc_info=True)
        return JSONResponse({"success": True, **result})

    @router.post("/calendar/purge-orphan-rows")
    async def purge_orphan_rows(request: Request) -> JSONResponse:
        """Delete the backtest_daily rows the truth-table marks ``orphan`` (有数据
        却不在册). Precise: deletes ONLY the orphan (code, day) rows, so a delisted
        stock keeps its valid pre-delist history while dead codes lose everything.

        ``?execute=1`` performs the delete; without it this is a DRY RUN (returns
        the breakdown so you can review before deleting anything). Run a calendar
        rebuild afterwards to reflect orphan → 0."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            raise HTTPException(status_code=503, detail="GreptimeDB storage 未就绪")
        execute = request.query_params.get("execute") in ("1", "true", "yes")
        result = await storage.purge_orphan_daily_rows(execute=execute)
        if execute:
            msg = (
                "【数据清理】已删除「有数据却不在册」的孤儿日线\n"
                f"涉及 {result['codes']} 只代码 / 删除 {result.get('deleted', 0):,} 行\n"
                "(退市后残留的占位行 + 北交所改代码/重组后废弃老代码的冗余数据)\n"
                "下一步: 重建真值表 → 孤儿应归零"
            )
            logger.info(
                "purge-orphan-rows executed: %s codes, %s rows",
                result["codes"],
                result.get("deleted"),
            )
            try:
                from src.common.feishu_bot import FeishuBot

                bot = FeishuBot()
                if bot.is_configured():
                    await bot.send_message(msg)
            except Exception:  # noqa: BLE001
                logger.warning("purge-orphan-rows 飞书通知失败", exc_info=True)
        return JSONResponse({"success": True, **result})

    @router.get("/calendar/status")
    async def calendar_status(request: Request) -> JSONResponse:
        """Query the trading_calendar truth table: per-state / per-trade-status
        counts + covered date range + whether a rebuild is running (DAT-006)."""
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            return JSONResponse({"ready": False})
        try:
            summary = await storage.get_trading_calendar_summary()
        except Exception as e:
            logger.warning("calendar_status 读取失败: %s", e, exc_info=True)
            return JSONResponse({"ready": False, "error": str(e)})
        summary["ready"] = True
        summary["rebuild_running"] = bool(
            getattr(request.app.state, "calendar_rebuild_running", False)
        )
        return JSONResponse(summary)

    @router.get("/calendar/problems")
    async def calendar_problems(request: Request) -> JSONResponse:
        """List trading_calendar problem rows for one state (DAT-006), to inspect/fix.

        ``?state=missing|wrong_suspended|wrong_traded|orphan|source_none`` (default
        missing), ``?limit=`` (default 2000, max 20000).
        """
        storage = getattr(request.app.state, "storage", None)
        if storage is None or not getattr(storage, "is_ready", False):
            return JSONResponse({"ready": False})
        state = request.query_params.get("state", "missing")
        try:
            limit = int(request.query_params.get("limit", "2000"))
        except ValueError:
            limit = 2000
        try:
            rows = await storage.get_calendar_problems(state, limit=limit)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            logger.warning("calendar_problems 读取失败: %s", e, exc_info=True)
            return JSONResponse({"ready": False, "error": str(e)})
        return JSONResponse({"ready": True, "state": state, "count": len(rows), "rows": rows})

    return router
