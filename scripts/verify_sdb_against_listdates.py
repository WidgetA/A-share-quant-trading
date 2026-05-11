# === MODULE PURPOSE ===
# Cross-check: for every (date, code) pair where `code` appeared in the
# B - D - S diff for that date, verify the date is strictly *before*
# the real list_date that kimi-cli independently fetched.
#
# If ALL pairs satisfy diff_date < list_date → "未来股污染" hypothesis holds
# across the entire 2023..today range, and bak_basic can be safely dropped.
# If ANY pair has diff_date >= list_date → that's a counterexample — B is
# contributing real information that D ∪ S misses, can't drop.

from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

SDB_DIR = Path("data/audit/sdb")
LIST_DATES_DIR = Path("data/audit/list_dates")


def _load_list_dates() -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for f in LIST_DATES_DIR.glob("*.json"):
        d = json.loads(f.read_text(encoding="utf-8"))
        code = d.get("code") or f.stem
        out[code] = d.get("list_date")  # may be None
    return out


def _parse_date(s: str) -> date | None:
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def main() -> int:
    list_dates_raw = _load_list_dates()
    list_dates: dict[str, date] = {}
    not_found: set[str] = set()
    for code, ld in list_dates_raw.items():
        if ld is None:
            not_found.add(code)
            continue
        pd = _parse_date(ld)
        if pd is None:
            not_found.add(code)
        else:
            list_dates[code] = pd

    print(f"已知真实 list_date 的代码: {len(list_dates)}", file=sys.stderr)
    print(f"kimi 标 not_found 的代码:   {len(not_found)}", file=sys.stderr)

    sdb_files = sorted(SDB_DIR.glob("*.json"))
    print(f"待核查 sdb 文件:            {len(sdb_files)} 天", file=sys.stderr)

    pass_count = 0  # diff_date < list_date (符合假说)
    fail_count = 0  # diff_date >= list_date (反例)
    skip_count = 0  # 代码在 not_found 池里,无法判定
    counterexamples: list[tuple[str, str, str]] = []  # (diff_date, code, list_date_str)
    # Distribution: for each code, how many days does it appear ahead of its list_date?
    days_ahead_dist: list[int] = []
    code_appearances: dict[str, int] = defaultdict(int)

    for f in sdb_files:
        d = json.loads(f.read_text(encoding="utf-8"))
        sdb_date_str = d["date"]
        sdb_date = _parse_date(sdb_date_str)
        if sdb_date is None:
            continue
        for code in d["b_minus_d_minus_s"]:
            code_appearances[code] += 1
            if code in not_found:
                skip_count += 1
                continue
            ld = list_dates.get(code)
            if ld is None:
                skip_count += 1
                continue
            if sdb_date < ld:
                pass_count += 1
                days_ahead_dist.append((ld - sdb_date).days)
            else:
                fail_count += 1
                counterexamples.append((sdb_date_str, code, ld.isoformat()))

    total_pairs = pass_count + fail_count + skip_count
    print(file=sys.stderr)
    print(f"=== 总 (date, code) 配对数: {total_pairs} ===", file=sys.stderr)
    print(
        f"  ✅ 符合假说 (sdb_date < list_date): {pass_count}  "
        f"({pass_count / total_pairs * 100:.2f}%)",
        file=sys.stderr,
    )
    print(
        f"  ❌ 反例     (sdb_date >= list_date): {fail_count}  "
        f"({fail_count / total_pairs * 100:.2f}%)",
        file=sys.stderr,
    )
    print(
        f"  ⏭ 跳过     (code 未拿到真实 list_date): {skip_count}  "
        f"({skip_count / total_pairs * 100:.2f}%)",
        file=sys.stderr,
    )

    if counterexamples:
        print(file=sys.stderr)
        print("=== 反例样本 (前 20 条) ===", file=sys.stderr)
        for sdb_d, code, ld in counterexamples[:20]:
            print(f"  sdb_date={sdb_d}  code={code}  真实 list_date={ld}", file=sys.stderr)
        if len(counterexamples) > 20:
            print(f"  ... 及其余 {len(counterexamples) - 20} 条", file=sys.stderr)

    if days_ahead_dist:
        import statistics

        print(file=sys.stderr)
        print(
            "=== 符合假说的配对中, sdb_date 提前 list_date 的天数分布 ===",
            file=sys.stderr,
        )
        print(
            f"  最小={min(days_ahead_dist)} 中位={int(statistics.median(days_ahead_dist))} "
            f"平均={statistics.mean(days_ahead_dist):.1f} 最大={max(days_ahead_dist)}",
            file=sys.stderr,
        )

    # Also print which "not found" codes are most actively in the diff —
    # those are the highest-value to chase down independently.
    print(file=sys.stderr)
    print("=== kimi 标 not_found 但在 diff 里活跃的 top 10 ===", file=sys.stderr)
    nf_active = sorted(
        ((code_appearances.get(c, 0), c) for c in not_found),
        reverse=True,
    )[:10]
    for cnt, c in nf_active:
        if cnt:
            print(f"  {c}: 出现于 {cnt} 个 sdb 文件中", file=sys.stderr)

    # Write a structured report file.
    report = {
        "total_pairs": total_pairs,
        "pass": pass_count,
        "fail": fail_count,
        "skip": skip_count,
        "counterexamples": [
            {"sdb_date": d, "code": c, "real_list_date": ld} for d, c, ld in counterexamples
        ],
        "not_found_codes_with_appearance_count": {c: code_appearances.get(c, 0) for c in not_found},
    }
    Path("data/audit/sdb_verification_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(file=sys.stderr)
    print("写入: data/audit/sdb_verification_report.json", file=sys.stderr)
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
