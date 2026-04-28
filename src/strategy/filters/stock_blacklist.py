# === MODULE PURPOSE ===
# Hardcoded stock-level blacklist. Stocks listed here are removed at the
# very top of the funnel (before Step 0 universe enters any filter), and
# also stripped from the training data stream so the ranking model does
# not learn from their bars.
#
# This is for individual stocks that violate strategy assumptions even
# though they pass every other filter (e.g. structurally low-volatility
# heavyweights, illiquid tickers that print misleading early-window
# signals, names that have repeatedly produced false positives).
#
# Board-level filtering lives in `board_filter.JUNK_BOARDS`. Use this
# module ONLY for individual codes — if a whole theme is bad, add the
# board there instead.
#
# === MAINTENANCE ===
# - Add codes here, commit, push. Watchtower picks up the new image
#   on prod within ~60s of CI passing.
# - Always include a reason (the value in the dict). The reason is the
#   audit trail — without it, future-you will not remember why a code
#   was banned and will be tempted to re-add it.
# - Codes are 6-digit bare codes (no .SH/.SZ suffix), matching the
#   convention used everywhere else in the funnel.

# code → reason. Reason MUST be filled in (see module doc).
BLACKLISTED_STOCKS: dict[str, str] = {
    # Add entries as: "600000": "reason why this code is excluded (added YYYY-MM-DD)",
}


def is_blacklisted(code: str) -> bool:
    """Return True if a 6-digit bare stock code is blacklisted."""
    return code in BLACKLISTED_STOCKS


def filter_blacklisted(codes):
    """Yield codes from `codes` that are not blacklisted."""
    for code in codes:
        if code not in BLACKLISTED_STOCKS:
            yield code
