# === MODULE PURPOSE ===
# ONE global in-process lock serializing every kimi-cli spawn.
#
# kimi now has three consumers in this service:
#   ① daily-maintenance pipeline step ② (listing verify, kimi_listing_verifier)
#   ② NOTE-002 AI journal writer (src/notes/ai_journal.py)
#   ③ AST-001 Feishu assistant (src/assistant/)
# Each already runs serially *within itself*, but nothing stopped two consumers
# from spawning kimi concurrently (e.g. assistant query during the 3am pipeline).
# kimi runs are heavyweight (minutes, network, memory) and share one API key /
# quota, so cross-consumer concurrency is capped at 1 — acquire this lock around
# the whole spawn-to-exit section, never around post-processing.

import asyncio

# Bound to the running loop on first await — this service has exactly one loop.
KIMI_GLOBAL_LOCK = asyncio.Lock()
