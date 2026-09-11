#!/usr/bin/env bash
# Resume the NL primary (basisonderwijs) description backfill.
#
# Google's grounded-search free tier allows roughly 1,500 requests/day, and the
# 6,060 primary schools need one grounded call each. The pipeline is resumable:
# a school with a grounded description is skipped, and one written without
# research (pass1_grounded=false) is regenerated once research succeeds — so
# running this daily walks the backlog down without redoing finished work.
#
# Safe to run at any time: if the quota is already spent it burns a few 429s,
# logs them, and exits without corrupting anything.
set -uo pipefail

REPO="/Volumes/Patriot SSD/AI-Side-Projects/schoolnossa"
WORKTREE="$REPO/.claude/worktrees/data-asset-expansion-priority-17b5fe"
LOG="/tmp/po_desc_$(date +%Y%m%d_%H%M).log"

cd "$WORKTREE" || exit 1
export GEMINI_API_KEY="$(grep -E '^GEMINI_API_KEY=' "$REPO/.env" | cut -d= -f2- | tr -d '"'"'"'')"
# 2 workers: the quota is daily rather than per-second, so more concurrency
# only spends the same allowance faster and draws avoidable 429s.
export GEMINI_MODEL=gemini-flash-lite-latest
export DESC_WORKERS=2

"$REPO/venv/bin/python" -u scripts_international/description_pipeline_international.py \
    --country NL_PO --passes 0,1 > "$LOG" 2>&1

"$REPO/venv/bin/python" - <<'PY'
import json
c = json.load(open('data_nl_po/cache/description_pipeline.json'))
grounded = sum(1 for v in c.values() if v.get('pass1_grounded') is True)
ungrounded = sum(1 for v in c.values() if v.get('pass1_grounded') is False)
print(f"RESULT grounded={grounded} ungrounded={ungrounded} total={len(c)}")
print("REMAINING_DAYS", max(0, -(-ungrounded // 1400)))
PY
echo "log: $LOG"
