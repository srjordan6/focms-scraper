#!/usr/bin/env python3
"""
focms_nightly_jobs.py v0.6.0 (2026-07-23)

NorthStar-Scraper cron entry point - runs every scheduled NorthStar job in
sequence, each isolated in its own subprocess so one failure never blocks the
others. Exits non-zero if ANY job failed so the Render run shows red.

Schedule: 30 4 * * * (daily 04:30 UTC). Manual Build after every code change.

Jobs:
  birthday-billing        daily    age-band membership re-billing (T-7 charge,
                                   birthday hold, emails)
  nces-scorecard-refresh  monthly  (1st of month) target-university data refresh
  scorecard-all-colleges  daily    outcomes data (cost, tuition, SAT/ACT bands,
                                   admit rate, enrolment) for EVERY college in
                                   the universities table, not just the ranked
                                   ones.
  ipeds-hd-refresh        weekly   (Sundays) directory data the Scorecard does
                                   not carry: institutional phone (GENTELE),
                                   street address, city/state/ZIP, website and
                                   admissions URL, from the IPEDS HD file, for
                                   EVERY college. Every write is COALESCE-
                                   guarded, so it fills gaps and never
                                   overwrites curated values. Weekly because HD
                                   is published annually - nightly is noise.

ONE LIST, ONE TRUTH
  `universities` is the single source of truth: every college we know about.
  The "US News National Universities" and "US News National Liberal Arts
  Colleges" lists are NOT separate stores - they are that same table filtered
  on us_news_rank_national / us_news_rank_liberal_arts. A student picking a
  ranked school reads the identical row an IPEDS search would return.
  Consequence for this file: refresh jobs run over ALL colleges, never over the
  ranked subset. Enriching only the ranked rows left ~5,500 colleges hollow, so
  any search outside the rankings surfaced a school with no address, phone,
  cost or test bands.

To add a job: append a row to JOBS. gate=None means every run.
"""

import logging
import subprocess
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nightly")

NOW = datetime.now(timezone.utc)

JOBS = [
    # (name, argv, gate: callable -> bool | None for always)
    ("birthday-billing", [sys.executable, "focms_birthday_billing.py"], None),
    ("nces-scorecard-refresh", [sys.executable, "nces_scorecard_worker.py", "refresh-targets"],
     lambda: NOW.day == 1),
    # v0.6.0 (2026-07-23): the refresh set is now EVERY college, not the ranked
    # subset. `universities` is the source of truth and the US News lists are
    # just filtered views of it, so enriching only ranked rows left the other
    # ~5,500 colleges without address, phone, cost or test bands - and those are
    # exactly the rows an IPEDS search returns.
    ("scorecard-all-colleges", [sys.executable, "nces_scorecard_worker.py", "refresh-all"],
     None),
    # IPEDS HD carries what the Scorecard has no field for at all: the
    # institutional telephone (GENTELE), street address and admissions URL.
    # HD is published annually, so Sundays is ample.
    ("ipeds-hd-refresh", [sys.executable, "ipeds_hd_worker.py", "refresh-all"],
     lambda: NOW.weekday() == 6),
]


def main() -> int:
    failures = 0
    for name, argv, gate in JOBS:
        if gate is not None and not gate():
            log.info("skip %s (gate not met today)", name)
            continue
        log.info("=== start %s ===", name)
        try:
            r = subprocess.run(argv, timeout=3600)
            if r.returncode == 0:
                log.info("=== done %s ===", name)
            else:
                failures += 1
                log.error("=== FAILED %s rc=%s ===", name, r.returncode)
        except subprocess.TimeoutExpired:
            failures += 1
            log.error("=== TIMEOUT %s (1h) ===", name)
        except Exception as exc:
            failures += 1
            log.error("=== ERROR %s: %r ===", name, exc)
    log.info("nightly run complete: %s job(s) failed", failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
