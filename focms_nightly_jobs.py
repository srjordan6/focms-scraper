#!/usr/bin/env python3
"""
focms_nightly_jobs.py v0.4.0 (2026-07-22)

NorthStar-Scraper cron entry point - runs every scheduled NorthStar job in
sequence, each isolated in its own subprocess so one failure never blocks the
others. Exits non-zero if ANY job failed so the Render run shows red.

Schedule: 30 4 * * * (daily 04:30 UTC). Manual Build after every code change.

Jobs:
  birthday-billing        daily    age-band membership re-billing (T-7 charge,
                                   birthday hold, emails)
  nces-scorecard-refresh  monthly  (1st of month) target-university data refresh
  usnews-admissions       daily    v0.4.0: fills admissions address/URL for all
                                   US-News-ranked schools (Target Schools
                                   dropdown, 104 schools) via Scorecard
                                   refresh-ranked mode. Nightly until complete,
                                   then switch its gate to weekly (Sundays).

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
    # v0.4.0 (2026-07-22): fill admissions address/URL for every US-News-ranked
    # school (the Target Schools dropdown set, 104 schools) via Scorecard.
    # Runs NIGHTLY so the ranked list fills fast; once complete, switch the gate
    # to weekly by replacing None with:  lambda: NOW.weekday() == 6  (Sundays).
    ("usnews-admissions", [sys.executable, "nces_scorecard_worker.py", "refresh-ranked"],
     None),
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
