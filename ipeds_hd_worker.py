#!/usr/bin/env python3
"""
ipeds_hd_worker.py v0.1.0 (2026-07-23)

Keeps the universities table current with the IPEDS HD (Directory) file.

WHY THIS EXISTS
---------------
The College Scorecard API has no telephone field at all, and its address block
is the mailing address rather than the institutional directory record. IPEDS HD
carries GENTELE (general institutional phone), ADDR/CITY/STABBR/ZIP, WEBADDR and
ADMINURL for ~6,100 institutions with ~98% phone coverage. Scorecard and IPEDS
are complementary, not redundant: this worker owns the directory columns and
nces_scorecard_worker.py owns the outcomes columns (cost, SAT/ACT, admit rate).

WHAT IT WRITES
--------------
  admissions_phone    <- GENTELE, normalised to (NNN) NNN-NNNN
  admissions_address  <- "ADDR, CITY, STABBR ZIP"
  city / state        <- CITY / STABBR
  website             <- WEBADDR
  admissions_url      <- ADMINURL (only when IPEDS actually supplies one)
  admissions_source   <- appends 'ipeds_hd<YEAR>'
  admissions_updated_at <- now()

Never blanks a populated column: every assignment is COALESCE(new, existing) so
a hand-curated value or a Scorecard value survives. Phone is the exception it
owns outright, and even then a malformed source number is skipped rather than
written.

MODES
-----
  refresh-ranked   (default) every school carrying a US News rank
  refresh-all      every row in universities with a matching UNITID
  refresh-missing  only rows where admissions_phone IS NULL

USAGE
  python ipeds_hd_worker.py [mode]

ENV
  DATABASE_URL   required
  IPEDS_HD_YEAR  optional, pins the survey year (default: newest that resolves)
"""

import asyncio
import csv
import io
import logging
import os
import sys
import zipfile
from datetime import datetime, timezone
from urllib.request import urlopen, Request

import asyncpg

LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("ipeds-hd")

HD_URL = "https://nces.ed.gov/ipeds/datacenter/data/HD{year}.zip"
UA = "Mozilla/5.0 (compatible; FOCMS-IPEDS-Worker/0.1)"
MODES = ("refresh-ranked", "refresh-all", "refresh-missing")


# ---------------------------------------------------------------- fetch/parse

def candidate_years():
    """Newest plausible HD years first. IPEDS publishes ~18 months in arrears."""
    pinned = os.environ.get("IPEDS_HD_YEAR", "").strip()
    if pinned:
        return [int(pinned)]
    now = datetime.now(timezone.utc).year
    return [now - 1, now - 2, now - 3, now - 4]


def download_hd():
    """Return (year, list-of-row-dicts). Raises if no year resolves."""
    last_err = None
    for year in candidate_years():
        url = HD_URL.format(year=year)
        try:
            req = Request(url, headers={"User-Agent": UA})
            with urlopen(req, timeout=120) as resp:
                if resp.status != 200:
                    last_err = f"HTTP {resp.status} for {url}"
                    continue
                blob = resp.read()
            zf = zipfile.ZipFile(io.BytesIO(blob))
            # Prefer the plain data file over any _RV (revised) companion.
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                last_err = f"no csv inside {url}"
                continue
            names.sort(key=lambda n: ("_rv" in n.lower(), len(n)))
            raw = zf.read(names[0])
            text = raw.decode("utf-8-sig", errors="replace")
            rows = list(csv.DictReader(io.StringIO(text)))
            if not rows:
                last_err = f"empty csv inside {url}"
                continue
            log.info("HD%s: %s institutions from %s", year, len(rows), names[0])
            return year, rows
        except Exception as exc:  # noqa: BLE001 - try the next year
            last_err = f"{url}: {exc}"
            log.warning("HD%s unavailable (%s)", year, exc)
    raise RuntimeError(f"no IPEDS HD file could be fetched: {last_err}")


def fmt_phone(raw):
    """(NNN) NNN-NNNN, or None when the source value is not a usable number."""
    digits = "".join(ch for ch in (raw or "") if ch.isdigit())
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def clean(v):
    v = (v or "").strip()
    return v or None


def fmt_url(raw):
    u = clean(raw)
    if not u:
        return None
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u
    return u


def build_record(row):
    """Map one HD row to the columns this worker owns."""
    uid = clean(row.get("UNITID"))
    if not uid:
        return None
    addr, city = clean(row.get("ADDR")), clean(row.get("CITY"))
    state, zipc = clean(row.get("STABBR")), clean(row.get("ZIP"))
    one_line = None
    if addr and city and state:
        one_line = f"{addr}, {city}, {state}" + (f" {zipc}" if zipc else "")
    return {
        "leaid": uid,
        "phone": fmt_phone(row.get("GENTELE")),
        "address": one_line,
        "city": city,
        "state": state,
        "website": fmt_url(row.get("WEBADDR")),
        "admissions_url": fmt_url(row.get("ADMINURL")),
    }


# ------------------------------------------------------------------- database

TARGET_SQL = {
    "refresh-ranked": (
        "SELECT leaid FROM universities "
        "WHERE us_news_rank_national IS NOT NULL "
        "   OR us_news_rank_liberal_arts IS NOT NULL"
    ),
    "refresh-all": "SELECT leaid FROM universities",
    "refresh-missing": (
        "SELECT leaid FROM universities WHERE admissions_phone IS NULL"
    ),
}

UPSERT_SQL = """
UPDATE universities u SET
    admissions_phone      = COALESCE($2, u.admissions_phone),
    admissions_address    = COALESCE(u.admissions_address, $3),
    city                  = COALESCE(u.city, $4),
    state                 = COALESCE(u.state, $5),
    website               = COALESCE(u.website, $6),
    admissions_url        = COALESCE(u.admissions_url, $7),
    admissions_source     = CASE
        WHEN COALESCE(u.admissions_source, '') = '' THEN $8
        WHEN u.admissions_source LIKE '%' || $8 || '%' THEN u.admissions_source
        ELSE u.admissions_source || '+' || $8
    END,
    admissions_updated_at = now()
WHERE u.leaid = $1
"""


async def run(mode):
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set")
        return 1

    year, rows = download_hd()
    by_uid = {}
    for r in rows:
        rec = build_record(r)
        if rec:
            by_uid[rec["leaid"]] = rec
    log.info("parsed %s usable HD records", len(by_uid))

    tag = f"ipeds_hd{year}"
    conn = await asyncpg.connect(dsn)
    try:
        targets = [r["leaid"] for r in await conn.fetch(TARGET_SQL[mode])]
        log.info("mode=%s targets=%s", mode, len(targets))

        matched = updated = no_phone = missing = 0
        async with conn.transaction():
            for leaid in targets:
                rec = by_uid.get(str(leaid))
                if not rec:
                    missing += 1
                    continue
                matched += 1
                if not rec["phone"]:
                    no_phone += 1
                await conn.execute(
                    UPSERT_SQL,
                    leaid,
                    rec["phone"],
                    rec["address"],
                    rec["city"],
                    rec["state"],
                    rec["website"],
                    rec["admissions_url"],
                    tag,
                )
                updated += 1

        cov = await conn.fetchrow(
            "SELECT count(*) AS total, count(admissions_phone) AS phone "
            "FROM universities WHERE us_news_rank_national IS NOT NULL "
            "   OR us_news_rank_liberal_arts IS NOT NULL"
        )
        log.info(
            "updated=%s matched=%s not-in-hd=%s no-usable-phone=%s",
            updated, matched, missing, no_phone,
        )
        log.info("ranked phone coverage now %s/%s", cov["phone"], cov["total"])
    finally:
        await conn.close()
    return 0


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "refresh-ranked"
    if mode not in MODES:
        log.error("unknown mode %r (expected one of %s)", mode, ", ".join(MODES))
        return 2
    log.info("=== ipeds-hd worker v0.1.0 mode=%s ===", mode)
    return asyncio.run(run(mode))


if __name__ == "__main__":
    sys.exit(main())
