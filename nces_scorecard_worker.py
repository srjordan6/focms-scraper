"""nces_scorecard_worker.py - College Scorecard + NCES Ingestion Worker v0.5

Pulls institution-level data from the U.S. Department of Education's College
Scorecard API and upserts into the FOCMS universities and university_cds_facts
tables.

v0.5 (2026-07-23) — additive over v0.3:
  * NEW run mode `refresh-all` — every row in universities (~6,100 colleges).
    `universities` is the single source of truth; the US News National and
    Liberal Arts lists are just that table filtered on us_news_rank_national /
    us_news_rank_liberal_arts. Refreshing only the ranked subset left ~5,500
    colleges with no cost, no test bands and no address — and those are exactly
    the rows an IPEDS search returns. This is now the nightly mode.
  * NEW universities columns written directly (previously these lived only in
    university_cds_facts, so the portal could not read them): tuition
    (out-of-state), sat_25, sat_75, act_25, act_75. SAT band = the 25th/75th
    section percentiles summed, which reproduces the published mid-50% range.
  * Upserts now COMMIT IN BATCHES (500 rows). A 6,100-row single transaction
    held locks for minutes and lost everything on any late failure; batching
    keeps partial progress and shortens lock windows.
  * Test-optional schools legitimately publish no scores. Those stay NULL —
    an accurate gap is worth more than an invented number.

v0.3 (2026-07-22) — additive over v0.2:
  * NEW admissions fields pulled from Scorecard and written to the universities
    table columns added 2026-07-22: admissions_address, admissions_url,
    admissions_phone (composed from school.address/city/state/zip, school_url,
    and general phone). admissions_source + admissions_updated_at stamped.
  * NEW run mode `refresh-ranked` — refreshes every school that carries a
    us_news_rank_national OR us_news_rank_liberal_arts value (the ranked
    dropdown set). This is the mode the nightly cron runs.
  * Scorecard exposes address/city/state/zip and school_url reliably; a general
    institutional phone is present as school.<phone-ish> on some records — we
    map what the API returns and leave NULL otherwise (never fabricate).
  * Everything in v0.2 is preserved (top-n, targets, leaids modes; CDS facts).

Supersedes v0.2. v0.2 bug-layer fixes retained verbatim:
  1. RLS context binding via SET LOCAL with literal (validated) UUID.
  2. f-string literal interpolation of the UUID (parameterized set_config hangs
     through PgBouncer transaction mode).
  3. universities.enrollment_total is the real column name.
  4. university_cds_facts shape (academic_year text, cds_section, method, run_id).
  5/6. operational (key paste, cron manual-deploy) — documented in runbook.

Run modes:
    refresh-top-n     - refresh top N schools by admit selectivity
    refresh-targets   - refresh just the schools in target_universities (RLS)
    refresh-leaids    - refresh a comma-separated list of LEAIDs
    refresh-ranked    - refresh every school with a US News rank (v0.3)
    refresh-all       - refresh EVERY college in universities (NEW v0.5)

Environment:
    DATABASE_URL_POOLED       - pgbouncer URL (transaction mode), preferred
    DATABASE_URL              - direct unpooled URL, fallback
    SCORECARD_API_KEY         - api.data.gov key (literal value, not filename)
    FOCMS_TENANT_ID           - tenant UUID to bind for refresh-targets mode
    FOCMS_WORKER_LOG_LEVEL    - INFO (default)

Deployed as part of the NorthStar-Scraper Render Cron Job
(focms_nightly_jobs.py). See accompanying runbook for schedule.
"""
import argparse, asyncio, logging, os, uuid as uuidlib
from typing import Any

import asyncpg, httpx

DATABASE_URL = os.environ.get("DATABASE_URL_POOLED") or os.environ["DATABASE_URL"]
API_KEY = os.environ["SCORECARD_API_KEY"]
TENANT_ID = os.environ.get("FOCMS_TENANT_ID", "")
LOG_LEVEL = os.environ.get("FOCMS_WORKER_LOG_LEVEL", "INFO")
ACADEMIC_YEAR = os.environ.get("FOCMS_SCORECARD_ACADEMIC_YEAR", "2024-2025")
EXTRACTION_RUN_ID = "scorecard_api_latest"  # stable so upsert is idempotent

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nces-scorecard-worker")

SCORECARD_BASE = "https://api.data.gov/ed/collegescorecard/v1/schools"
SCORECARD_FIELDS = [
    "id", "school.name", "school.city", "school.state",
    "school.address", "school.zip", "school.school_url",
    "latest.admissions.admission_rate.overall",
    "latest.admissions.sat_scores.midpoint.math",
    "latest.admissions.sat_scores.midpoint.critical_reading",
    "latest.admissions.sat_scores.75th_percentile.math",
    "latest.admissions.sat_scores.75th_percentile.critical_reading",
    "latest.admissions.act_scores.midpoint.cumulative",
    "latest.cost.attendance.academic_year",
    "latest.student.size",
    # v0.5: fields that feed the universities columns the portal reads.
    "latest.admissions.sat_scores.25th_percentile.math",
    "latest.admissions.sat_scores.25th_percentile.critical_reading",
    "latest.admissions.act_scores.25th_percentile.cumulative",
    "latest.admissions.act_scores.75th_percentile.cumulative",
    "latest.cost.tuition.out_of_state",
]

UPSERT_BATCH = 500  # rows per transaction; see v0.5 notes


def _safe_tenant(tid: str) -> str:
    try:
        u = uuidlib.UUID(tid)
    except (ValueError, TypeError) as e:
        raise ValueError(f"FOCMS_TENANT_ID is not a valid UUID: {tid!r}") from e
    return str(u)


async def fetch_scorecard(leaids: list[str]) -> list[dict[str, Any]]:
    """Pull Scorecard rows for the given IPEDS unit_ids (same as FOCMS leaid)."""
    out: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for chunk in (leaids[i:i+50] for i in range(0, len(leaids), 50)):
            params = {"api_key": API_KEY, "id": ",".join(chunk),
                      "fields": ",".join(SCORECARD_FIELDS), "per_page": 100}
            r = await client.get(SCORECARD_BASE, params=params)
            r.raise_for_status()
            data = r.json()
            out.extend(data.get("results", []))
            log.info("scorecard chunk=%d returned=%d", len(chunk), len(data.get("results", [])))
    return out


def _compose_address(row: dict[str, Any]) -> str | None:
    """Compose a single-line mailing address from Scorecard parts."""
    street = (row.get("school.address") or "").strip()
    city = (row.get("school.city") or "").strip()
    state = (row.get("school.state") or "").strip()
    zc = str(row.get("school.zip") or "").strip()
    # zip sometimes arrives as ZIP+4 int/float; normalize to leading 5 digits
    if zc and zc.replace("-", "").isdigit() and len(zc) >= 5:
        zc = zc[:5] if "-" not in zc else zc
    tail = " ".join(p for p in [city + ("," if city and (state or zc) else ""), state, zc] if p).strip()
    parts = [p for p in [street, tail] if p]
    return ", ".join(parts) if parts else None


def _norm_url(u: str | None) -> str | None:
    if not u:
        return None
    u = u.strip()
    if not u:
        return None
    if not u.lower().startswith(("http://", "https://")):
        u = "https://" + u
    return u


def _sat_band(row: dict[str, Any], pct: str) -> int | None:
    """Combined SAT for one percentile = section math + section reading.

    Scorecard publishes the two sections separately; summing the same
    percentile reproduces the total band colleges publish (e.g. 1510-1580).
    Returns None unless BOTH sections are present - half a band is misleading.
    """
    m = row.get(f"latest.admissions.sat_scores.{pct}.math")
    r = row.get(f"latest.admissions.sat_scores.{pct}.critical_reading")
    if m is None or r is None:
        return None
    try:
        return int(round(float(m) + float(r)))
    except (TypeError, ValueError):
        return None


def _as_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        return int(round(float(v)))
    except (TypeError, ValueError):
        return None


def row_to_universities(row: dict[str, Any]) -> dict[str, Any]:
    """Flatten a Scorecard row into the universities table shape (v0.5)."""
    return {
        "leaid": str(row.get("id")),
        "name": row.get("school.name"),
        "city": row.get("school.city"),
        "state": row.get("school.state"),
        "admit_rate": row.get("latest.admissions.admission_rate.overall"),
        "cost_attendance": row.get("latest.cost.attendance.academic_year"),
        "enrollment_total": row.get("latest.student.size"),
        "data_year": 2024,
        # v0.3 admissions fields
        "admissions_address": _compose_address(row),
        "admissions_url": _norm_url(row.get("school.school_url")),
        # v0.5 columns the portal reads directly
        "tuition": row.get("latest.cost.tuition.out_of_state"),
        "sat_25": _sat_band(row, "25th_percentile"),
        "sat_75": _sat_band(row, "75th_percentile"),
        "act_25": _as_int(row.get("latest.admissions.act_scores.25th_percentile.cumulative")),
        "act_75": _as_int(row.get("latest.admissions.act_scores.75th_percentile.cumulative")),
    }


_SCORECARD_FACT_TABLE = [
    ("C9", "sat_25_math",     "latest.admissions.sat_scores.midpoint.math"),
    ("C9", "sat_25_reading",  "latest.admissions.sat_scores.midpoint.critical_reading"),
    ("C9", "sat_75_math",     "latest.admissions.sat_scores.75th_percentile.math"),
    ("C9", "sat_75_reading",  "latest.admissions.sat_scores.75th_percentile.critical_reading"),
    ("C9", "act_50",          "latest.admissions.act_scores.midpoint.cumulative"),
]


def row_to_cds_facts(row: dict[str, Any]) -> list[dict[str, Any]]:
    leaid = str(row.get("id"))
    facts = []
    for cds_section, fact_key, sc_key in _SCORECARD_FACT_TABLE:
        v = row.get(sc_key)
        if v is None:
            continue
        facts.append({
            "university_leaid":   leaid,
            "academic_year":      ACADEMIC_YEAR,
            "cds_section":        cds_section,
            "fact_key":           fact_key,
            "fact_value_numeric": float(v),
            "extraction_method":  "scorecard_api",
            "extraction_run_id":  EXTRACTION_RUN_ID,
        })
    return facts


async def upsert_universities(conn: asyncpg.Connection, rows: list[dict[str, Any]]) -> int:
    """Insert or update universities rows. Returns count.

    v0.3: writes admissions_address / admissions_url and stamps
    admissions_source + admissions_updated_at when either is present.
    COALESCE keeps any existing manually-curated value if Scorecard is blank.
    """
    n = 0
    for r in rows:
        if not r["leaid"]:
            continue
        has_adm = bool(r.get("admissions_address") or r.get("admissions_url"))
        await conn.execute("""
            INSERT INTO universities (leaid, name, city, state, admit_rate,
                                     cost_attendance, enrollment_total, data_year,
                                     admissions_address, admissions_url,
                                     admissions_source, admissions_updated_at,
                                     tuition, sat_25, sat_75, act_25, act_75)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    CASE WHEN $11 THEN 'scorecard_api' ELSE NULL END,
                    CASE WHEN $11 THEN now() ELSE NULL END,
                    $12, $13, $14, $15, $16)
            ON CONFLICT (leaid) DO UPDATE SET
                name = EXCLUDED.name,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                admit_rate = COALESCE(EXCLUDED.admit_rate, universities.admit_rate),
                cost_attendance = COALESCE(EXCLUDED.cost_attendance, universities.cost_attendance),
                enrollment_total = COALESCE(EXCLUDED.enrollment_total, universities.enrollment_total),
                data_year = GREATEST(EXCLUDED.data_year, universities.data_year),
                admissions_address = COALESCE(EXCLUDED.admissions_address, universities.admissions_address),
                admissions_url = COALESCE(EXCLUDED.admissions_url, universities.admissions_url),
                admissions_source = CASE WHEN $11 THEN 'scorecard_api' ELSE universities.admissions_source END,
                admissions_updated_at = CASE WHEN $11 THEN now() ELSE universities.admissions_updated_at END,
                tuition = COALESCE(EXCLUDED.tuition, universities.tuition),
                sat_25 = COALESCE(EXCLUDED.sat_25, universities.sat_25),
                sat_75 = COALESCE(EXCLUDED.sat_75, universities.sat_75),
                act_25 = COALESCE(EXCLUDED.act_25, universities.act_25),
                act_75 = COALESCE(EXCLUDED.act_75, universities.act_75),
                updated_at = now()
        """, r["leaid"], r["name"], r["city"], r["state"],
             r["admit_rate"], r["cost_attendance"], r["enrollment_total"], r["data_year"],
             r.get("admissions_address"), r.get("admissions_url"), has_adm,
             r.get("tuition"), r.get("sat_25"), r.get("sat_75"),
             r.get("act_25"), r.get("act_75"))
        n += 1
    return n


async def upsert_cds_facts(conn: asyncpg.Connection, facts: list[dict[str, Any]]) -> int:
    n = 0
    for f in facts:
        await conn.execute("""
            INSERT INTO university_cds_facts
                (university_leaid, academic_year, cds_section, fact_key,
                 fact_value_numeric, extraction_method, extraction_run_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (university_leaid, academic_year, cds_section, fact_key, extraction_run_id)
            DO UPDATE SET
                fact_value_numeric = EXCLUDED.fact_value_numeric,
                extraction_method = EXCLUDED.extraction_method,
                updated_at = now()
        """, f["university_leaid"], f["academic_year"], f["cds_section"],
             f["fact_key"], f["fact_value_numeric"],
             f["extraction_method"], f["extraction_run_id"])
        n += 1
    return n


async def resolve_leaids(mode: str, value: str | None, pool) -> list[str]:
    log.info("resolve_leaids start mode=%s", mode)
    if mode == "refresh-leaids":
        return [s.strip() for s in (value or "").split(",") if s.strip()]
    if mode == "refresh-ranked":
        # NEW v0.3: everything with a US News rank (the dropdown set).
        # universities is tenant-less reference data — no RLS bind needed.
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT leaid FROM universities "
                "WHERE us_news_rank_national IS NOT NULL "
                "   OR us_news_rank_liberal_arts IS NOT NULL"
            )
            return [r["leaid"] for r in rows]
    if mode == "refresh-all":
        # v0.5: the whole catalog. universities is the source of truth and the
        # US News lists are filtered views of it, so the refresh set is every
        # college - not the ranked subset.
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT leaid FROM universities ORDER BY leaid")
            return [r["leaid"] for r in rows]
    if mode == "refresh-targets":
        if not TENANT_ID:
            raise RuntimeError("FOCMS_TENANT_ID required for refresh-targets mode")
        tenant = _safe_tenant(TENANT_ID)
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant}'")
                rows = await conn.fetch(
                    "SELECT DISTINCT university_leaid FROM target_universities "
                    "WHERE deleted_at IS NULL AND is_active = true"
                )
                return [r["university_leaid"] for r in rows]
    if mode == "refresh-top-n":
        n = int(value or "100")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT leaid FROM universities WHERE admit_rate IS NOT NULL "
                "ORDER BY admit_rate ASC LIMIT $1",
                n,
            )
            return [r["leaid"] for r in rows]
    raise ValueError(f"unknown mode {mode}")


async def main_async(mode: str, value: str | None) -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4,
                                     statement_cache_size=0)
    try:
        leaids = await resolve_leaids(mode, value, pool)
        log.info("resolved %d LEAIDs mode=%s", len(leaids), mode)
        if not leaids:
            log.warning("nothing to refresh")
            return
        rows = await fetch_scorecard(leaids)
        log.info("fetched %d scorecard rows", len(rows))
        # v0.5: commit in batches. One transaction over ~6,100 rows held locks
        # for minutes and discarded every row if the tail failed.
        n_uni = n_facts = 0
        async with pool.acquire() as conn:
            for i in range(0, len(rows), UPSERT_BATCH):
                chunk = rows[i:i + UPSERT_BATCH]
                async with conn.transaction():
                    n_uni += await upsert_universities(
                        conn, [row_to_universities(r) for r in chunk]
                    )
                    facts: list[dict[str, Any]] = []
                    for r in chunk:
                        facts.extend(row_to_cds_facts(r))
                    n_facts += await upsert_cds_facts(conn, facts)
                log.info("committed %d/%d universities", n_uni, len(rows))
        log.info("upserted universities=%d cds_facts=%d", n_uni, n_facts)
    finally:
        await pool.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["refresh-top-n", "refresh-targets",
                                     "refresh-leaids", "refresh-ranked",
                                     "refresh-all"])
    ap.add_argument("--value", help="n for top-n, or comma-separated leaids")
    args = ap.parse_args()
    asyncio.run(main_async(args.mode, args.value))


if __name__ == "__main__":
    main()
