"""
USA Swimming Data Hub Scraper - Multi-Tenant
============================================

Scrapes USA Swimming Data Hub for all enrolled swimmers across all tenants
and upserts to Postgres events + personal_records tables.

v0.6.0 (2026-06-26): DISCOVERY MODE + MULTI-TENANT
- BREAKING ARCHITECTURE CHANGE. No longer reads SWIMMER_* env vars.
  Reads student_external_identifiers WHERE system_name='usa_swimming'
  AND is_primary=true AND deleted_at IS NULL.
- Direct URL navigation. No form scraping. Uses stable external_id
  to construct URL: {external_url}/best-times.
- Per-swimmer try/except. One swimmer failure does NOT kill the batch.
- Per-swimmer sync_log entries with student_id reference.
- Updates last_synced_at, last_sync_status, last_sync_summary on each
  student_external_identifiers row.
- DISCOVERY MODE: captures all JSON responses to response_log archive
  but does NOT yet parse races. Once first run lands, inspect
  response_log to identify new API shape, then v0.7.0 adds parser.

v0.5.0 (2026-06-26): Shape-based response detection + response_log.
v0.4.0 (2026-06-25): Multi-strategy selectors + sync_log + DOM dump.
v0.3.0 (2026-06-23): RLS session SET fix.
v0.2.0 (2026-06-23): asyncpg date binding fix.
v0.1.0 (2026-06-21): initial Playwright Tier 3 scraper.
"""
import asyncio
import json
import os
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

from playwright.async_api import async_playwright, Page
import asyncpg

SCRAPER_VERSION = "0.6.0"
STROKE_LONG = {"FR": "Free", "BK": "Back", "BR": "Breast", "FL": "Fly", "IM": "IM"}


@dataclass
class Race:
    """Race record - kept for v0.7.0 parser. Not used in discovery mode."""
    source_id: str
    event_date: str
    distance_m: int
    stroke_short: str
    course: str
    swim_time: str
    is_relay_leg: bool
    age: int
    points: Optional[int]
    standard: Optional[str]
    meet: str
    lsc: str
    team: str

    @property
    def title(self) -> str:
        stroke_long = STROKE_LONG[self.stroke_short]
        return f"{self.distance_m} {stroke_long} {self.course} {self.swim_time} ({self.event_date})"


# =============================================================================
# Diagnostic helpers
# =============================================================================

async def _diag_conn(tenant_id: str):
    """Open a short-lived connection with tenant context set."""
    dsn = os.environ["DATABASE_URL"]
    conn = await asyncpg.connect(dsn)
    await conn.execute(f"SET app.current_tenant_id = '{tenant_id}'")
    return conn


async def _write_sync_log(tenant_id: str, status: str, summary: str,
                           detail: str = "", student_id: Optional[str] = None,
                           external_id: Optional[str] = None):
    """Write a sync_log archive_entry. Swallows its own errors."""
    try:
        conn = await _diag_conn(tenant_id)
        try:
            created_by = os.environ["CREATED_BY"]
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            student_part = f"_{student_id[:8]}" if student_id else ""
            await conn.execute("""
                INSERT INTO archive_entries (
                    id, tenant_id, archive_type, archive_date, version,
                    title, summary, detail, source, source_id, visibility, created_by
                ) VALUES (
                    gen_random_uuid_v7(), $1::uuid, 'sync_log', CURRENT_DATE, $2,
                    $3, $4, $5, 'usa_swimming_scraper', $6, 'private', $7::uuid
                )
            """, tenant_id, SCRAPER_VERSION,
                 f"Scraper {status} at {now.isoformat()} ({external_id or 'batch'})",
                 summary, detail,
                 f"scraper_run_{timestamp}_{status}{student_part}",
                 created_by)
        finally:
            await conn.close()
    except Exception as e:
        print(f"WARN: sync_log write failed: {e}", file=sys.stderr)


async def _dump_responses_log(tenant_id: str, captured: list, label: str,
                               student_id: Optional[str] = None):
    """Dump summary of captured JSON responses for forensics."""
    try:
        summary_lines = []
        for i, r in enumerate(captured):
            preview = r["body"][:300].replace("\n", " ")
            summary_lines.append(f"#{i+1} [{r['method']}] {r['url']} - {r['body_length']} bytes")
            summary_lines.append(f"   preview: {preview!r}")
        detail_text = "\n".join(summary_lines) if summary_lines else "(no JSON responses captured)"

        conn = await _diag_conn(tenant_id)
        try:
            created_by = os.environ["CREATED_BY"]
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            student_part = f"_{student_id[:8]}" if student_id else ""
            await conn.execute("""
                INSERT INTO archive_entries (
                    id, tenant_id, archive_type, archive_date, version,
                    title, summary, detail, source, source_id, visibility, created_by
                ) VALUES (
                    gen_random_uuid_v7(), $1::uuid, 'response_log', CURRENT_DATE, $2,
                    $3, $4, $5, 'usa_swimming_scraper', $6, 'private', $7::uuid
                )
            """, tenant_id, SCRAPER_VERSION,
                 f"Response log - {label}",
                 f"{len(captured)} JSON responses captured for {label}",
                 detail_text[:50000],
                 f"response_log_{timestamp}_{label}{student_part}",
                 created_by)
        finally:
            await conn.close()
    except Exception as e:
        print(f"WARN: response_log failed: {e}", file=sys.stderr)


async def _dump_dom(tenant_id: str, page: Page, label: str,
                     student_id: Optional[str] = None):
    """Dump page HTML to archive_entries. Truncates to 50KB."""
    try:
        html = await page.content()
        truncated = html[:50000]
        url = page.url
        title = await page.title()
        conn = await _diag_conn(tenant_id)
        try:
            created_by = os.environ["CREATED_BY"]
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            student_part = f"_{student_id[:8]}" if student_id else ""
            await conn.execute("""
                INSERT INTO archive_entries (
                    id, tenant_id, archive_type, archive_date, version,
                    title, summary, detail, source, source_id, visibility, created_by
                ) VALUES (
                    gen_random_uuid_v7(), $1::uuid, 'dom_dump', CURRENT_DATE, $2,
                    $3, $4, $5, 'usa_swimming_scraper', $6, 'private', $7::uuid
                )
            """, tenant_id, SCRAPER_VERSION,
                 f"DOM dump - {label}",
                 f"Page URL: {url}\nPage title: {title}\nHTML truncated to 50KB.",
                 truncated,
                 f"dom_dump_{timestamp}_{label}{student_part}",
                 created_by)
        finally:
            await conn.close()
    except Exception as e:
        print(f"WARN: dom_dump failed: {e}", file=sys.stderr)


# =============================================================================
# v0.6.0 NEW: Enrollment queries
# =============================================================================

async def get_enrolled_swimmers(tenant_id: str) -> list[dict]:
    """Return all primary, non-deleted USA Swimming enrollments for this tenant."""
    conn = await _diag_conn(tenant_id)
    try:
        rows = await conn.fetch("""
            SELECT id, student_id, external_id, external_url, details, notes
            FROM student_external_identifiers
            WHERE system_name = 'usa_swimming'
              AND is_primary = true
              AND deleted_at IS NULL
            ORDER BY created_at
        """)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


async def update_sync_status(tenant_id: str, identifier_id: str,
                              status: str, summary: str):
    """Update last_synced_at/status/summary on a student_external_identifiers row."""
    try:
        conn = await _diag_conn(tenant_id)
        try:
            updated_by = os.environ["CREATED_BY"]
            await conn.execute("""
                UPDATE student_external_identifiers
                SET last_synced_at = now(),
                    last_sync_status = $1,
                    last_sync_summary = $2,
                    updated_at = now(),
                    updated_by = $3::uuid
                WHERE id = $4::uuid
                  AND tenant_id = $5::uuid
            """, status, summary, updated_by, identifier_id, tenant_id)
        finally:
            await conn.close()
    except Exception as e:
        print(f"WARN: update_sync_status failed: {e}", file=sys.stderr)


# =============================================================================
# v0.6.0 NEW: Per-swimmer discovery scrape
# =============================================================================

async def scrape_one_swimmer(tenant_id: str, enrollment: dict) -> dict:
    """Discovery-mode scrape: navigate to swimmer's profile, capture all JSON responses.
    
    Returns dict with 'status' (success/failure), 'summary', 'response_count'."""
    student_id = str(enrollment["student_id"])
    external_id = enrollment["external_id"]
    external_url = enrollment["external_url"]

    # Try /best-times first since that's the only confirmed sub-route
    target_url = f"{external_url}/best-times"
    print(f"[swimmer {external_id}] navigating to {target_url}")

    captured = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context(viewport={"width": 1400, "height": 1400})
            page = await ctx.new_page()

            async def on_response(resp):
                try:
                    ct = resp.headers.get("content-type", "").lower()
                    if "json" in ct and resp.status == 200:
                        body = await resp.text()
                        captured.append({
                            "url": resp.url, "status": resp.status,
                            "method": resp.request.method, "body": body,
                            "body_length": len(body),
                        })
                except Exception:
                    pass
            page.on("response", on_response)

            await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(20)  # generous hydration window for SPA + data fetch

            # Capture page state for forensics regardless of success
            await _dump_dom(tenant_id, page, f"best_times_{external_id}",
                            student_id=student_id)
        finally:
            await browser.close()

    # Always dump captured responses in discovery mode
    await _dump_responses_log(tenant_id, captured,
                               f"best_times_{external_id}",
                               student_id=student_id)

    return {
        "status": "success" if captured else "failure",
        "summary": f"Captured {len(captured)} JSON responses from {target_url}",
        "response_count": len(captured),
        "external_id": external_id,
        "student_id": student_id,
    }


# =============================================================================
# Entry point: multi-tenant, multi-swimmer iteration
# =============================================================================

async def main():
    """Iterate tenants and enrolled swimmers."""
    tenant_ids_env = os.environ.get("TENANT_IDS", os.environ.get("TENANT_ID", ""))
    if not tenant_ids_env:
        raise RuntimeError("Either TENANT_IDS or TENANT_ID env var required")
    tenant_ids = [t.strip() for t in tenant_ids_env.split(",") if t.strip()]

    started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{started_at}] scraper v{SCRAPER_VERSION} starting for {len(tenant_ids)} tenant(s)")

    total_swimmers = 0
    total_success = 0
    total_failure = 0
    per_swimmer_results = []

    for tenant_id in tenant_ids:
        await _write_sync_log(
            tenant_id, "started",
            f"Scraper v{SCRAPER_VERSION} starting for tenant {tenant_id}",
            f"started_at={started_at}\ntenant_id={tenant_id}"
        )

        try:
            enrollments = await get_enrolled_swimmers(tenant_id)
            print(f"[tenant {tenant_id}] found {len(enrollments)} enrolled swimmers")
            total_swimmers += len(enrollments)

            for enrollment in enrollments:
                identifier_id = str(enrollment["id"])
                external_id = enrollment["external_id"]
                try:
                    result = await scrape_one_swimmer(tenant_id, enrollment)
                    per_swimmer_results.append(result)
                    if result["status"] == "success":
                        total_success += 1
                        await update_sync_status(tenant_id, identifier_id,
                                                  "success", result["summary"])
                        await _write_sync_log(
                            tenant_id, "swimmer_success",
                            f"Discovery scrape complete for {external_id}",
                            f"external_id={external_id}\n{result['summary']}",
                            student_id=str(enrollment["student_id"]),
                            external_id=external_id,
                        )
                    else:
                        total_failure += 1
                        await update_sync_status(tenant_id, identifier_id,
                                                  "failure", result["summary"])
                        await _write_sync_log(
                            tenant_id, "swimmer_failure",
                            f"Discovery scrape captured 0 responses for {external_id}",
                            result["summary"],
                            student_id=str(enrollment["student_id"]),
                            external_id=external_id,
                        )
                except Exception as e:
                    total_failure += 1
                    tb = traceback.format_exc()
                    print(f"FAILED swimmer {external_id}: {e}\n{tb}", file=sys.stderr)
                    await update_sync_status(
                        tenant_id, identifier_id, "failure",
                        f"{type(e).__name__}: {str(e)[:300]}"
                    )
                    await _write_sync_log(
                        tenant_id, "swimmer_failure",
                        f"Scraper failed for {external_id}: {type(e).__name__}",
                        f"external_id={external_id}\nerror={str(e)}\n\nTraceback:\n{tb}",
                        student_id=str(enrollment["student_id"]),
                        external_id=external_id,
                    )

            await _write_sync_log(
                tenant_id, "tenant_complete",
                f"Tenant {tenant_id} processed {len(enrollments)} swimmers",
                f"started_at={started_at}\ncompleted_at={datetime.now(timezone.utc).isoformat()}\n"
                f"total={len(enrollments)}\nsuccess={sum(1 for r in per_swimmer_results if r['status']=='success')}"
            )

        except Exception as e:
            tb = traceback.format_exc()
            print(f"FAILED tenant {tenant_id}: {e}\n{tb}", file=sys.stderr)
            await _write_sync_log(
                tenant_id, "tenant_failure",
                f"Tenant {tenant_id} scraper failed: {type(e).__name__}",
                f"error={str(e)}\n\nTraceback:\n{tb}"
            )

    print(f"\n=== BATCH COMPLETE ===")
    print(f"Tenants processed: {len(tenant_ids)}")
    print(f"Total swimmers: {total_swimmers}")
    print(f"Success: {total_success}")
    print(f"Failure: {total_failure}")


if __name__ == "__main__":
    asyncio.run(main())
