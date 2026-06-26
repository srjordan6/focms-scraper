"""
USA Swimming Data Hub Individual Times Scraper
==============================================

Scrapes a swimmer's full career times from USA Swimming Data Hub and upserts
to Postgres events table (event_type='swim_race', source_system='usa_swimming_data_hub').

v0.4.0 (2026-06-25):
- Fix: USA Swimming changed form HTML; #firstOrPreferredName no longer exists.
  Replace ID-only selectors with multi-strategy locators (id, name, placeholder,
  label, role). Scraper now survives this UI change and future ones.
- Add: sync_log writes at scraper_start, scraper_success, scraper_failure.
  Every run leaves a paper trail in archive_entries regardless of outcome.
- Add: page DOM dump to archive_entries on selector failure. Post-mortem
  forensics without re-running the scraper.
- Add: try/except wrapper around main() with full traceback captured into
  the failure sync_log. Silent failures become visible failures.

v0.3.0 (2026-06-23): RLS session SET fix.
v0.2.0 (2026-06-23): asyncpg date binding fix.
v0.1.0 (2026-06-21): initial Playwright Tier 3 scraper.
"""
import asyncio
import json
import os
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Callable, Optional

from playwright.async_api import async_playwright, Page
import asyncpg

SEARCH_URL = "https://data.usaswimming.org/datahub/usas/individualsearch"
SCRAPER_VERSION = "0.4.0"

STROKE_LONG = {"FR": "Free", "BK": "Back", "BR": "Breast", "FL": "Fly", "IM": "IM"}


# =============================================================================
# Race dataclass and parser (unchanged from v0.3.0)
# =============================================================================

@dataclass
class Race:
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
    person_key: int
    meet_key: str
    usas_swim_time_key: str

    @property
    def title(self) -> str:
        stroke_long = STROKE_LONG[self.stroke_short]
        return f"{self.distance_m} {stroke_long} {self.course} {self.swim_time} ({self.event_date})"

    @property
    def details(self) -> dict:
        d = {
            "age": self.age,
            "lsc": self.lsc,
            "meet": self.meet,
            "team": self.team,
            "course": self.course,
            "points": self.points,
            "stroke": self.stroke_short,
            "swim_time": self.swim_time,
            "distance_m": self.distance_m,
            "time_standard": self.standard,
        }
        if self.is_relay_leg:
            d["relay_leg"] = True
        return d


def _parse_race_row(headers: list, row: list) -> Race:
    """Map a Sisense JAQL row to a Race dataclass."""
    def cell(name):
        idx = headers.index(name)
        return row[idx].get("text", row[idx].get("data"))

    event = cell("Event")
    swim_time = cell("Swim Time")
    parts = event.split()
    distance = int(parts[0])
    stroke_short = parts[1]
    course = parts[2]

    swim_date = cell("Swim Date")
    m, d, y = swim_date.split("/")
    iso_date = f"{y}-{int(m):02d}-{int(d):02d}"
    date_compact = f"{y}{int(m):02d}{int(d):02d}"

    is_relay = swim_time.endswith("r")
    clean_time = swim_time.rstrip("r")
    source_id = f"{date_compact}_{distance}{stroke_short}_{course}_{clean_time}"
    if is_relay:
        source_id += "r"

    return Race(
        source_id=source_id,
        event_date=iso_date,
        distance_m=distance,
        stroke_short=stroke_short,
        course=course,
        swim_time=clean_time,
        is_relay_leg=is_relay,
        age=int(cell("Age")),
        points=int(cell("Points")) if cell("Points") else None,
        standard=cell("Time Standard") or None,
        meet=cell("Meet"),
        lsc=cell("LSC"),
        team=cell("Team"),
        person_key=int(cell("PersonKey")),
        meet_key=str(cell("MeetKey")),
        usas_swim_time_key=str(cell("UsasSwimTimeKey")),
    )


# =============================================================================
# v0.4.0 NEW: Diagnostic helpers (sync_log, dom_dump)
# =============================================================================

async def _diag_conn():
    """Open a short-lived connection for diagnostic writes. Caller closes."""
    dsn = os.environ["DATABASE_URL"]
    tenant_id = os.environ["TENANT_ID"]
    conn = await asyncpg.connect(dsn)
    await conn.execute(f"SET app.current_tenant_id = '{tenant_id}'")
    return conn


async def _write_sync_log(status: str, summary: str, detail: str = ""):
    """Write a sync_log archive_entry. status in {'started','success','failure'}.
    Swallows its own errors to avoid masking the real failure."""
    try:
        conn = await _diag_conn()
        try:
            tenant_id = os.environ["TENANT_ID"]
            created_by = os.environ["CREATED_BY"]
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            await conn.execute("""
                INSERT INTO archive_entries (
                    id, tenant_id, archive_type, archive_date, version,
                    title, summary, detail, source, source_id, visibility, created_by
                ) VALUES (
                    gen_random_uuid_v7(), $1::uuid, 'sync_log', CURRENT_DATE, $2,
                    $3, $4, $5, 'usa_swimming_scraper', $6, 'private', $7::uuid
                )
            """, tenant_id, SCRAPER_VERSION,
                 f"Scraper {status} at {now.isoformat()}",
                 summary, detail,
                 f"scraper_run_{timestamp}_{status}",
                 created_by)
        finally:
            await conn.close()
    except Exception as e:
        print(f"WARN: sync_log write failed: {e}", file=sys.stderr)


async def _dump_dom(page: Page, field_name: str):
    """Dump current page HTML to archive_entries for forensics. Truncates to 50KB."""
    try:
        html = await page.content()
        truncated = html[:50000]
        url = page.url
        title = await page.title()
        conn = await _diag_conn()
        try:
            tenant_id = os.environ["TENANT_ID"]
            created_by = os.environ["CREATED_BY"]
            now = datetime.now(timezone.utc)
            timestamp = now.strftime("%Y%m%d_%H%M%S")
            await conn.execute("""
                INSERT INTO archive_entries (
                    id, tenant_id, archive_type, archive_date, version,
                    title, summary, detail, source, source_id, visibility, created_by
                ) VALUES (
                    gen_random_uuid_v7(), $1::uuid, 'dom_dump', CURRENT_DATE, $2,
                    $3, $4, $5, 'usa_swimming_scraper', $6, 'private', $7::uuid
                )
            """, tenant_id, SCRAPER_VERSION,
                 f"DOM dump - {field_name} selector failure",
                 f"Page URL: {url}\nPage title: {title}\nHTML truncated to 50KB.",
                 truncated,
                 f"dom_dump_{timestamp}_{field_name}",
                 created_by)
        finally:
            await conn.close()
    except Exception as e:
        print(f"WARN: dom_dump failed: {e}", file=sys.stderr)


# =============================================================================
# v0.4.0 NEW: Multi-strategy locators
# =============================================================================

async def _smart_fill(page: Page, value: str, field_label: str, strategies: list):
    """Try each locator strategy; first match wins. Dumps DOM and raises on total failure."""
    last_error = None
    for i, strategy in enumerate(strategies):
        try:
            locator = strategy(page)
            count = await locator.count()
            if count > 0:
                await locator.first.fill(value, timeout=5000)
                print(f"[{field_label}] filled via strategy #{i+1}")
                return
        except Exception as e:
            last_error = e
            continue
    await _dump_dom(page, field_label)
    raise RuntimeError(f"All {len(strategies)} selector strategies failed for {field_label}. Last error: {last_error}")


async def _smart_click(page: Page, button_label: str, strategies: list):
    """Try each locator strategy; first match wins."""
    last_error = None
    for i, strategy in enumerate(strategies):
        try:
            locator = strategy(page)
            count = await locator.count()
            if count > 0:
                await locator.first.click(timeout=5000)
                print(f"[{button_label}] clicked via strategy #{i+1}")
                return
        except Exception as e:
            last_error = e
            continue
    await _dump_dom(page, button_label)
    raise RuntimeError(f"All {len(strategies)} selector strategies failed for {button_label}. Last error: {last_error}")


# Selector strategies for the USA Swimming search form.
# Tried in order; first that finds an element wins. Defensive against UI changes.
FIRST_NAME_STRATEGIES = [
    lambda p: p.locator("#firstOrPreferredName"),
    lambda p: p.locator("input[name*='irst' i]"),
    lambda p: p.locator("input[id*='irst' i]"),
    lambda p: p.locator("input[placeholder*='first' i]"),
    lambda p: p.get_by_label(re.compile(r"first.*name|first.*or.*preferred", re.I)),
    lambda p: p.get_by_role("textbox", name=re.compile(r"first", re.I)),
]

LAST_NAME_STRATEGIES = [
    lambda p: p.locator("#lastName"),
    lambda p: p.locator("input[name*='ast' i]"),
    lambda p: p.locator("input[id*='ast' i]"),
    lambda p: p.locator("input[placeholder*='last' i]"),
    lambda p: p.get_by_label(re.compile(r"last.*name", re.I)),
    lambda p: p.get_by_role("textbox", name=re.compile(r"last", re.I)),
]

SUBMIT_STRATEGIES = [
    lambda p: p.locator('button[type="submit"]'),
    lambda p: p.get_by_role("button", name=re.compile(r"search|submit|find", re.I)),
    lambda p: p.locator("button:has-text('Search')"),
    lambda p: p.locator("input[type='submit']"),
]


# =============================================================================
# Search and fetch (now using smart selectors)
# =============================================================================

async def find_person_key(first_name: str, last_name: str, club: str, lsc: str) -> int:
    """Search by name, return PersonKey for the club+LSC match."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()

        person_jaql = []
        async def on_response(resp):
            if "Public%20Person%20Search/jaql" in resp.url and resp.status == 200:
                person_jaql.append(await resp.text())
        page.on("response", on_response)

        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(10)
        await _smart_fill(page, first_name, "firstName", FIRST_NAME_STRATEGIES)
        await _smart_fill(page, last_name, "lastName", LAST_NAME_STRATEGIES)
        await _smart_click(page, "submitSearch", SUBMIT_STRATEGIES)
        await asyncio.sleep(8)
        await browser.close()

    if not person_jaql:
        raise RuntimeError("No Public Person Search JAQL response captured")

    body = json.loads(person_jaql[0])
    H = body["headers"]
    for row in body["values"]:
        clb = row[H.index("Club")]["data"]
        lsc_val = row[H.index("LSC")]["data"]
        pk = row[H.index("PersonKey")]["data"]
        if club.lower() in clb.lower() and lsc_val.upper() == lsc.upper():
            return int(pk)
    raise RuntimeError(f"No match for {first_name} {last_name} at {club}/{lsc}")


async def fetch_all_times(first_name: str, last_name: str, club: str, lsc: str) -> list[Race]:
    """Drive the full search → click flow, capture full times response."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(viewport={"width": 1400, "height": 1400})
        page = await ctx.new_page()

        person_responses = []
        times_responses = []
        async def on_response(resp):
            try:
                if "Public%20Person%20Search/jaql" in resp.url and resp.status == 200:
                    person_responses.append(await resp.text())
                elif "USA%20Swimming%20Times%20Elasticube/jaql" in resp.url and resp.status == 200:
                    times_responses.append(await resp.text())
            except Exception:
                pass
        page.on("response", on_response)

        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(10)
        await _smart_fill(page, first_name, "firstName", FIRST_NAME_STRATEGIES)
        await _smart_fill(page, last_name, "lastName", LAST_NAME_STRATEGIES)
        await _smart_click(page, "submitSearch", SUBMIT_STRATEGIES)
        await asyncio.sleep(8)

        if not person_responses:
            await _dump_dom(page, "personSearchJAQL")
            await browser.close()
            raise RuntimeError("Person search JAQL not captured")
        people = json.loads(person_responses[0])
        H = people["headers"]
        target_idx = None
        for i, row in enumerate(people["values"]):
            clb = row[H.index("Club")]["data"]
            lsc_val = row[H.index("LSC")]["data"]
            if club.lower() in clb.lower() and lsc_val.upper() == lsc.upper():
                target_idx = i
                break
        if target_idx is None:
            await browser.close()
            raise RuntimeError(f"No match for {first_name} {last_name} at {club}/{lsc}")

        # See Results / View Results / Times - flexible button text match
        await page.evaluate(f"""() => {{
            const buttons = Array.from(document.querySelectorAll('button'))
                .filter(b => /see.*results|view.*results|view.*times|times/i.test(b.textContent || ''));
            if (buttons.length > {target_idx}) buttons[{target_idx}].click();
        }}""")
        await asyncio.sleep(25)
        await browser.close()

    if not times_responses:
        raise RuntimeError("Times Elasticube JAQL not captured - swimmer found but times never loaded")

    best = None
    for body_str in times_responses:
        try:
            body = json.loads(body_str)
            hs = body.get("headers", [])
            if "Event" in hs and "Swim Time" in hs and "Swim Date" in hs:
                if best is None or len(body.get("values", [])) > len(best.get("values", [])):
                    best = body
        except Exception:
            continue
    if not best:
        raise RuntimeError("No times JAQL response with race columns")

    return [_parse_race_row(best["headers"], r) for r in best["values"]]


async def upsert_to_postgres(races: list[Race], tenant_id: str, student_id: str, created_by: str, dsn: str):
    """Insert races whose source_id isn't already in events. Returns (inserted, skipped, total)."""
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f"SET app.current_tenant_id = '{tenant_id}'")

        existing = await conn.fetch("""
            SELECT source_id FROM events
            WHERE event_type = 'swim_race'
              AND source_system = 'usa_swimming_data_hub'
              AND student_id = $1
              AND deleted_at IS NULL
        """, student_id)
        existing_ids = {r["source_id"] for r in existing}

        to_insert = [r for r in races if r.source_id not in existing_ids]
        if not to_insert:
            return 0, len(races), len(races)

        async with conn.transaction():
            await conn.executemany("""
                INSERT INTO events (
                    tenant_id, student_id, event_type, title, event_date, location_name,
                    details, visibility, source_system, source_id, created_by
                ) VALUES ($1, $2, 'swim_race', $3, $4::date, $5, $6::jsonb, 'private',
                          'usa_swimming_data_hub', $7, $8)
            """, [
                (tenant_id, student_id, r.title, date.fromisoformat(r.event_date), r.meet,
                 json.dumps(r.details), r.source_id, created_by)
                for r in to_insert
            ])
        return len(to_insert), len(races) - len(to_insert), len(races)
    finally:
        await conn.close()


# =============================================================================
# Entry point with sync_log instrumentation
# =============================================================================

async def main():
    """CLI / Render Cron entrypoint."""
    first = os.environ.get("SWIMMER_FIRST_NAME", "John")
    last = os.environ.get("SWIMMER_LAST_NAME", "Jordan")
    club = os.environ.get("SWIMMER_CLUB", "Iron Horse Aquatics")
    lsc = os.environ.get("SWIMMER_LSC", "NT")
    tenant_id = os.environ["TENANT_ID"]
    student_id = os.environ["STUDENT_ID"]
    created_by = os.environ["CREATED_BY"]
    dsn = os.environ["DATABASE_URL"]

    started_at = datetime.now(timezone.utc).isoformat()
    print(f"[{started_at}] scraping {first} {last} ({club}/{lsc})...")

    await _write_sync_log(
        "started",
        f"Scraper v{SCRAPER_VERSION} starting for {first} {last} ({club}/{lsc})",
        f"started_at={started_at}\nfirst={first}\nlast={last}\nclub={club}\nlsc={lsc}"
    )

    try:
        races = await fetch_all_times(first, last, club, lsc)
        print(f"fetched {len(races)} races from Data Hub")
        ins, skp, tot = await upsert_to_postgres(races, tenant_id, student_id, created_by, dsn)
        print(f"inserted={ins} skipped(existing)={skp} total={tot}")
        latest_race = max(r.event_date for r in races) if races else "none"
        print(f"latest race: {latest_race}")

        await _write_sync_log(
            "success",
            f"Scraper completed: {ins} new races inserted, {skp} skipped, {tot} fetched",
            f"started_at={started_at}\ncompleted_at={datetime.now(timezone.utc).isoformat()}\n"
            f"inserted={ins}\nskipped={skp}\ntotal={tot}\nlatest_race={latest_race}"
        )
    except Exception as e:
        tb = traceback.format_exc()
        print(f"FAILED: {e}\n{tb}", file=sys.stderr)
        await _write_sync_log(
            "failure",
            f"Scraper failed: {type(e).__name__}: {str(e)[:300]}",
            f"started_at={started_at}\nfailed_at={datetime.now(timezone.utc).isoformat()}\n"
            f"error_type={type(e).__name__}\nerror_message={str(e)}\n\nTraceback:\n{tb}"
        )
        raise  # Re-raise so Render Cron marks run as failed


if __name__ == "__main__":
    asyncio.run(main())
