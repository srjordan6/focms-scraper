"""
USA Swimming Data Hub Individual Times Scraper
==============================================

Scrapes a swimmer's full career times from USA Swimming Data Hub and upserts
to Postgres events table (event_type='swim_race', source_system='usa_swimming_data_hub').

Run modes:
  - LIST search (initial discovery): given first_name + last_name + club, find PersonKey
  - TIMES fetch (ongoing): given PersonKey, fetch all 182+ races

Render Worker deployment: schedule via Render Cron Job, daily at 04:30 UTC.
Idempotent by source_id (YYYYMMDD_DDSTROKE_COURSE_TIME[r]).

Architecture v2.1 §11 Tier 3 pattern:
  1. Connector  → this script (Playwright headless chromium)
  2. Staging    → no separate table; we parse directly into events
  3. Mapper     → parse_race() below
  4. Sync log   → INSERT into archive_entries with archive_type='sync_log'

Reverse-engineered from Sisense network calls. URLs / IDs subject to change
if USA Swimming reskins Data Hub. Test in staging before any prod cron run
on a non-trivial cadence.
"""
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Optional

from playwright.async_api import async_playwright
import asyncpg

SEARCH_URL = "https://data.usaswimming.org/datahub/usas/individualsearch"

STROKE_LONG = {"FR": "Free", "BK": "Back", "BR": "Breast", "FL": "Fly", "IM": "IM"}


@dataclass
class Race:
    source_id: str
    event_date: str       # YYYY-MM-DD
    distance_m: int
    stroke_short: str     # FR | BK | BR | FL | IM
    course: str           # SCY | SCM | LCM
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

    event = cell("Event")                     # "50 FR SCY"
    swim_time = cell("Swim Time")             # "30.15" or "30.15r"
    parts = event.split()
    distance = int(parts[0])
    stroke_short = parts[1]
    course = parts[2]

    swim_date = cell("Swim Date")             # "10/12/2025"
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


async def find_person_key(first_name: str, last_name: str, club: str, lsc: str) -> int:
    """Search by name, return PersonKey for the match where club+LSC match."""
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
        await page.fill("#firstOrPreferredName", first_name)
        await page.fill("#lastName", last_name)
        await page.click('button[type="submit"]')
        await asyncio.sleep(8)
        await browser.close()

    if not person_jaql:
        raise RuntimeError("No Public Person Search JAQL response captured")

    body = json.loads(person_jaql[0])
    H = body["headers"]
    for row in body["values"]:
        name = row[H.index("Name")]["data"]
        clb = row[H.index("Club")]["data"]
        lsc_val = row[H.index("LSC")]["data"]
        pk = row[H.index("PersonKey")]["data"]
        if club.lower() in clb.lower() and lsc_val.upper() == lsc.upper():
            return int(pk)
    raise RuntimeError(f"No match for {first_name} {last_name} at {club}/{lsc}")


async def fetch_all_times(first_name: str, last_name: str, club: str, lsc: str) -> list[Race]:
    """Drive the full search → click flow, capture the 182-row times response."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(viewport={"width": 1400, "height": 1400})
        page = await ctx.new_page()

        # Discover which "See Results" row matches the target swimmer.
        # First need to know the alphabetical index by club in the search results.
        person_responses = []
        times_responses = []
        async def on_response(resp):
            try:
                if "Public%20Person%20Search/jaql" in resp.url and resp.status == 200:
                    person_responses.append(await resp.text())
                elif "USA%20Swimming%20Times%20Elasticube/jaql" in resp.url and resp.status == 200:
                    body = await resp.text()
                    times_responses.append(body)
            except Exception:
                pass
        page.on("response", on_response)

        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(10)
        await page.fill("#firstOrPreferredName", first_name)
        await page.fill("#lastName", last_name)
        await page.click('button[type="submit"]')
        await asyncio.sleep(8)

        # Identify target row index by sorted (asc) club name
        if not person_responses:
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

        await page.evaluate(f"""() => {{
            const buttons = Array.from(document.querySelectorAll('button'))
                .filter(b => (b.textContent||'').trim() === 'See Results');
            if (buttons.length > {target_idx}) buttons[{target_idx}].click();
        }}""")
        await asyncio.sleep(25)  # times load
        await browser.close()

    # Find the response with the full headers + most rows
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
    """Insert any races whose source_id isn't already in events. Returns (inserted, skipped, total)."""
    conn = await asyncpg.connect(dsn)
    try:
        # Set RLS tenant context (per playbook §5.10)
        await conn.execute(f"SET LOCAL app.current_tenant_id = '{tenant_id}'")

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
                (tenant_id, student_id, r.title, r.event_date, r.meet,
                 json.dumps(r.details), r.source_id, created_by)
                for r in to_insert
            ])
        return len(to_insert), len(races) - len(to_insert), len(races)
    finally:
        await conn.close()


async def main():
    """CLI / Render Worker entrypoint."""
    first = os.environ.get("SWIMMER_FIRST_NAME", "John")
    last = os.environ.get("SWIMMER_LAST_NAME", "Jordan")
    club = os.environ.get("SWIMMER_CLUB", "Iron Horse Aquatics")
    lsc = os.environ.get("SWIMMER_LSC", "NT")
    tenant_id = os.environ["TENANT_ID"]
    student_id = os.environ["STUDENT_ID"]
    created_by = os.environ["CREATED_BY"]
    dsn = os.environ["DATABASE_URL"]

    started_at = datetime.utcnow().isoformat()
    print(f"[{started_at}] scraping {first} {last} ({club}/{lsc})...")
    races = await fetch_all_times(first, last, club, lsc)
    print(f"fetched {len(races)} races from Data Hub")
    ins, skp, tot = await upsert_to_postgres(races, tenant_id, student_id, created_by, dsn)
    print(f"inserted={ins} skipped(existing)={skp} total={tot}")
    print(f"latest race: {max(r.event_date for r in races)}")


if __name__ == "__main__":
    asyncio.run(main())
