# focms-scraper

USA Swimming Data Hub scraper for FOCMS. Headless chromium via Playwright,
upserts to `events` table via asyncpg with RLS tenant context.

## Quick facts

- **Schedule:** daily 04:30 UTC (~22:30 CST / 23:30 CDT)
- **Service type:** Render Cron Job (Docker)
- **Idempotency:** dedup by `source_id` = `YYYYMMDD_DDSTROKE_COURSE_TIME[r]`
- **First production run:** 2026-06-21 (backfilled 25 races to reach 182-row parity)

## Why a separate repo

This is the canonical pattern for FOCMS connectors. Each Tier-3 data source
(USA Swimming, future MaxPreps, future SwimRankings) gets its own repo and
its own Cron Job. The `focms-api` repo stays clean.

## Files

| File                       | Purpose                                          |
|----------------------------|--------------------------------------------------|
| `usa_swimming_scraper.py`  | Connector + mapper + upsert; 294 lines           |
| `Dockerfile`               | Playwright base image + pip install              |
| `requirements.txt`         | Pinned: playwright 1.50.0, asyncpg 0.30.0        |
| `render.yaml`              | Blueprint declaring the Cron Job                 |

## Env vars (set in Render dashboard, never in repo)

| Var                  | Value                                                |
|----------------------|------------------------------------------------------|
| `DATABASE_URL`       | focms-prod-db internal URL (or PgBouncer URL)        |
| `TENANT_ID`          | `019ed384-56fc-7516-bfbf-efaa5231e281` (JRJ tenant)  |
| `STUDENT_ID`         | `019ed384-5769-72ca-864a-28e40c4e5d30` (John)        |
| `CREATED_BY`         | `019ed384-56d8-77fb-bfe6-00b1d064da18` (Stephen)     |
| `SWIMMER_FIRST_NAME` | `John`                                               |
| `SWIMMER_LAST_NAME`  | `Jordan`                                             |
| `SWIMMER_CLUB`       | `Iron Horse Aquatics`                                |
| `SWIMMER_LSC`        | `NT`                                                 |

## Deploy

See `scraper_deploy_runbook.md` in archive for full procedure. Short version:
upload all four files to `github.com/srjordan6/focms-scraper/upload/main`,
create a Render Cron Job pointing at the repo, set the env vars above,
hit Manual Build.

## What to expect on a clean run

```
[2026-06-22T04:30:00Z] scraping John Jordan (Iron Horse Aquatics/NT)...
fetched 182 races from Data Hub
inserted=0 skipped(existing)=182 total=182
latest race: 2026-06-07
```

On a day after a new meet:

```
fetched 187 races from Data Hub
inserted=5 skipped(existing)=182 total=187
latest race: 2026-06-21
```

## Operational notes

- Sisense JAQL endpoint shape is reverse-engineered. If USA Swimming reskins
  Data Hub, this breaks. Run logs will show `RuntimeError: No times JAQL
  response with race columns` — that's the canary.
- Render Cron Jobs do NOT auto-deploy on push (autoDeploy is ignored).
  Manual Build required after every code push.
- Chromium adds ~250 MB to the image. Starter plan has 512 MB which is
  enough for the single page session.
- Playwright timeout is 60s for page goto, then 8–25s sleeps for React
  hydration + JAQL response capture. Single run should fit well within
  Render's 60-minute Cron Job ceiling.
