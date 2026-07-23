# NorthStar-Scraper container - the single nightly cron for all NorthStar jobs.
#
# Base image: official Playwright Python image (Microsoft-maintained) with
# chromium + all OS deps pre-installed. Pinned for reproducibility.
#
# Entry point: focms_nightly_jobs.py runs every job in an isolated subprocess:
#   birthday-billing        daily    age-band membership re-billing
#   nces-scorecard-refresh  monthly  (1st) target-university data refresh
#
# Render Cron Jobs build this from the repo root on every Manual Build.
# (Cron Jobs do NOT auto-deploy on push - operational trap per playbook §5.X.)

FROM mcr.microsoft.com/playwright/python:v1.50.0-noble

# Working directory inside the container.
WORKDIR /app

# Python deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Job sources.
COPY *.py .

# Unbuffered stdout so Render captures logs in real time.
ENV PYTHONUNBUFFERED=1

# Single entry point; jobs read their config from env vars
# (DATABASE_URL, STRIPE_SECRET_KEY, GMAIL_SMTP_*, SCORECARD_API_KEY).
CMD ["python", "focms_nightly_jobs.py"]
