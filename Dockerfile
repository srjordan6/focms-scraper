# USA Swimming scraper container.
#
# Base image: official Playwright Python image (Microsoft-maintained) with
# chromium + all OS deps pre-installed. Pinned for reproducibility.
#
# Render Cron Jobs build this from the repo root on every Manual Build.
# (Cron Jobs do NOT auto-deploy on push — operational trap per playbook §5.X.)

FROM mcr.microsoft.com/playwright/python:v1.50.0-noble

# Working directory inside the container.
WORKDIR /app

# Python deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Scraper source.
COPY usa_swimming_scraper.py .

# Unbuffered stdout so Render captures logs in real time.
ENV PYTHONUNBUFFERED=1

# Render Cron Job command. The scraper reads its config from env vars
# (TENANT_ID, STUDENT_ID, CREATED_BY, DATABASE_URL, SWIMMER_*) and exits.
CMD ["python", "usa_swimming_scraper.py"]
