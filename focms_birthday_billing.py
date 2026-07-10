#!/usr/bin/env python3
"""
focms_birthday_billing.py v0.1.0 (2026-07-09)

Daily Render Cron. Age-band membership re-billing anchored to each student's
birthday (pricing lives in pricing_tiers; bands: 0-10 free, 11-13, 14-16,
17-18, 19+).

Sweep A (T-7): students whose birthday is exactly 7 days out AND whose new age
  crosses into a different PAID band -> off-session Stripe charge on the card
  on file for the new band's annual price.
  Success -> tenant_settings.feature_flags.membership = {key, paid_until, pi}
  Failure -> feature_flags.membership.pending_hold = birthday; warning email.

Sweep B (birthday): pending_hold reached without payment ->
  feature_flags.billing_hold = true (public site stops rendering, edits
  blocked by the API) + final email. Hold clears automatically when the
  retried charge succeeds (stripe webhook payment_intent.succeeded) or via
  support.

Env: DATABASE_URL, STRIPE_SECRET_KEY, optional GMAIL_SMTP_USER/GMAIL_SMTP_PASS
     or RESEND_API_KEY, EMAIL_FROM.
Render Cron schedule: 0 9 * * *  (daily 09:00 UTC). Manual Build after every
code change - cron jobs do not auto-deploy.
"""

import asyncio
import json
import logging
import os
import smtplib
from datetime import date, datetime, timedelta, timezone
from email.mime.text import MIMEText

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("birthday-billing")

BAND_KEYS = [
    (10, "membership_age_0_10"),
    (13, "membership_age_11_13"),
    (16, "membership_age_14_16"),
    (18, "membership_age_17_18"),
    (999, "membership_age_19_plus"),
]


def band_for_age(age: int) -> str:
    for cap, key in BAND_KEYS:
        if age <= cap:
            return key
    return BAND_KEYS[-1][1]


def age_on(birth: date, on: date) -> int:
    return on.year - birth.year - ((on.month, on.day) < (birth.month, birth.day))


def next_birthday(birth: date, today: date) -> date:
    """Next occurrence of the birthday strictly after today-1 (Feb 29 -> Mar 1)."""
    for yr in (today.year, today.year + 1):
        try:
            cand = birth.replace(year=yr)
        except ValueError:  # Feb 29
            cand = date(yr, 3, 1)
        if cand >= today:
            return cand
    return birth.replace(year=today.year + 1)


def send_email(to_email: str, subject: str, html: str) -> None:
    g_user, g_pass = os.environ.get("GMAIL_SMTP_USER"), os.environ.get("GMAIL_SMTP_PASS")
    try:
        if g_user and g_pass:
            msg = MIMEText(html, "html", "utf-8")
            msg["Subject"] = subject
            msg["From"] = os.environ.get("EMAIL_FROM", f"outcomestar <{g_user}>")
            msg["To"] = to_email
            with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as srv:
                srv.starttls()
                srv.login(g_user, g_pass)
                srv.sendmail(g_user, [to_email], msg.as_string())
            return
        key = os.environ.get("RESEND_API_KEY")
        if key:
            httpx.post("https://api.resend.com/emails",
                       headers={"Authorization": f"Bearer {key}"},
                       json={"from": os.environ.get("EMAIL_FROM", "outcomestar <support@outcomestar.app>"),
                             "to": [to_email], "subject": subject, "html": html},
                       timeout=15)
    except Exception as exc:  # non-fatal
        log.warning("email to %s failed: %r", to_email, exc)


async def merge_membership(conn, tenant_id: str, patch: dict) -> None:
    await conn.execute(
        """INSERT INTO tenant_settings (tenant_id, feature_flags)
           VALUES ($1::uuid, jsonb_build_object('membership', $2::jsonb))
           ON CONFLICT (tenant_id) DO UPDATE SET
           feature_flags = coalesce(tenant_settings.feature_flags,'{}'::jsonb)
                           || jsonb_build_object('membership',
                              coalesce(tenant_settings.feature_flags->'membership','{}'::jsonb) || $2::jsonb),
           updated_at = now()""",
        tenant_id, json.dumps(patch))


async def set_billing_hold(conn, tenant_id: str, value: bool) -> None:
    await conn.execute(
        """INSERT INTO tenant_settings (tenant_id, feature_flags)
           VALUES ($1::uuid, jsonb_build_object('billing_hold', $2::bool))
           ON CONFLICT (tenant_id) DO UPDATE SET
           feature_flags = coalesce(tenant_settings.feature_flags,'{}'::jsonb)
                           || jsonb_build_object('billing_hold', $2::bool),
           updated_at = now()""",
        tenant_id, value)


async def charge_off_session(stripe_key: str, customer_id: str, cents: int,
                             description: str, meta: dict) -> tuple[bool, str]:
    """Charge the card on file. Returns (ok, payment_intent_or_error)."""
    async with httpx.AsyncClient(timeout=25) as client:
        pms = await client.get("https://api.stripe.com/v1/payment_methods",
                               params={"customer": customer_id, "type": "card", "limit": 1},
                               headers={"Authorization": f"Bearer {stripe_key}"})
        data = (pms.json().get("data") or []) if pms.status_code < 300 else []
        if not data:
            return False, "no_card_on_file"
        form = {"amount": str(cents), "currency": "usd", "customer": customer_id,
                "payment_method": data[0]["id"], "off_session": "true", "confirm": "true",
                "description": description}
        for k, v in meta.items():
            form[f"metadata[{k}]"] = str(v)
        r = await client.post("https://api.stripe.com/v1/payment_intents",
                              headers={"Authorization": f"Bearer {stripe_key}"}, data=form)
        body = r.json()
        if r.status_code < 300 and body.get("status") == "succeeded":
            return True, body.get("id", "")
        return False, (body.get("error") or {}).get("code") or f"http_{r.status_code}"


async def main() -> None:
    db_url = os.environ["DATABASE_URL"]
    stripe_key = os.environ.get("STRIPE_SECRET_KEY", "")
    today = datetime.now(timezone.utc).date()
    target = today + timedelta(days=7)

    conn = await asyncpg.connect(db_url)
    try:
        tiers = {r["plan_key"]: int(r["price_usd_cents"]) for r in await conn.fetch(
            "SELECT plan_key, price_usd_cents FROM pricing_tiers WHERE plan_key LIKE 'membership_%' AND active")}

        rows = await conn.fetch(
            """SELECT s.id::text AS student_id, s.tenant_id::text AS tenant_id, s.birth_date,
                      s.first_name, t.primary_email, t.stripe_customer_id, t.display_name,
                      coalesce(ts.feature_flags->'membership','{}'::jsonb) AS membership,
                      coalesce((ts.feature_flags->>'billing_hold')::bool, false) AS billing_hold
                 FROM students s
                 JOIN tenants t ON t.id = s.tenant_id AND t.deleted_at IS NULL AND t.status = 'active'
                 LEFT JOIN tenant_settings ts ON ts.tenant_id = t.id
                WHERE s.deleted_at IS NULL AND s.birth_date IS NOT NULL""")

        charged = failed = held = 0
        for r in rows:
            birth: date = r["birth_date"]
            mem = json.loads(r["membership"]) if isinstance(r["membership"], str) else dict(r["membership"] or {})
            nb = next_birthday(birth, today)
            new_key = band_for_age(age_on(birth, nb))
            cur_key = band_for_age(age_on(birth, today))
            new_cents = tiers.get(new_key, 0)

            # ---- Sweep A: birthday in exactly 7 days, band changes, new band is paid
            if nb == target and new_key != cur_key and new_cents > 0 \
                    and mem.get("paid_key") != new_key:
                if not stripe_key or not r["stripe_customer_id"]:
                    ok, ref = False, "no_customer"
                else:
                    ok, ref = await charge_off_session(
                        stripe_key, r["stripe_customer_id"], new_cents,
                        f"outcomestar membership renewal - {new_key.replace('membership_age_', 'ages ').replace('_', '-')}",
                        {"birthday_billing": "1", "tenant_id": r["tenant_id"],
                         "student_id": r["student_id"], "membership_key": new_key})
                if ok:
                    charged += 1
                    await merge_membership(conn, r["tenant_id"], {
                        "paid_key": new_key, "paid_at": today.isoformat(),
                        "paid_until": (nb + timedelta(days=365)).isoformat(),
                        "payment_intent": ref, "pending_hold": None})
                    send_email(r["primary_email"], "outcomestar - membership renewed",
                               f"<p>{r['first_name']}'s membership moves to the next age band on "
                               f"{nb.isoformat()}. The card on file was charged ${new_cents/100:.2f} "
                               f"for the year ahead. Nothing else to do.</p>")
                else:
                    failed += 1
                    await merge_membership(conn, r["tenant_id"], {
                        "pending_hold": nb.isoformat(), "next_key": new_key,
                        "last_charge_error": ref, "charge_attempted_at": today.isoformat()})
                    send_email(r["primary_email"], "outcomestar - action needed: payment failed",
                               f"<p>{r['first_name']}'s membership moves to the next age band on "
                               f"{nb.isoformat()} (${new_cents/100:.2f}/yr), but the card on file "
                               f"was declined ({ref}). Update the card in your portal before "
                               f"{nb.isoformat()} - otherwise the student website will pause and "
                               "record editing will be locked until payment succeeds. Your data is "
                               "never deleted.</p>")
                    log.warning("charge failed tenant=%s student=%s err=%s", r["tenant_id"], r["student_id"], ref)

            # ---- Sweep B: hold date reached without payment
            ph = mem.get("pending_hold")
            if ph and not r["billing_hold"]:
                try:
                    hold_date = date.fromisoformat(ph)
                except Exception:
                    continue
                if hold_date <= today and mem.get("paid_key") != mem.get("next_key"):
                    held += 1
                    await set_billing_hold(conn, r["tenant_id"], True)
                    send_email(r["primary_email"], "outcomestar - account paused",
                               "<p>Membership payment for the new age band did not go through, so "
                               f"{r['first_name']}'s public website is paused and record editing is "
                               "locked. Nothing has been deleted. Update your card in the portal "
                               "(Storage &amp; Billing) and the account unlocks the moment payment "
                               "succeeds.</p>")
                    log.info("billing hold set tenant=%s", r["tenant_id"])

        log.info("birthday billing done: charged=%s failed=%s holds=%s scanned=%s",
                 charged, failed, held, len(rows))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
