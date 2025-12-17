from playwright.sync_api import sync_playwright
from datetime import datetime
import re
import requests
import os
import sys
from dotenv import load_dotenv
import dns.resolver
import socket
import psutil
import signal
from datetime import datetime, UTC
import sqlite3
from contextlib import closing

DASHBOARD_DB_PATH = os.getenv(
    "DASHBOARD_DB_PATH", "/root/job_scraper/dashboard/data/logs.db"
)


LOCKFILE = "/tmp/job_scraper.lock"
MAX_RUNTIME_SECONDS = 5 * 60 * 60  # 5 hours


def timeout_handler(signum, frame):
    print("⏰ Max runtime exceeded, killing process")
    raise TimeoutError("Max runtime exceeded")


signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(MAX_RUNTIME_SECONDS)

# Verificar lockfile
if os.path.exists(LOCKFILE):
    with open(LOCKFILE, "r") as f:
        old_pid = f.read().strip()

    if old_pid.isdigit() and psutil.pid_exists(int(old_pid)):
        print("❌ Another instance is already running. Exiting.")
        sys.exit(0)
    else:
        print("⚠️ Removing stale lockfile")
        os.remove(LOCKFILE)


with open(LOCKFILE, "w") as f:
    f.write(str(os.getpid()))

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_COMPANIES_TABLE = os.getenv("AIRTABLE_COMPANIES_TABLE", "Companies")
AIRTABLE_JOBS_TABLE = os.getenv("AIRTABLE_JOBS_TABLE", "Jobs")
AIRTABLE_CONTACTS_TABLE = os.getenv("AIRTABLE_CONTACTS_TABLE", "Contacts")


print("🔐 Token prefix:", (AIRTABLE_API_KEY or "")[:10])


def db_exec(query: str, params: tuple = ()):
    with closing(sqlite3.connect(DASHBOARD_DB_PATH)) as conn:
        conn.execute(query, params)
        conn.commit()


def db_query_one(query: str, params: tuple = ()):
    with closing(sqlite3.connect(DASHBOARD_DB_PATH)) as conn:
        cur = conn.execute(query, params)
        return cur.fetchone()


def run_log_start():
    started_at = datetime.now(UTC).isoformat()
    with closing(sqlite3.connect(DASHBOARD_DB_PATH)) as conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, status) VALUES (?, ?)",
            (started_at, "running"),
        )
        conn.commit()
        run_id = cur.lastrowid
    return run_id, started_at


def run_log_finish(
    run_id: int,
    status: str,
    jobs_found=0,
    companies_created=0,
    contacts_created=0,
    error_message=None,
):
    finished_at = datetime.now(UTC).isoformat()
    db_exec(
        """UPDATE runs
           SET finished_at=?, status=?, jobs_found=?, companies_created=?, contacts_created=?, error_message=?
           WHERE id=?""",
        (
            finished_at,
            status,
            jobs_found,
            companies_created,
            contacts_created,
            error_message,
            run_id,
        ),
    )


def airtable_request(method, table_name, json_data=None, params=None):
    """Small wrapper around Airtable HTTP API."""
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
        raise RuntimeError("AIRTABLE_API_KEY or AIRTABLE_BASE_ID not set")

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_name}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }

    resp = requests.request(
        method, url, headers=headers, json=json_data, params=params, timeout=30
    )

    if not resp.ok:
        print(
            f"⚠️ Airtable error [{method} {table_name}]: "
            f"{resp.status_code} {resp.text}"
        )

    return resp.json()


def load_existing_companies():
    companies_by_name = {}
    offset = None
    total = 0

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        data = airtable_request("GET", AIRTABLE_COMPANIES_TABLE, params=params)

        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            name = fields.get("company_name", "")
            if not name:
                continue
            key = name.strip().lower()
            companies_by_name[key] = rec["id"]
            total += 1

        offset = data.get("offset")
        if not offset:
            break

    print(f"📊 Loaded {total} companies from Airtable")
    return companies_by_name


def load_existing_jobs():
    jobs_by_url = set()
    offset = None
    total = 0

    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset

        data = airtable_request("GET", AIRTABLE_JOBS_TABLE, params=params)

        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            url = fields.get("job_url")
            if url:
                jobs_by_url.add(url.strip())
                total += 1

        offset = data.get("offset")
        if not offset:
            break

    print(f"📊 Loaded {total} jobs from Airtable")
    return jobs_by_url


def create_company(company_record):
    payload = {"records": [{"fields": company_record}]}
    data = airtable_request("POST", AIRTABLE_COMPANIES_TABLE, json_data=payload)

    try:
        record = data["records"][0]
        company_id = record["id"]
        print(f"✅ Created new company: {record['fields'].get('company_name')}")
        return company_id, True
    except Exception:
        print(f"⚠️ Unexpected response when creating company: {data}")
        return None


def create_job(job_record, company_id):
    if not company_id:
        print("⚠️ Skipping job creation: company_id is None")
        return

    fields = dict(job_record)
    fields["company"] = [company_id]

    payload = {"records": [{"fields": fields}]}
    data = airtable_request("POST", AIRTABLE_JOBS_TABLE, json_data=payload)

    try:
        record = data["records"][0]
        print(f"💼 Job added: {record['fields'].get('job_title')}")
        return True
    except Exception:
        print(f"⚠️ Unexpected response when creating job: {data}")
        return False


EMAIL_BLACKLIST_SUBSTRINGS = [
    "example@",
    "user@domain",
    "admin@domain",
    "@domain.com",
    "@domain.net",
    "@example.",
    "no-reply@",
    "noreply@",
    "do-not-reply@",
    "test@",
    "email@",
    "yourname@",
    "your@email",
    "sample@",
    "@sentry.",
    "core-js-bundle@",
    "react@",
    "react-dom@",
    "lodash@",
    "intl-segmenter@",
    "focus-within-polyfill@",
]


def extract_emails_from_html(html: str):
    """Extract and filter emails from raw HTML."""
    emails = re.findall(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", html, flags=re.IGNORECASE
    )
    cleaned = []
    for e in emails:
        if any(bad in e for bad in EMAIL_BLACKLIST_SUBSTRINGS):
            continue
        cleaned.append(e.lower())
    seen = set()
    final = []
    for e in cleaned:
        if e not in seen:
            seen.add(e)
            final.append(e)
    return final


def find_internal_links(page, base_url: str):
    anchors = page.query_selector_all("a[href]")
    internal = []
    base_domain = base_url.split("/")[2] if "://" in base_url else base_url

    for a in anchors:
        href = a.get_attribute("href")
        if not href:
            continue

        lower = href.lower()
        if not any(
            k in lower for k in ["contact", "about", "support", "team", "impressum"]
        ):
            continue

        # normalizar URL relativa
        if href.startswith("/"):
            href = f"{base_url.rstrip('/')}{href}"

        if base_domain in href:
            internal.append(href)

    # remove dupes, limit a 3
    unique = list(dict.fromkeys(internal))[:3]
    return unique


def validate_email_syntax(email: str):
    pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    return re.match(pattern, email) is not None


def validate_mx_records(domain: str):
    try:
        answers = dns.resolver.resolve(domain, "MX", lifetime=2)
        return [str(r.exchange).rstrip(".") for r in answers]
    except Exception:
        return None


def smtp_zero_handshake(mx_host: str):
    try:
        server = socket.create_connection((mx_host, 25), timeout=1)
        server.close()
        return True
    except Exception:
        return False


def validate_email(email: str):
    if not validate_email_syntax(email):
        return {"status": "syntax_error", "mx": None}

    domain = email.split("@")[1]

    mx_records = validate_mx_records(domain)
    if not mx_records:
        return {"status": "invalid_mx", "mx": None}

    handshake_ok = smtp_zero_handshake(mx_records[0])

    return {
        "status": "smtp_verified" if handshake_ok else "valid_mx",
        "mx": mx_records,
    }


def create_contact(email, source_url, company_id, validation, scraped_at):
    record = {
        "email": email,
        "source_url": source_url,
        "company": [company_id],
        "verification_status": validation["status"],
        "mx_records": ", ".join(validation["mx"]) if validation["mx"] else "",
        "created_at_utc": scraped_at,
    }

    payload = {"records": [{"fields": record}]}
    resp = airtable_request("POST", AIRTABLE_CONTACTS_TABLE, json_data=payload)

    if "records" in resp:
        print(f"📇 Contact added: {email}")
        return True
    else:
        print("⚠️ Failed to create contact:", resp)
        return False


def choose_primary_email(email_list):
    priority = ["jobs@", "careers@", "hr@", "talent@"]
    secondary = ["contact@", "info@"]

    for e in email_list:
        if any(p in e for p in priority):
            return e

    for e in email_list:
        if any(s in e for s in secondary):
            return e

    return email_list[0]


def run_once():
    print("🌐 Accessing Backpacker Job Board...")

    companies_created = 0
    contacts_created = 0
    new_jobs = 0

    companies_by_name = load_existing_companies()
    jobs_by_url = load_existing_jobs()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        context.set_default_timeout(15000)  # 15 segundos
        context.set_default_navigation_timeout(15000)

        page.goto(
            "https://www.backpackerjobboard.com.au/", wait_until="domcontentloaded"
        )
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except:
            page.wait_for_load_state("domcontentloaded")

        print("🧱 Looking for the 'Labourer Jobs' link...")
        page.wait_for_selector("a[href*='/jobs/labour-trade/']", timeout=45000)
        page.click("a[href*='/jobs/labour-trade/']")
        page.wait_for_url("**/jobs/labour-trade/**", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except:
            page.wait_for_load_state("domcontentloaded")

        print("📋 Collecting job listings...")
        page.wait_for_selector("div.jobs-list", timeout=60000)
        jobs = page.query_selector_all("div.jobs-list .job-entry")
        print(f"✅ Total jobs found: {len(jobs)}")

        print("\n🔗 Collecting job links and metadata...")
        job_items = []
        for job in jobs:
            title_el = job.query_selector("h3.job-title a")
            company_el = job.query_selector(
                "p.job-company span span"
            ) or job.query_selector("p.job-company")
            location_el = job.query_selector("p.job-location")
            category_el = job.query_selector("p.job-category span")

            title = title_el.inner_text().strip() if title_el else ""
            company_name = company_el.inner_text().strip() if company_el else ""
            location = (
                location_el.inner_text().strip().replace("\n", " ")
                if location_el
                else ""
            )
            category = category_el.inner_text().strip() if category_el else ""

            href = title_el.get_attribute("href") if title_el else ""
            if href and href.startswith("/"):
                href = f"https://www.backpackerjobboard.com.au{href}"

            job_items.append(
                {
                    "job_title": title,
                    "company_name": company_name,
                    "job_location": location,
                    "category": category,
                    "job_url": href,
                }
            )

        if job_items:
            print(f"✅ Links collected: {len(job_items)}")
            print("Example:", [j["job_url"] for j in job_items[:3]])

        print("\n🧭 Visiting job details and company websites...\n")

        for idx, job_info in enumerate(job_items, start=1):
            job_url = job_info["job_url"]
            title = job_info["job_title"]
            company_name = job_info["company_name"]
            location = job_info["job_location"]
            category = job_info["category"]

            print(f"➡️  ({idx}/{len(job_items)}) Opening job: {job_url}")

            if job_url and job_url in jobs_by_url:
                print(f"🔄 Job already exists: {title}")
                continue

            company_key = company_name.strip().lower() if company_name else ""
            existing_company_id = companies_by_name.get(company_key)

            scraped_at = datetime.now(UTC).strftime("%Y-%m-%d")

            if existing_company_id:
                print(f"🔄 Company already exists: {company_name}")
                job_record = {
                    "job_title": title,
                    "job_location": location,
                    "job_url": job_url,
                    "category": category,
                    "scraped_at_utc": scraped_at,
                }
                created_job = create_job(job_record, existing_company_id)
                if created_job:
                    new_jobs += 1
                    jobs_by_url.add(job_url)
                continue

            detail = context.new_page()
            try:
                detail.goto(job_url, wait_until="domcontentloaded", timeout=45000)

                employer_profile = detail.query_selector("div.employer-profile")
                if not employer_profile:
                    print("❌ No employer profile block found.")
                    detail.close()
                    continue

                site_el = employer_profile.query_selector("a[href^='http']")
                if not site_el:
                    print("❌ No company website link found in employer profile.")
                    detail.close()
                    continue

                site_url = site_el.get_attribute("href")
                if not site_url or "backpackerjobboard.com.au" in site_url:
                    print("ℹ️ Company website link is internal or invalid.")
                    detail.close()
                    continue

                if site_url.startswith("http://https://"):
                    site_url = site_url.replace("http://https://", "https://")

                if site_url.startswith("https://https://"):
                    site_url = site_url.replace("https://https://", "https://")

                if site_url.startswith("http//"):
                    site_url = "http://" + site_url[5:]

                if "https//" in site_url and "https://" not in site_url:
                    site_url = site_url.replace("https//", "https://")

                print(f"🌐 Company website found: {site_url}")

                company_page = context.new_page()
                try:
                    company_page.goto(
                        site_url, wait_until="domcontentloaded", timeout=45000
                    )

                    internal_links = find_internal_links(company_page, site_url)
                    print(f"🔗 Relevant internal pages: {internal_links}")

                    emails = extract_emails_from_html(company_page.content())

                    for internal_url in internal_links:
                        try:
                            company_page.goto(
                                internal_url,
                                wait_until="domcontentloaded",
                                timeout=45000,
                            )
                            emails += extract_emails_from_html(company_page.content())
                        except Exception:
                            continue

                    emails = list(dict.fromkeys(emails))
                    if not emails:
                        print("❌ No public emails found.")
                        continue

                    print(f"📧 Public emails found: {emails}")
                    # -----------------------------------------
                    # Milestone 2: Email validation + Contacts
                    # -----------------------------------------

                    company_record = {
                        "company_name": company_name,
                        "company_website": site_url,
                        "emails_found": ", ".join(emails),
                        "scraped_at_utc": scraped_at,
                    }
                    new_company_id, created = create_company(company_record)
                    if not new_company_id:
                        continue

                    if created:
                        companies_created += 1

                    companies_by_name[company_key] = new_company_id
                    validated_emails = []

                    for email in emails:
                        validation = validate_email(email)
                        create_contact(
                            email=email,
                            source_url=site_url,
                            company_id=new_company_id,
                            validation=validation,
                            scraped_at=scraped_at,
                        )
                        contacts_created += 1
                        validated_emails.append(email)

                    # Choose primary email for outreach
                    primary_email = choose_primary_email(validated_emails)

                    # Update company with primary email + status
                    airtable_request(
                        "PATCH",
                        AIRTABLE_COMPANIES_TABLE,
                        json_data={
                            "records": [
                                {
                                    "id": new_company_id,
                                    "fields": {
                                        "primary_contact_email": primary_email,
                                        "email_discovery_status": "completed",
                                    },
                                }
                            ]
                        },
                    )

                    print(f"🏆 Primary contact set: {primary_email}")

                    job_record = {
                        "job_title": title,
                        "job_location": location,
                        "job_url": job_url,
                        "category": category,
                        "scraped_at_utc": scraped_at,
                    }
                    created_job = create_job(job_record, new_company_id)
                    if created_job:
                        new_jobs += 1
                        jobs_by_url.add(job_url)

                except Exception as e:
                    print(f"⚠️ Error while visiting company site: {e}")
                finally:
                    try:
                        company_page.close()
                    except Exception:
                        pass

            except Exception as e:
                print(f"⚠️ Error while opening job detail {job_url}: {e}")
            finally:
                try:
                    detail.close()
                except Exception:
                    pass

        context.close()
        browser.close()
        return new_jobs, companies_created, contacts_created


if __name__ == "__main__":
    run_id = None
    companies_created = 0
    contacts_created = 0
    jobs_found = 0

    try:
        run_id, _ = run_log_start()
        # ajuste o run_once() para retornar métricas:
        jobs_found, companies_created, contacts_created = run_once()
        run_log_finish(
            run_id, "success", jobs_found, companies_created, contacts_created
        )

    except Exception as e:
        print(f"❌ Fatal error: {e}")
        if run_id is not None:
            run_log_finish(run_id, "error", error_message=str(e))

    finally:
        signal.alarm(0)
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
