"""Web scrapers for Google Maps, JobStreet, and Hiredly."""

import logging
import re
import time

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from utils import (
    classify_phone, clean_phone, normalize_name, PHONE_PATTERN,
)

logger = logging.getLogger(__name__)

# ============================================================
# Constants
# ============================================================

KL_AREAS = [
    "KLCC", "Bukit Bintang", "Bangsar", "Bangsar South", "KL Sentral",
    "Mid Valley", "Damansara Heights", "Sri Hartamas", "Mont Kiara",
    "Desa ParkCity", "Cheras", "Kepong", "Setapak", "Wangsa Maju",
    "Sentul", "Segambut", "Titiwangsa", "Jalan Ipoh", "Bukit Jalil",
    "Sri Petaling", "Desa Petaling", "Sungai Besi", "Batu Caves",
    "Selayang", "Gombak", "Brickfields", "Pudu", "Imbi",
]

SELANGOR_AREAS = [
    "Petaling Jaya", "Shah Alam", "Subang Jaya", "Klang", "Ampang",
    "Damansara", "Puchong", "Cyberjaya", "Putrajaya", "Seri Kembangan",
    "Kajang", "Bangi", "Rawang", "Sungai Buloh", "Kelana Jaya",
    "USJ", "Sunway", "Ara Damansara", "Kota Damansara",
    "Mutiara Damansara", "Bandar Utama", "Setia Alam",
    "Bukit Jelutong", "Port Klang", "Semenyih", "Sepang",
]

BUSINESS_KEYWORDS = [
    "company", "business", "office", "enterprise", "corporation",
    "IT company", "software company", "marketing agency",
    "accounting firm", "law firm", "construction company",
    "trading company", "logistics company", "manufacturing company",
    "restaurant", "clinic", "hotel", "shop",
]


# ============================================================
# Google Maps Scraper
# ============================================================


def scrape_google_maps(
    queries: list[str], max_results: int, progress_callback=None
) -> list[dict]:
    results: list[dict] = []
    all_urls: list[str] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-MY",
            timezone_id="Asia/Kuala_Lumpur",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        # Phase 1: Collect listing URLs
        for qi, query in enumerate(queries):
            if progress_callback:
                progress_callback(
                    f"Searching: {query}", qi / len(queries) * 0.4
                )

            search_url = (
                f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
            )
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
                time.sleep(3)
            except PlaywrightTimeout:
                logger.warning("Timeout loading search: %s", query)
                continue

            # Accept cookies dialog (safe to ignore failure)
            try:
                btn = page.query_selector('button:has-text("Accept all")')
                if btn:
                    btn.click()
                    time.sleep(1)
            except Exception:
                pass

            # Scroll results feed
            feed_sel = 'div[role="feed"]'
            try:
                page.wait_for_selector(feed_sel, timeout=10000)
            except PlaywrightTimeout:
                continue

            prev_count = 0
            no_change = 0
            for _ in range(80):
                page.evaluate(
                    f"document.querySelector('{feed_sel}').scrollTop = "
                    f"document.querySelector('{feed_sel}').scrollHeight"
                )
                time.sleep(1.5)
                end = page.query_selector(
                    'p.fontBodyMedium span:has-text("end of list")'
                ) or page.query_selector("span.HlvSq")
                if end:
                    break
                listings = page.query_selector_all(
                    'div[role="feed"] > div > div > a[href*="/maps/place/"]'
                )
                if len(listings) == prev_count:
                    no_change += 1
                    if no_change >= 5:
                        break
                else:
                    no_change = 0
                prev_count = len(listings)

            links = page.query_selector_all(
                'div[role="feed"] > div > div > a[href*="/maps/place/"]'
            )
            for link in links:
                href = link.get_attribute("href")
                if href and href not in all_urls:
                    all_urls.append(href)

            if len(all_urls) >= max_results:
                all_urls = all_urls[:max_results]
                break

        # Phase 2: Extract details from each listing
        total = len(all_urls)
        for i, url in enumerate(all_urls):
            if progress_callback:
                progress_callback(
                    f"Extracting {i + 1}/{total}", 0.4 + (i / total) * 0.6
                )

            biz: dict[str, str] = {
                "name": "", "phone": "", "phone_type": "", "website": "",
                "address": "", "category": "", "rating": "", "reviews": "",
                "source": "google_maps", "google_maps_url": url,
            }

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=15000)
                time.sleep(2)
            except PlaywrightTimeout:
                logger.warning("Timeout loading listing: %s", url)
                continue

            try:
                # Name
                h1 = page.query_selector("h1")
                if h1:
                    biz["name"] = h1.inner_text().strip()

                # Phone
                phone_el = page.query_selector('a[href^="tel:"]')
                if phone_el:
                    raw_href = phone_el.get_attribute("href") or ""
                    biz["phone"] = clean_phone(raw_href.replace("tel:", ""))
                else:
                    btns = page.query_selector_all('button[data-item-id*="phone"]')
                    for btn in btns:
                        label = btn.get_attribute("aria-label") or btn.inner_text()
                        match = PHONE_PATTERN.search(label)
                        if match:
                            biz["phone"] = clean_phone(match.group(1))
                            break
                if biz["phone"]:
                    biz["phone_type"] = classify_phone(biz["phone"])

                # Website
                web = page.query_selector('a[data-item-id="authority"]')
                if web:
                    biz["website"] = web.get_attribute("href") or ""

                # Address
                addr = page.query_selector('button[data-item-id="address"]')
                if addr:
                    biz["address"] = (
                        addr.get_attribute("aria-label") or ""
                    ).replace("Address: ", "")

                # Category
                cat = page.query_selector('button[jsaction*="category"]')
                if cat:
                    biz["category"] = cat.inner_text().strip()

                # Rating
                rat = page.query_selector('div.F7nice span[aria-hidden="true"]')
                if rat:
                    biz["rating"] = rat.inner_text().strip()

            except Exception as exc:
                logger.warning("Error extracting details from %s: %s", url, exc)

            if biz["name"]:
                results.append(biz)

            time.sleep(0.5)

        browser.close()

    return results


# ============================================================
# JobStreet Scraper
# ============================================================


def scrape_jobstreet(
    locations: list[str], max_pages: int, progress_callback=None
) -> list[dict]:
    results: list[dict] = []
    skip_names = {
        "write a review", "sign in", "employer site",
        "companies", "community", "",
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900}, locale="en-MY"
        )
        page = context.new_page()

        # Phase 1: Collect company URLs
        company_list: list[tuple[str, str]] = []
        for li, location in enumerate(locations):
            if progress_callback:
                progress_callback(
                    f"JobStreet: {location}", li / len(locations) * 0.3
                )

            for pg in range(1, max_pages + 1):
                url = (
                    f"https://my.jobstreet.com/companies"
                    f"?location={location.replace(' ', '+')}&page={pg}"
                )
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    time.sleep(4)
                except PlaywrightTimeout:
                    logger.warning("Timeout loading JobStreet page: %s", url)
                    break

                links = page.query_selector_all('a[href*="/companies/"]')
                count = 0
                for link in links:
                    href = link.get_attribute("href") or ""
                    try:
                        name = re.sub(
                            r"[^\x20-\x7E]", "", link.inner_text().strip()
                        )
                    except Exception:
                        continue
                    if (
                        name.lower() in skip_names
                        or len(name) < 2
                        or "/companies?" in href
                    ):
                        continue
                    if not href.startswith("http"):
                        href = f"https://my.jobstreet.com{href}"
                    href = href.split("?")[0]
                    if not any(c[1] == href for c in company_list):
                        company_list.append((name, href))
                        count += 1
                if count == 0:
                    break
                time.sleep(2)

        # Phase 2: Extract details
        total = len(company_list)
        for i, (name, url) in enumerate(company_list):
            if progress_callback:
                progress_callback(
                    f"Details {i + 1}/{total}", 0.3 + (i / total) * 0.7
                )

            company: dict[str, str] = {
                "name": name, "phone": "", "phone_type": "",
                "website": "", "address": "", "category": "",
                "company_size": "", "source": "jobstreet", "jobstreet_url": url,
            }

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=15000)
                time.sleep(3)
                body = page.inner_text("body")
                lines = [
                    re.sub(r"[^\x20-\x7E]", "", ln.strip())
                    for ln in body.split("\n")
                ]

                for j, line in enumerate(lines):
                    if j + 1 >= len(lines):
                        break
                    nxt = lines[j + 1]
                    if line == "Website" and nxt and len(nxt) < 200:
                        company["website"] = nxt
                    elif line == "Industry" and nxt and len(nxt) < 200:
                        company["category"] = nxt
                    elif line == "Company size" and nxt and len(nxt) < 100:
                        company["company_size"] = nxt
                    elif line == "Primary location" and nxt and len(nxt) < 300:
                        company["address"] = nxt
            except Exception as exc:
                logger.warning("Error extracting JobStreet details from %s: %s", url, exc)

            results.append(company)
            time.sleep(1)

        browser.close()

    return results


# ============================================================
# Hiredly Scraper
# ============================================================


def scrape_hiredly(max_companies: int, progress_callback=None) -> list[dict]:
    results: list[dict] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900}, locale="en-MY"
        )
        page = context.new_page()

        if progress_callback:
            progress_callback("Loading Hiredly companies...", 0.1)

        try:
            page.goto(
                "https://my.hiredly.com/companies",
                wait_until="domcontentloaded",
                timeout=20000,
            )
            time.sleep(4)
        except PlaywrightTimeout:
            logger.warning("Timeout loading Hiredly companies page")
            browser.close()
            return results

        # Scroll to load more
        for _ in range(30):
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            links = page.query_selector_all('a[href*="/companies/"]')
            if len(links) >= max_companies + 5:
                break

        # Collect URLs
        company_list: list[str] = []
        seen: set[str] = set()
        for link in page.query_selector_all('a[href*="/companies/"]'):
            href = link.get_attribute("href") or ""
            if "/companies/" not in href or "/companies?" in href or href in seen:
                continue
            if not href.startswith("http"):
                href = f"https://my.hiredly.com{href}"
            seen.add(href)
            company_list.append(href)
            if len(company_list) >= max_companies:
                break

        # Extract details
        total = len(company_list)
        for i, url in enumerate(company_list):
            if progress_callback:
                progress_callback(
                    f"Hiredly {i + 1}/{total}", 0.2 + (i / total) * 0.8
                )

            company: dict[str, str] = {
                "name": "", "phone": "", "phone_type": "",
                "website": "", "address": "", "category": "",
                "source": "hiredly", "hiredly_url": url,
            }

            try:
                page.goto(url, wait_until="domcontentloaded", timeout=15000)
                time.sleep(3)
                body = page.inner_text("body")
                lines = [
                    re.sub(r"[^\x20-\x7E]", "", ln.strip())
                    for ln in body.split("\n")
                ]
                lines = [ln for ln in lines if ln and len(ln) > 1]

                skip = {
                    "Companies", "Log In", "Sign Up", "For Employers",
                    "Jobs Search", "Internships", "Drop Resume", "English",
                }
                for line in lines:
                    if line not in skip and 2 < len(line) < 100:
                        company["name"] = line
                        break

                # Industry (line after company name)
                name_found = False
                for line in lines:
                    if line == company["name"]:
                        name_found = True
                        continue
                    if name_found and len(line) < 80 and not line.endswith("jobs"):
                        company["category"] = line
                        break

                # Address
                for j, line in enumerate(lines):
                    if line == "Our Address" and j + 1 < len(lines):
                        for k in range(j + 1, min(j + 5, len(lines))):
                            if lines[k] and len(lines[k]) > 10:
                                company["address"] = lines[k]
                                break
                        break

                # Website
                for a in page.query_selector_all("a[href]"):
                    href = a.get_attribute("href") or ""
                    blocked = [
                        "hiredly", "google", "facebook", "linkedin",
                        "twitter", "instagram", "youtube", "tiktok",
                    ]
                    if href.startswith("http") and not any(
                        x in href for x in blocked
                    ):
                        company["website"] = href
                        break

            except Exception as exc:
                logger.warning("Error extracting Hiredly details from %s: %s", url, exc)

            if company["name"]:
                results.append(company)
            time.sleep(1)

        browser.close()

    return results


# ============================================================
# Merge & Deduplicate
# ============================================================


def merge_results(all_results: list[dict]) -> list[dict]:
    """Merge and deduplicate scraped results. Returns a new list of dicts (no mutation)."""
    companies: dict[str, dict] = {}

    for row in all_results:
        name = row.get("name", "")
        if not name:
            continue
        key = normalize_name(name)
        if not key:
            continue

        existing = companies.get(key)
        if not existing:
            # Store a copy to avoid mutating input
            companies[key] = {**row, "sources": row.get("source", "")}
        else:
            # Build updated copy instead of mutating existing
            updated = {**existing}

            ex_phone = existing.get("phone", "")
            new_phone = row.get("phone", "")
            if (
                classify_phone(new_phone) == "mobile"
                and classify_phone(ex_phone) != "mobile"
            ):
                updated["phone"] = new_phone
            elif not ex_phone and new_phone:
                updated["phone"] = new_phone

            new_web = row.get("website", "")
            if new_web.startswith("http") and not existing.get(
                "website", ""
            ).startswith("http"):
                updated["website"] = new_web

            for field in ["address", "category", "company_size", "rating"]:
                if not existing.get(field) and row.get(field):
                    updated[field] = row[field]

            for url_field in [
                "google_maps_url", "jobstreet_url", "hiredly_url",
            ]:
                if not existing.get(url_field) and row.get(url_field):
                    updated[url_field] = row[url_field]

            src = row.get("source", "")
            if src and src not in existing.get("sources", ""):
                updated["sources"] = (
                    f"{existing.get('sources', '')}, {src}".strip(", ")
                )

            companies[key] = updated

    rows = list(companies.values())

    # Update phone_type
    for r in rows:
        r["phone_type"] = classify_phone(r.get("phone", ""))

    # Sort: mobile first, then landline, then none
    rows.sort(
        key=lambda r: (
            0 if r.get("phone_type") == "mobile"
            else 1 if r.get("phone_type") == "landline"
            else 2
        )
    )

    return rows
