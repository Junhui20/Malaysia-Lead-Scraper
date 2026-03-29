"""Web scrapers for Google Maps, JobStreet, and Hiredly.

Phase 1 (URL collection) runs sequentially — scrolling is inherently serial.
Phase 2 (detail extraction) runs concurrently with asyncio + Semaphore.
"""

import asyncio
import logging
import re
import sys
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from utils import (
    classify_phone, clean_phone, normalize_name, normalize_my_phone,
    is_valid_my_phone, PHONE_PATTERN, MY_PHONE_PATTERN, PHONE_CONTEXT_WORDS,
)

logger = logging.getLogger(__name__)

DEFAULT_CONCURRENCY = 3

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

# Default blocklist: large corps, chains, banks, government, telcos
# Users can customise this in Settings
SKIP_LARGE_COMPANIES = [
    # ======== Banks & Finance ========
    # Commercial banks
    "maybank", "cimb", "public bank", "rhb bank", "hong leong bank",
    "ambank", "bank islam", "bank rakyat", "affin bank", "alliance bank",
    "bank muamalat", "agro bank", "agrobank", "bsn", "bank simpanan",
    "mbsb bank", "al-rajhi bank",
    # Foreign banks
    "standard chartered", "hsbc", "uob", "ocbc", "bank negara",
    "citibank", "deutsche bank", "bnp paribas", "jp morgan",
    "bank of china", "icbc", "mizuho", "sumitomo",
    # Digital banks
    "gxbank", "boost bank", "aeon bank", "kaf digital", "ryt bank",
    "touch n go", "touch 'n go", "tng digital", "bigpay", "grab pay",
    # Investment banks & brokerages
    "kenanga", "maybank investment", "cimb investment", "hong leong investment",
    "rakuten trade", "moomoo", "phillip capital",
    # ======== Insurance & Takaful ========
    "allianz", "aia ", "prudential", "great eastern", "zurich",
    "tokio marine", "generali", "manulife", "sunlife", "sun life",
    "etiqa", "takaful", "tune protect", "fwd insurance", "liberty insurance",
    "axa affin", "berjaya sompo", "chubb insurance", "msig",
    "lonpac insurance", "pacific & orient", "rhb insurance",
    "hong leong assurance", "mcis insurance", "ammetlife",
    "hannover re", "swiss re", "malaysian re",
    "syarikat takaful", "prudential bsn",
    # ======== Telcos & ISP ========
    "maxis", "digi", "celcom", "u mobile", "unifi", "tm ", "telekom malaysia",
    "yes 4g", "redone", "astro", "time dotcom", "allo technology",
    "viewqwest", "mytv broadcasting",
    # ======== Government, GLC & Statutory Bodies ========
    "jabatan", "kementerian", "pejabat", "suruhanjaya", "lembaga",
    "polis", "bomba", "kastam", "imigresen", "jpj", "jpn",
    "majlis perbandaran", "majlis bandaraya", "dewan bandaraya",
    "perbadanan", "agensi", "tribunal", "mahkamah",
    "kwsp", "epf", "socso", "perkeso", "lhdn", "hasil",
    "kwap", "tabung haji", "ltat", "felda", "risda",
    "mara", "tekun", "teraju", "matrade", "mida", "miti",
    "dbkl", "mbpj", "mbsa", "mpsj", "mpaj", "mpkj",
    "prasarana", "ktmb", "rapidkl", "mrt corp",
    "khazanah", "permodalan nasional", "pnb",
    # ======== Petrol Stations ========
    "petronas dagangan", "petronas station", "petronas mesra",
    "shell", "petron", "caltex", "bhpetrol",
    # ======== Fast Food (International) ========
    "mcdonald", "kfc", "pizza hut", "domino", "burger king",
    "starbucks", "subway", "nando", "kenny rogers",
    "texas chicken", "wendy's", "a&w ", "carl's jr",
    "taco bell", "popeyes", "wingstop", "shake shack",
    "five guys", "chili's", "tgi friday",
    "4 fingers", "4fingers",
    # ======== Fast Food & Restaurant Chains (Local/Regional) ========
    "marrybrown", "ayam penyet", "nasi kandar pelita",
    "sushi king", "sushi zanmai", "sakae sushi", "genki sushi",
    "boat noodle", "myeongdong topokki", "kyochon",
    "the chicken rice shop", "pappa rich", "papparich",
    "manhattan fish market", "fish & co",
    "nene chicken", "jollibee", "yoshinoya", "ootoya",
    "haidilao", "xiabu xiabu", "wagyu more",
    "nam heong", "village park", "restoran oversea",
    "tony roma", "outback steakhouse", "red lobster",
    "italiannies", "la risata", "the alley",
    "ramly", "myburgerlab", "the daily grind",
    "oldtown white coffee", "secret recipe",
    "absolute thai", "thai express",
    "sate kajang hj samuri", "wong kok",
    # ======== Bakery & Dessert Chains ========
    "bread talk", "tous les jours", "dunkin", "krispy kreme",
    "baskin robbins", "haagen dazs", "llao llao",
    "inside scoop", "sangkaya", "myeongrang",
    "lavender bakery", "rt pastry", "komugi",
    "paris baguette", "hokkaido baked cheese tart",
    "auntie anne", "big apple donuts", "j.co donuts",
    "rotiboy", "dutch lady",
    "freemori", "barcook", "kenny hills bakers",
    # ======== Bubble Tea / Milk Tea Chains ========
    "chatime", "tealive", "daboba", "gong cha", "tiger sugar",
    "coolblog", "xing fu tang", "each a cup",
    "the whale tea", "moojicha", "chagee",
    "liho tea", "heytea", "auntea jenny",
    "beautea", "cute tea", "ning cha",
    "mixue", "coco bubble", "koi the",
    "share tea", "black whale", "don't yell at me",
    # ======== Coffee Chains ========
    "zus coffee", "luckin coffee", "coffee bean", "san francisco coffee",
    "toby's estate", "my liberica", "bask bear",
    "gigi coffee", "kenangan coffee", "oriental kopi",
    "flash coffee", "% arabica", "pacific coffee",
    "dôme", "o'briens", "plan b",
    # ======== Supermarkets & Hypermarkets ========
    "aeon", "aeon big", "tesco", "lotus's", "giant",
    "mydin", "parkson", "isetan",
    "jaya grocer", "village grocer", "cold storage", "mercato",
    "econsave", "99 speedmart", "nsk trade city",
    "lulu hypermarket", "billion supermarket", "hero market",
    "pacific hypermarket", "pantai supermarket",
    "family store", "cmart", "c-mart",
    "benns", "ben's independent grocer", "b.i.g.",
    "sam's groceria", "k grocer",
    # ======== Convenience Stores ========
    "7-eleven", "family mart", "mynews", "kkmart", "kk mart",
    "easymart", "cmart", "petronas mesra",
    # ======== Pharmacy Chains ========
    "watsons", "guardian", "caring pharmacy", "big pharmacy",
    "alpro pharmacy", "aa pharmacy",
    "healthlane", "medicare pharmacy",
    # ======== Electronics & Gadget Retail ========
    "senheng", "harvey norman", "courts", "best denki",
    "machines", "switch", "directd",
    "all it hypermarket", "thunder match",
    "urban republic", "tmtbythephone",
    "lowyat plaza",
    # ======== Fashion & Clothing Retail ========
    "zara", "cotton on", "padini", "brands outlet",
    "bonia", "vincci", "seed kid", "carlo rino",
    "uniqlo", "h&m", "muji", "gu ",
    "sephora", "watsons",
    "nike", "adidas", "puma", "new balance",
    "giordano", "esprit", "levi's",
    "charles & keith", "pedro", "coach",
    "mango", "topshop", "forever 21",
    "shein", "factorie",
    # ======== Home & Furniture Chains ========
    "ikea", "homepro", "index living",
    "ace hardware", "mr diy",
    "majuhome", "nitori", "kinsen home",
    "kaison", "daiso", "mr dollar",
    "ssf home", "fella design", "lavino",
    # ======== Big Tech / MNCs ========
    "google", "microsoft", "apple", "amazon", "meta", "samsung",
    "huawei", "intel", "ibm", "oracle", "dell", "hp ",
    "sony", "panasonic", "sharp", "daikin", "toshiba", "hitachi",
    "siemens", "bosch", "schneider electric", "abb",
    "cisco", "sap", "salesforce", "adobe",
    "grab ", "shopee", "lazada",
    # ======== Big 4 / Consultancies ========
    "accenture", "deloitte", "pwc", "pricewaterhouse",
    "kpmg", "ernst & young", "ey ",
    "mckinsey", "bain", "bcg", "boston consulting",
    "capgemini", "infosys", "tcs ", "wipro",
    # ======== MY Conglomerates & Bursa Blue Chips ========
    "petronas", "tenaga nasional", "tnb",
    "sime darby", "gamuda", "ijm corp",
    "ytl", "genting", "ioi ", "sunway group",
    "sp setia", "mah sing", "eco world", "tropicana",
    "uem sunrise", "uda holdings",
    "axiata", "sapura", "misc berhad", "dialog group",
    "top glove", "hartalega", "supermax", "kossan",
    "press metal", "malayan cement", "yinson",
    "hap seng", "berjaya", "naza", "tan chong",
    "hong leong", "kuok group", "robert kuok",
    "cdb aviation", "airasia", "malaysia airlines",
    "malindo", "batik air", "firefly",
    "mmhe", "velesto", "serba dinamik",
    "mr d.i.y", "ql resources", "nestle malaysia",
    "dutch lady", "f&n", "fraser neave",
    "carlsberg", "heineken", "guinness anchor",
    # ======== Hospitals & Healthcare Chains ========
    "kpj", "ihh healthcare", "pantai hospital", "gleneagles",
    "columbia asia", "prince court", "sunway medical",
    "thomson hospital", "beacon hospital",
    "bp healthcare", "pathlab", "quest lab",
    "tung shin", "assunta", "subang jaya medical",
    "regency specialist", "mahkota medical",
    "island hospital", "loh guan lye",
    "national cancer institute", "institut jantung negara",
    "klinik 1malaysia", "klinik kesihatan",
    # ======== Automotive Brands (Dealerships) ========
    "toyota", "honda", "nissan", "mazda", "mitsubishi",
    "hyundai", "kia", "proton", "perodua",
    "mercedes", "bmw", "audi", "volkswagen", "volvo",
    "lexus", "porsche", "jaguar", "land rover",
    "subaru", "suzuki", "isuzu", "hino",
    "ford", "chevrolet", "jeep", "peugeot",
    "renault", "citroen", "mini cooper",
    "tesla", "byd", "chery", "haval", "ora good cat",
    "geely", "proton x", "smart #",
    # ======== Courier & Logistics ========
    "pos malaysia", "poslaju", "j&t express", "ninja van",
    "dhl", "fedex", "gdex", "skynet", "city-link",
    "grab express", "lalamove", "aramex",
    "flash express", "shopee express", "lazada logistics",
    "best express", "zepto express", "abx express",
    "tnt express", "ups ", "dpex",
    "pgeon", "matdespatch",
    # ======== Cinemas & Entertainment ========
    "tgv cinema", "gsc cinema", "mbo cinema", "mmcineplexes",
    "golden screen", "goldenscreen", "dadi cinema",
    "timezone", "funcity", "berjaya times square theme park",
    "genting skyworlds", "legoland", "sunway lagoon",
    "kidzania", "district 21",
    # ======== Gym & Fitness Chains ========
    "celebrity fitness", "fitness first", "anytime fitness",
    "chi fitness", "jetts fitness", "snap fitness",
    "true fitness", "gold's gym", "f45 training",
    "bpm fitness",
    # ======== Hotels (International Chains) ========
    "hilton", "marriott", "sheraton", "westin", "hyatt",
    "four seasons", "ritz carlton", "intercontinental",
    "holiday inn", "crowne plaza", "novotel", "ibis",
    "shangri-la", "mandarin oriental",
    "w hotel", "st regis", "fairmont",
    "pullman", "sofitel", "accor",
    "radisson", "best western", "renaissance",
    "le meridien", "doubletree", "hampton",
    "oyo rooms", "oyo ", "tune hotel", "fave hotel",
    "sunway hotel", "impiana", "berjaya hotel",
    # ======== Education (Big Institutions) ========
    "universiti malaya", "universiti kebangsaan", "universiti putra",
    "universiti teknologi", "uitm", "upm", "ukm", "um ",
    "taylor's university", "monash", "nottingham",
    "sunway university", "ucsi", "inti international",
    "asia pacific university", "apu ", "mmu",
    "help university", "segi university",
    "limkokwing", "binary university",
    "kolej", "politeknik",
    # Tuition chains
    "kumon", "mind stretcher", "brainy bunch",
    # ======== Property Developers (listed) ========
    "cbre", "knight frank", "savills", "jones lang",
    "henry butcher", "rahim & co",
    "ijm land", "ecoworld", "paramount",
    "matrix concepts", "lbs bina", "glomac",
    "plenitude", "e&o ", "eastern & oriental",
    "country garden", "r&f ", "greenland",
    "emkay", "naza ttdi",
    # ======== Co-working Spaces ========
    "wework", "common ground", "colony", "worq",
    "regus", "spaces", "compass offices",
    # ======== Laundry Chains ========
    "cleanpro", "mydobi", "dobi queen", "mr clean laundry",
    "speed queen", "laundrybar", "pressto",
    # ======== Optical Chains ========
    "focus point", "capitol optical", "eye pro",
    "lensmart", "owndays", "better vision",
    # ======== Car Rental Chains ========
    "hertz", "avis", "europcar", "mayflower",
    "hawk rent a car", "galaxy asia", "kasina",
    # ======== Money Changers ========
    "vital rate", "max money", "merchantrade",
    # ======== Pet Store Chains ========
    "pet lovers centre", "petmart", "pet safari",
    "pet world", "pet village",
]


def _compile_blocklist(blocklist: list[str]) -> re.Pattern:
    """Compile blocklist into a single regex for O(1) matching."""
    escaped = [re.escape(p.strip().lower()) for p in blocklist if p.strip()]
    return re.compile("|".join(escaped), re.IGNORECASE)


_DEFAULT_BLOCKLIST_RE = _compile_blocklist(SKIP_LARGE_COMPANIES)


def _is_large_company(name: str, blocklist: list[str]) -> bool:
    """Check if a company name matches the large-company blocklist."""
    return bool(_DEFAULT_BLOCKLIST_RE.search(name))


def _is_large_company_custom(name: str, blocklist_re: re.Pattern) -> bool:
    """Check against a custom compiled blocklist."""
    return bool(blocklist_re.search(name))


# ============================================================
# Async helpers
# ============================================================

_BROWSER_ARGS = ["--disable-blink-features=AutomationControlled"]
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


def _run_async(coro):
    """Run an async coroutine from sync context (Streamlit-safe)."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def _new_browser_context(playwright, *, timezone: str | None = "Asia/Kuala_Lumpur"):
    """Shared browser + context factory. Caller must close browser."""
    browser = await playwright.chromium.launch(headless=True, args=_BROWSER_ARGS)
    opts: dict = {
        "viewport": {"width": 1280, "height": 900},
        "locale": "en-MY",
        "user_agent": _USER_AGENT,
    }
    if timezone:
        opts["timezone_id"] = timezone
    context = await browser.new_context(**opts)
    return browser, context


async def _run_concurrent(
    items: list,
    extract_fn,
    concurrency: int,
    progress_callback,
    progress_label: str = "Processing",
    progress_base: float = 0.0,
    progress_scale: float = 1.0,
    delay: float = 0.3,
) -> list[dict]:
    """Shared concurrent extraction with semaphore. Returns list of non-None dicts."""
    if not items:
        return []
    sem = asyncio.Semaphore(concurrency)
    completed = [0]
    skipped = [0]
    total = len(items)

    async def _worker(item):
        async with sem:
            result = await extract_fn(item)
        # Sleep OUTSIDE semaphore — don't hold slot during delay
        completed[0] += 1
        if progress_callback:
            skip_msg = f", skipped {skipped[0]}" if skipped[0] else ""
            progress_callback(
                f"{progress_label} {completed[0]}/{total} ({concurrency} tabs{skip_msg})",
                progress_base + (completed[0] / total) * progress_scale,
            )
        if delay > 0:
            await _sleep(delay)
        return result

    # Expose skipped counter for callers that need it
    _worker.skipped = skipped

    batch = await asyncio.gather(*[_worker(it) for it in items], return_exceptions=True)
    return [r for r in batch if isinstance(r, dict)]


def _clean_body_lines(body: str) -> list[str]:
    """Clean page body text into lines (strip non-ASCII, whitespace)."""
    return [re.sub(r"[^\x20-\x7E]", "", ln.strip()) for ln in body.split("\n")]


# ============================================================
# Google Maps Scraper
# ============================================================


async def _gmaps_collect_urls(
    queries: list[str], max_results: int, progress_callback,
) -> list[str]:
    """Phase 1: Collect listing URLs (sequential — requires scrolling)."""
    all_urls: list[str] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=_BROWSER_ARGS)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-MY",
            timezone_id="Asia/Kuala_Lumpur",
            user_agent=_USER_AGENT,
        )
        page = await context.new_page()

        for qi, query in enumerate(queries):
            if progress_callback:
                progress_callback(f"Searching: {query}", qi / len(queries) * 0.4)

            search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
            try:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
                await _sleep(3)
            except PlaywrightTimeout:
                logger.warning("Timeout loading search: %s", query)
                continue

            # Accept cookies dialog
            try:
                btn = await page.query_selector('button:has-text("Accept all")')
                if btn:
                    await btn.click()
                    await _sleep(1)
            except Exception:
                pass

            # Scroll results feed
            feed_sel = 'div[role="feed"]'
            try:
                await page.wait_for_selector(feed_sel, timeout=10000)
            except PlaywrightTimeout:
                continue

            prev_count = 0
            no_change = 0
            for _ in range(80):
                await page.evaluate(
                    f"document.querySelector('{feed_sel}').scrollTop = "
                    f"document.querySelector('{feed_sel}').scrollHeight"
                )
                await _sleep(1.5)
                end = (
                    await page.query_selector('p.fontBodyMedium span:has-text("end of list")')
                    or await page.query_selector("span.HlvSq")
                )
                if end:
                    break
                listings = await page.query_selector_all(
                    'div[role="feed"] > div > div > a[href*="/maps/place/"]'
                )
                if len(listings) == prev_count:
                    no_change += 1
                    if no_change >= 5:
                        break
                else:
                    no_change = 0
                prev_count = len(listings)

            links = await page.query_selector_all(
                'div[role="feed"] > div > div > a[href*="/maps/place/"]'
            )
            for link in links:
                href = await link.get_attribute("href")
                if href and href not in all_urls:
                    all_urls.append(href)

            if len(all_urls) >= max_results:
                all_urls = all_urls[:max_results]
                break

        await browser.close()

    return all_urls


async def _gmaps_extract_one(context, url: str) -> dict | None:
    """Extract details from a single Google Maps listing."""
    biz: dict[str, str] = {
        "name": "", "phone": "", "phone_type": "", "website": "",
        "address": "", "category": "", "rating": "", "reviews": "",
        "source": "google_maps", "google_maps_url": url,
    }

    page = await context.new_page()
    try:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            await _sleep(2)
        except PlaywrightTimeout:
            logger.warning("Timeout loading listing: %s", url)
            return None

        try:
            h1 = await page.query_selector("h1")
            if h1:
                biz["name"] = (await h1.inner_text()).strip()

            phone_el = await page.query_selector('a[href^="tel:"]')
            if phone_el:
                raw_href = (await phone_el.get_attribute("href")) or ""
                biz["phone"] = clean_phone(raw_href.replace("tel:", ""))
            else:
                btns = await page.query_selector_all('button[data-item-id*="phone"]')
                for btn in btns:
                    label = (await btn.get_attribute("aria-label")) or (await btn.inner_text())
                    match = PHONE_PATTERN.search(label)
                    if match:
                        biz["phone"] = clean_phone(match.group(1))
                        break
            if biz["phone"]:
                biz["phone_type"] = classify_phone(biz["phone"])

            web = await page.query_selector('a[data-item-id="authority"]')
            if web:
                biz["website"] = (await web.get_attribute("href")) or ""

            addr = await page.query_selector('button[data-item-id="address"]')
            if addr:
                biz["address"] = (
                    (await addr.get_attribute("aria-label")) or ""
                ).replace("Address: ", "")

            cat = await page.query_selector('button[jsaction*="category"]')
            if cat:
                biz["category"] = (await cat.inner_text()).strip()

            rat = await page.query_selector('div.F7nice span[aria-hidden="true"]')
            if rat:
                biz["rating"] = (await rat.inner_text()).strip()

        except Exception as exc:
            logger.warning("Error extracting details from %s: %s", url, exc)

        return biz if biz["name"] else None

    finally:
        await page.close()


def scrape_google_maps(
    queries: list[str],
    max_results: int,
    progress_callback=None,
    concurrency: int = DEFAULT_CONCURRENCY,
    skip_large: bool = False,
    skip_blocklist: list[str] | None = None,
) -> list[dict]:
    """Public API: scrape Google Maps with concurrent detail extraction."""
    blocklist_re = (
        _compile_blocklist(skip_blocklist) if skip_large and skip_blocklist
        else None
    )

    async def _run():
        urls = await _gmaps_collect_urls(queries, max_results, progress_callback)
        if not urls:
            return []

        async with async_playwright() as p:
            browser, context = await _new_browser_context(p)

            async def extract_one(url: str) -> dict | None:
                result = await _gmaps_extract_one(context, url)
                if result and blocklist_re and _is_large_company_custom(result["name"], blocklist_re):
                    return None
                return result

            results = await _run_concurrent(
                urls, extract_one, concurrency, progress_callback,
                progress_label="Extracting", progress_base=0.4, progress_scale=0.6,
            )
            await browser.close()
        return results

    return _run_async(_run())


# ============================================================
# JobStreet Scraper
# ============================================================


async def _jobstreet_collect_urls(
    locations: list[str], max_pages: int, progress_callback,
) -> list[tuple[str, str]]:
    """Phase 1: Collect company (name, url) pairs."""
    company_list: list[tuple[str, str]] = []
    seen_urls: set[str] = set()
    skip_names = {
        "write a review", "sign in", "employer site",
        "companies", "community", "",
    }

    async with async_playwright() as p:
        browser, context = await _new_browser_context(p, timezone=None)
        page = await context.new_page()

        for li, location in enumerate(locations):
            if progress_callback:
                progress_callback(f"JobStreet: {location}", li / len(locations) * 0.3)

            for pg in range(1, max_pages + 1):
                url = (
                    f"https://my.jobstreet.com/companies"
                    f"?location={location.replace(' ', '+')}&page={pg}"
                )
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    await _sleep(4)
                except PlaywrightTimeout:
                    logger.warning("Timeout loading JobStreet page: %s", url)
                    break

                links = await page.query_selector_all('a[href*="/companies/"]')
                count = 0
                for link in links:
                    href = (await link.get_attribute("href")) or ""
                    try:
                        name = re.sub(r"[^\x20-\x7E]", "", (await link.inner_text()).strip())
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
                    if href not in seen_urls:
                        seen_urls.add(href)
                        company_list.append((name, href))
                        count += 1
                if count == 0:
                    break
                await _sleep(2)

        await browser.close()

    return company_list


async def _jobstreet_extract_one(context, name: str, url: str) -> dict:
    """Extract details from a single JobStreet company page."""
    company: dict[str, str] = {
        "name": name, "phone": "", "phone_type": "",
        "website": "", "address": "", "category": "",
        "company_size": "", "source": "jobstreet", "jobstreet_url": url,
    }

    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await _sleep(3)
        body = await page.inner_text("body")
        lines = _clean_body_lines(body)

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
    finally:
        await page.close()

    return company


def scrape_jobstreet(
    locations: list[str],
    max_pages: int,
    progress_callback=None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> list[dict]:
    """Public API: scrape JobStreet with concurrent detail extraction."""

    async def _run():
        company_list = await _jobstreet_collect_urls(locations, max_pages, progress_callback)
        if not company_list:
            return []

        async with async_playwright() as p:
            browser, context = await _new_browser_context(p, timezone=None)

            async def extract_one(item: tuple[str, str]) -> dict:
                return await _jobstreet_extract_one(context, item[0], item[1])

            results = await _run_concurrent(
                company_list, extract_one, concurrency, progress_callback,
                progress_label="Details", progress_base=0.3, progress_scale=0.7,
                delay=0.5,
            )
            await browser.close()
        return results

    return _run_async(_run())


# ============================================================
# Hiredly Scraper
# ============================================================


async def _hiredly_collect_urls(max_companies: int, progress_callback) -> list[str]:
    """Phase 1: Scroll and collect company URLs."""
    company_list: list[str] = []

    async with async_playwright() as p:
        browser, context = await _new_browser_context(p, timezone=None)
        page = await context.new_page()

        if progress_callback:
            progress_callback("Loading Hiredly companies...", 0.1)

        try:
            await page.goto(
                "https://my.hiredly.com/companies",
                wait_until="domcontentloaded", timeout=20000,
            )
            await _sleep(4)
        except PlaywrightTimeout:
            logger.warning("Timeout loading Hiredly companies page")
            await browser.close()
            return company_list

        for _ in range(30):
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await _sleep(2)
            links = await page.query_selector_all('a[href*="/companies/"]')
            if len(links) >= max_companies + 5:
                break

        seen: set[str] = set()
        for link in await page.query_selector_all('a[href*="/companies/"]'):
            href = (await link.get_attribute("href")) or ""
            if "/companies/" not in href or "/companies?" in href or href in seen:
                continue
            if not href.startswith("http"):
                href = f"https://my.hiredly.com{href}"
            seen.add(href)
            company_list.append(href)
            if len(company_list) >= max_companies:
                break

        await browser.close()

    return company_list


async def _hiredly_extract_one(context, url: str) -> dict | None:
    """Extract details from a single Hiredly company page."""
    company: dict[str, str] = {
        "name": "", "phone": "", "phone_type": "",
        "website": "", "address": "", "category": "",
        "source": "hiredly", "hiredly_url": url,
    }

    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await _sleep(3)
        body = await page.inner_text("body")
        lines = [ln for ln in _clean_body_lines(body) if ln and len(ln) > 1]

        skip = {
            "Companies", "Log In", "Sign Up", "For Employers",
            "Jobs Search", "Internships", "Drop Resume", "English",
        }
        for line in lines:
            if line not in skip and 2 < len(line) < 100:
                company["name"] = line
                break

        name_found = False
        for line in lines:
            if line == company["name"]:
                name_found = True
                continue
            if name_found and len(line) < 80 and not line.endswith("jobs"):
                company["category"] = line
                break

        for j, line in enumerate(lines):
            if line == "Our Address" and j + 1 < len(lines):
                for k in range(j + 1, min(j + 5, len(lines))):
                    if lines[k] and len(lines[k]) > 10:
                        company["address"] = lines[k]
                        break
                break

        for a in await page.query_selector_all("a[href]"):
            href = (await a.get_attribute("href")) or ""
            blocked = [
                "hiredly", "google", "facebook", "linkedin",
                "twitter", "instagram", "youtube", "tiktok",
            ]
            if href.startswith("http") and not any(x in href for x in blocked):
                company["website"] = href
                break

    except Exception as exc:
        logger.warning("Error extracting Hiredly details from %s: %s", url, exc)
    finally:
        await page.close()

    return company if company["name"] else None


def scrape_hiredly(
    max_companies: int,
    progress_callback=None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> list[dict]:
    """Public API: scrape Hiredly with concurrent detail extraction."""

    async def _run():
        company_list = await _hiredly_collect_urls(max_companies, progress_callback)
        if not company_list:
            return []

        async with async_playwright() as p:
            browser, context = await _new_browser_context(p, timezone=None)

            async def extract_one(url: str) -> dict | None:
                return await _hiredly_extract_one(context, url)

            results = await _run_concurrent(
                company_list, extract_one, concurrency, progress_callback,
                progress_label="Hiredly", progress_base=0.2, progress_scale=0.8,
                delay=0.5,
            )
            await browser.close()
        return results

    return _run_async(_run())


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
            companies[key] = {**row, "sources": row.get("source", "")}
        else:
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

    for r in rows:
        r["phone_type"] = classify_phone(r.get("phone", ""))

    rows.sort(
        key=lambda r: (
            0 if r.get("phone_type") == "mobile"
            else 1 if r.get("phone_type") == "landline"
            else 2
        )
    )

    return rows


# ============================================================
# Website Phone Verification
# ============================================================


_CONTACT_SELECTOR = (
    'a[href*="contact"], a[href*="Contact"], '
    'a[href*="about-us"], a[href*="hubungi"], '
    'a:has-text("Contact"), a:has-text("Contact Us"), '
    'a:has-text("Hubungi")'
)
_SKIP_DOMAINS = frozenset([
    "facebook", "linkedin", "twitter", "instagram",
    "youtube", "tiktok", "whatsapp", "mailto:", "tel:",
])


async def _find_contact_page_url(page) -> str | None:
    """Look for a Contact Us link on the current page (single browser call)."""
    try:
        links = await page.query_selector_all(_CONTACT_SELECTOR)
    except Exception:
        return None

    for link in links:
        try:
            href = (await link.get_attribute("href")) or ""
            text = ((await link.inner_text()) or "").strip().lower()
        except Exception:
            continue
        if any(s in href.lower() for s in _SKIP_DOMAINS):
            continue
        if any(w in text for w in ("contact", "hubungi", "reach")):
            return href
        if any(w in href.lower() for w in ("contact", "hubungi")):
            return href
    return None


# Words near a number that indicate it's a fax (skip these)
_FAX_PATTERN = re.compile(r"(fax|faks|facsimile)", re.IGNORECASE)


def _is_fax_context(text: str) -> bool:
    """Check if nearby text suggests this is a fax number."""
    return bool(_FAX_PATTERN.search(text))


async def _extract_phones_from_page(page) -> list[dict]:
    """Extract phone numbers from a single page. Returns list of {phone, confidence}.
    Skips fax numbers automatically.
    """
    found: list[dict] = []
    seen: set[str] = set()

    def _add(phone: str, confidence: str) -> None:
        normalized = normalize_my_phone(phone)
        if normalized in seen or not is_valid_my_phone(normalized):
            return
        seen.add(normalized)
        found.append({"phone": normalized, "confidence": confidence})

    # Strategy 1: <a href="tel:..."> (HIGH confidence)
    # Check surrounding text for "fax" to skip fax numbers
    try:
        for link in await page.query_selector_all('a[href^="tel:"]'):
            href = (await link.get_attribute("href")) or ""
            # Check if parent/sibling text says "fax"
            nearby = ""
            try:
                parent = await link.evaluate_handle("el => el.parentElement")
                nearby = (await parent.inner_text()) if parent else ""
            except Exception:
                pass
            if _is_fax_context(nearby):
                continue
            raw = href.replace("tel:", "").replace("%20", "")
            _add(raw, "high")
    except Exception:
        pass

    # Strategy 2: WhatsApp links (HIGH confidence — common for MY SMEs)
    try:
        for link in await page.query_selector_all(
            'a[href*="wa.me"], a[href*="whatsapp"], a[href*="wasap"]'
        ):
            href = (await link.get_attribute("href")) or ""
            match = re.search(r"(\+?6?01\d{8,9})", href)
            if match:
                _add(match.group(1), "high")
    except Exception:
        pass

    # Strategy 3: Regex on visible text with context scoring
    # Skip numbers where surrounding text says "fax"
    try:
        body_text = await page.inner_text("body")
        for match in MY_PHONE_PATTERN.finditer(body_text):
            raw = match.group(1)
            start = max(0, match.start() - 60)
            end = min(len(body_text), match.end() + 20)
            context = body_text[start:end]
            # Skip fax numbers
            if _is_fax_context(context):
                continue
            confidence = "medium" if PHONE_CONTEXT_WORDS.search(context) else "low"
            _add(raw, confidence)
    except Exception:
        pass

    return found


async def _verify_one_website(context, company: dict) -> dict:
    """Visit a company website, find phone numbers.
    Stores up to 2 phones: best phone + second phone (e.g. office + mobile).
    """
    url = company["website"]
    entry: dict = {
        "id": company["id"],
        "website_phone": "",
        "website_phone_type": "",
        "website_phone2": "",
        "website_phone2_type": "",
        "website_phone_source": "",
        "all_phones": [],
    }

    page = await context.new_page()
    try:
        # Load homepage
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_load_state("networkidle", timeout=8000)
        except PlaywrightTimeout:
            pass
        except Exception:
            return entry

        await _sleep(1)
        all_phones = await _extract_phones_from_page(page)

        # Try contact page
        contact_url = await _find_contact_page_url(page)
        if contact_url:
            if not contact_url.startswith("http"):
                contact_url = urljoin(url, contact_url)
            try:
                await page.goto(contact_url, wait_until="domcontentloaded", timeout=15000)
                await _sleep(2)
                contact_phones = await _extract_phones_from_page(page)
                existing = {ph["phone"] for ph in all_phones}
                for ph in contact_phones:
                    if ph["phone"] not in existing:
                        all_phones.append(ph)
                        existing.add(ph["phone"])
                    else:
                        for a in all_phones:
                            if a["phone"] == ph["phone"] and ph["confidence"] == "high":
                                a["confidence"] = "high"
                                break
            except Exception:
                pass

        # Pick best: high > medium > low, prefer mobile
        all_phones.sort(
            key=lambda ph: (
                0 if ph["confidence"] == "high" else
                1 if ph["confidence"] == "medium" else 2,
                0 if classify_phone(ph["phone"]) == "mobile" else 1,
            )
        )

        entry["all_phones"] = [f"{ph['phone']} ({ph['confidence']})" for ph in all_phones]

        if all_phones:
            best = all_phones[0]
            entry["website_phone"] = best["phone"]
            entry["website_phone_type"] = classify_phone(best["phone"])
            entry["website_phone_source"] = best["confidence"]
            # Store second phone if available (e.g. office + mobile)
            if len(all_phones) >= 2:
                second = all_phones[1]
                entry["website_phone2"] = second["phone"]
                entry["website_phone2_type"] = classify_phone(second["phone"])

    finally:
        await page.close()

    return entry


def scrape_website_phones(
    companies: list[dict],
    progress_callback=None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> list[dict]:
    """Public API: verify phone numbers from company websites concurrently."""
    to_check = [c for c in companies if c.get("website", "").startswith("http")]
    if not to_check:
        return []

    async def _run():
        async with async_playwright() as p:
            browser, context = await _new_browser_context(p)

            async def verify_one(company: dict) -> dict:
                return await _verify_one_website(context, company)

            results = await _run_concurrent(
                to_check, verify_one, concurrency, progress_callback,
                progress_label="Checking", progress_base=0.0, progress_scale=1.0,
            )
            await browser.close()
        return results

    return _run_async(_run())
