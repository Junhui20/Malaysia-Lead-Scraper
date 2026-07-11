# Malaysia Lead Scraper

> Google Maps scraper + JobStreet scraper + Hiredly scraper — all-in-one lead generation tool for Malaysian businesses.

A Python web scraping tool with Streamlit GUI for extracting business contacts, phone numbers, emails, and company information from multiple Malaysian business directories. Built for sales teams, marketers, and business development professionals targeting KL (Kuala Lumpur) and Selangor.

## Why This Tool?

- **Multi-source scraping** — Google Maps, JobStreet, Hiredly in one click
- **Malaysian phone number detection** — auto-classifies mobile (01x) vs landline
- **Smart deduplication** — merges company data across sources, keeps the best record
- **No coding required** — full GUI with Streamlit, point-and-click operation
- **Portable** — build a self-contained Windows package, just unzip and run

## Data Sources

| Source | Data Extracted |
|--------|---------------|
| **Google Maps** | Company name, phone number, website, address, category, rating, Google Maps URL |
| **JobStreet** | Company name, website, industry, company size, location, JobStreet URL |
| **Hiredly** | Company name, website, industry, address, Hiredly URL |

## Features

### Scraping & Data Collection
- Automated Google Maps business scraper with configurable search areas
- JobStreet company directory scraper with pagination
- Hiredly company profile scraper
- Custom search queries (e.g. "restaurant in Bangsar", "IT company in Cyberjaya")
- Configurable max results and search depth

### Lead Management
- Phone number extraction and classification (mobile / landline)
- Company deduplication by normalized name (handles "Sdn Bhd", "Plt", etc.)
- Tag system for CRM-style workflow (called, interested, do not call, follow up)
- Bulk tagging for filtered results
- In-app data editor with inline editing

### Search & Filter
- Full-text search across company name, phone, address, category
- Filter by phone type (mobile only, landline only, has phone, no phone)
- Filter by data source (Google Maps, JobStreet, Hiredly)
- Filter by custom tags

### Import & Export
- Export to Excel (.xlsx) and CSV with column selection
- Import from Excel / CSV with automatic column mapping
- Duplicate detection on import
- **Delta export** — export only companies newly added since a date (fresh monthly list)

### Saved Searches & Scheduled Refresh
- Save the parameters of any scrape as a named **saved search**
- Headless CLI runner (`refresh.py`) re-runs saved searches on a schedule
- Incremental change detection: new companies get a `first_seen` timestamp; companies whose phone / email / website changed get a `last_updated` timestamp
- Export "new since date X" to Excel from the UI or the CLI

### Coverage Areas (Configurable)

**Kuala Lumpur:** KLCC, Bukit Bintang, Bangsar, Mont Kiara, Damansara Heights, Mid Valley, Cheras, Bukit Jalil, and 20+ more

**Selangor:** Petaling Jaya, Shah Alam, Subang Jaya, Cyberjaya, Puchong, Sunway, USJ, Ara Damansara, and 18+ more

## Screenshots

> _Coming soon_

## Quick Start

### Prerequisites

- Python 3.11+
- Google Chrome or Chromium

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/malaysia-lead-scraper.git
cd malaysia-lead-scraper

# Install dependencies
pip install -r requirements.txt

# Install browser for scraping
playwright install chromium
```

### Run

```bash
streamlit run app.py
```

Open `http://localhost:8501` in your browser.

### Build Portable Package (Windows)

```bash
python build_package.py
```

Creates a self-contained zip in `dist/LeadScraper/` — recipients just unzip and double-click `run.bat`. No Python installation needed.

## Scheduled Monthly Refresh

The tool can re-run your saved searches automatically and track what's new, so
you can sell a "fresh monthly list" subscription instead of a one-off export.

### 1. Save a search

Run a scrape from the **Dashboard**, then open **"Save this search for monthly
refresh"** and give it a name. Manage saved searches under the **Saved Searches**
page.

### 2. Run the headless refresh runner

`refresh.py` runs without the Streamlit UI and reuses the same scraper, dedup,
and website-verification pipeline:

```bash
python refresh.py                                  # refresh all saved searches
python refresh.py --search "PJ cafes"              # refresh one saved search
python refresh.py --list                           # list saved searches
python refresh.py --no-verify                      # skip website phone/email checks
python refresh.py --concurrency 5                  # more concurrent browser tabs

# Export just the new leads to Excel:
python refresh.py --export-new-since 2026-07-01 --output new_leads.xlsx
```

New companies get a `first_seen` timestamp; existing companies whose phone,
email, or website changed get a `last_updated` timestamp. Everything else just
has its `last_seen` bumped so it isn't re-exported as "new".

### 3. Schedule it

> **Note:** the commands below only *describe* how to schedule the runner — run
> them yourself on the target machine. Nothing is scheduled automatically.

**Windows (Task Scheduler)** — the primary deploy target (portable build). From
the unzipped package folder, create a task that runs on the 1st of each month:

```bat
schtasks /Create ^
  /TN "LeadScraper Monthly Refresh" ^
  /TR "\"C:\LeadScraper\python\python.exe\" \"C:\LeadScraper\refresh.py\"" ^
  /SC MONTHLY /D 1 /ST 02:00
```

Or use the Task Scheduler GUI: **Create Basic Task -> Monthly**, action **Start a
program**, program `python\python.exe` (bundled), argument `refresh.py`, "Start
in" set to the package folder. For a source install, point the program at your
`python.exe` and the script at the repo's `refresh.py`.

**Linux (cron)** — run at 02:00 on the 1st of every month (`crontab -e`):

```cron
0 2 1 * * cd "/path/to/Malaysia Lead Scraper" && /usr/bin/python3 refresh.py >> refresh.log 2>&1
```

## Project Structure

```
malaysia-lead-scraper/
├── app.py              # Streamlit GUI — dashboard, results, saved searches, settings, history, import/export
├── refresh.py          # Headless CLI runner — scheduled incremental refresh + delta export
├── scrapers.py         # Web scrapers — Google Maps, JobStreet, Hiredly (Playwright)
├── database.py         # SQLite data layer — companies, sessions, saved searches, tags, settings
├── utils.py            # Phone classification, name normalization, validation
├── build_package.py    # Windows portable package builder
├── requirements.txt    # Python dependencies
├── .streamlit/
│   └── config.toml     # Streamlit theme & server config
├── .gitignore
├── LICENSE
└── README.md
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11+ |
| GUI | Streamlit |
| Web Scraping | Playwright (headless Chromium) |
| Database | SQLite (WAL mode) |
| Data Processing | Pandas |
| Export | openpyxl (Excel), CSV |

## Disclaimer

This tool is for educational and legitimate business research purposes only. Please respect the terms of service of the websites being scraped. Use responsibly and comply with local data protection laws (PDPA Malaysia).

## Keywords

`google-maps-scraper` `lead-generation` `web-scraping` `business-leads` `malaysia` `kuala-lumpur` `selangor` `phone-number-extractor` `jobstreet-scraper` `company-directory` `streamlit` `playwright` `python-scraper` `b2b-leads` `sales-prospecting` `data-extraction` `business-directory-scraper` `contact-scraper` `crm-tool` `lead-management`

## License

MIT
