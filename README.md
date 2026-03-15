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

## Project Structure

```
malaysia-lead-scraper/
├── app.py              # Streamlit GUI — dashboard, results, settings, history, import/export
├── scrapers.py         # Web scrapers — Google Maps, JobStreet, Hiredly (Playwright)
├── database.py         # SQLite data layer — companies, sessions, tags, settings
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
