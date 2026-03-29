"""
Lead Scraper — Professional GUI
Multi-page Streamlit app for scraping and managing KL & Selangor business leads.
"""

import asyncio
import html as html_mod
import io
import json
import os
import sys
import time
from pathlib import Path

# Windows event loop fix for Playwright subprocess support
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Playwright browser path for packaged app
_app_dir = Path(__file__).parent
_browsers_dir = _app_dir / "browsers"
if _browsers_dir.exists():
    os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(_browsers_dir)

import pandas as pd
import streamlit as st

from database import (
    init_db, get_all_companies, save_companies, create_session,
    update_companies_from_df, deduplicate_companies,
    get_sessions, delete_session, get_session_companies,
    get_tags, add_tag, delete_tag,
    get_setting, save_setting, import_companies_from_df,
    get_company_count, bulk_append_tag, MAX_IMPORT_ROWS,
    update_website_phones, get_companies_with_website,
)
from scrapers import (
    scrape_google_maps, scrape_jobstreet, scrape_hiredly,
    scrape_website_phones,
    merge_results, KL_AREAS, SELANGOR_AREAS, BUSINESS_KEYWORDS,
    SKIP_LARGE_COMPANIES,
)

# ============================================================
# Page Config & CSS
# ============================================================

st.set_page_config(
    page_title="Lead Scraper",
    page_icon="📇",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Rounded:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" rel="stylesheet">
<style>
    /* Force Material Symbols font for Streamlit icons */
    .exvv1vr0, .emntfgb2 {
        font-family: 'Material Symbols Rounded' !important;
        font-size: 20px !important;
        -webkit-font-smoothing: antialiased;
        direction: ltr;
    }
    /* Hide sidebar collapse button (renders "keyboard_double_arrow_left" as text) */
    button[kind="headerNoPadding"] {
        display: none !important;
    }
    button[kind="headerNoPadding"] * {
        display: none !important;
    }
    /* ============================================
       Purity UI Dashboard Theme
       Based on Creative Tim Purity UI
       ============================================ */

    :root {
        /* Purity UI palette */
        --teal-300: #4FD1C5;
        --teal-400: #38B2AC;
        --teal-500: #319795;
        --gray-50:  #F7FAFC;
        --gray-100: #EDF2F7;
        --gray-200: #E2E8F0;
        --gray-300: #CBD5E0;
        --gray-400: #A0AEC0;
        --gray-500: #718096;
        --gray-600: #4A5568;
        --gray-700: #2D3748;
        --gray-800: #1A202C;
        --card-shadow: 0px 3.5px 5.5px rgba(0, 0, 0, 0.02);
        --card-radius: 15px;
    }

    /* ---- Global ---- */
    html, body, [class*="st-"] {
        font-family: 'DM Sans', 'Helvetica Neue', Helvetica, Arial, sans-serif !important;
    }

    /* Hide Streamlit chrome */
    #MainMenu, footer, header,
    .stDeployButton, [data-testid="stToolbar"] {
        display: none !important;
    }

    /* Page background */
    .stApp, .stMain {
        background: var(--gray-50) !important;
    }

    /* Main container */
    .block-container {
        padding: 24px 30px 20px !important;
        max-width: 1200px;
    }

    /* ---- Typography ---- */
    h1 {
        font-size: 1.75rem !important;
        font-weight: 700 !important;
        color: var(--gray-700) !important;
        letter-spacing: -0.02em !important;
        margin-bottom: 16px !important;
    }
    h2 {
        font-size: 1.125rem !important;
        font-weight: 700 !important;
        color: var(--gray-700) !important;
        margin-top: 20px !important;
        margin-bottom: 8px !important;
    }
    h3 {
        font-size: 1rem !important;
        font-weight: 700 !important;
        color: var(--gray-700) !important;
    }
    p, li, label, .stMarkdown {
        color: var(--gray-600) !important;
        line-height: 1.6 !important;
        font-size: 0.875rem !important;
    }

    /* ---- Sidebar (Purity style: white bg, rounded items) ---- */
    section[data-testid="stSidebar"] {
        background: white !important;
        border-right: none !important;
        box-shadow: none !important;
    }
    section[data-testid="stSidebar"] * {
        color: var(--gray-600) !important;
    }
    /* Nav title */
    .nav-title {
        font-size: 0.875rem;
        font-weight: 700;
        color: var(--gray-700) !important;
        letter-spacing: -0.01em;
        margin-bottom: 2px;
    }
    .nav-subtitle {
        font-size: 0.65rem;
        color: var(--gray-400) !important;
        margin-bottom: 20px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 700;
    }
    .db-count {
        font-size: 0.75rem;
        color: var(--gray-400) !important;
        padding: 8px 0;
        font-variant-numeric: tabular-nums;
    }
    /* Sidebar radio: Purity style active = white bg + teal icon */
    section[data-testid="stSidebar"] .stRadio label {
        color: var(--gray-400) !important;
        font-weight: 700 !important;
        font-size: 0.8rem !important;
        padding: 10px 14px !important;
        border-radius: var(--card-radius) !important;
        transition: all 0.2s ease;
        margin-bottom: 4px !important;
    }
    section[data-testid="stSidebar"] .stRadio label:hover {
        background: var(--gray-50) !important;
    }
    section[data-testid="stSidebar"] .stRadio label[data-checked="true"],
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label[aria-checked="true"] {
        background: white !important;
        color: var(--gray-700) !important;
        box-shadow: 0px 3.5px 5.5px rgba(0, 0, 0, 0.06) !important;
    }
    /* Sidebar divider */
    section[data-testid="stSidebar"] hr {
        border-color: var(--gray-200) !important;
    }

    /* ---- Cards / Metrics (Purity style) ---- */
    [data-testid="stMetric"] {
        background: white;
        border: none !important;
        border-radius: var(--card-radius);
        padding: 18px 22px !important;
        box-shadow: var(--card-shadow);
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.7rem !important;
        font-weight: 700 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.05em !important;
        color: var(--gray-400) !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.5rem !important;
        font-weight: 700 !important;
        color: var(--gray-700) !important;
        font-variant-numeric: tabular-nums !important;
    }

    /* ---- Buttons (Purity: teal primary, rounded) ---- */
    .stButton > button {
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 700 !important;
        font-size: 0.75rem !important;
        border-radius: 12px !important;
        padding: 8px 24px !important;
        transition: all 0.2s ease !important;
        border: none !important;
        background: var(--gray-50) !important;
        color: var(--gray-600) !important;
        box-shadow: none !important;
        text-transform: uppercase;
        letter-spacing: 0.02em;
    }
    .stButton > button:hover {
        background: var(--gray-100) !important;
        box-shadow: var(--card-shadow) !important;
    }
    .stButton > button[kind="primary"],
    .stButton > button[data-testid="stBaseButton-primary"] {
        background: var(--teal-300) !important;
        color: white !important;
    }
    .stButton > button[kind="primary"]:hover,
    .stButton > button[data-testid="stBaseButton-primary"]:hover {
        background: var(--teal-500) !important;
    }

    /* ---- Inputs (Purity: rounded, clean) ---- */
    .stTextInput input, .stTextArea textarea, .stSelectbox select {
        font-family: 'DM Sans', sans-serif !important;
        border-radius: var(--card-radius) !important;
        border: 1px solid var(--gray-200) !important;
        font-size: 0.875rem !important;
        padding: 10px 16px !important;
        background: white !important;
    }
    .stTextInput input:focus, .stTextArea textarea:focus {
        border-color: var(--teal-300) !important;
        box-shadow: 0 0 0 3px rgba(79,209,197,0.15) !important;
    }

    /* ---- Data editor / table ---- */
    [data-testid="stDataFrame"],
    .stDataEditor {
        border-radius: var(--card-radius) !important;
        border: none !important;
        box-shadow: var(--card-shadow);
        overflow: hidden;
    }

    /* ---- Tabs (Purity style) ---- */
    .stTabs [data-baseweb="tab"] {
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 700 !important;
        font-size: 0.8rem !important;
        color: var(--gray-400) !important;
        padding: 8px 16px !important;
    }
    .stTabs [aria-selected="true"] {
        color: var(--teal-500) !important;
        border-bottom-color: var(--teal-300) !important;
    }

    /* ---- Progress bar (teal gradient) ---- */
    .stProgress > div > div {
        background: linear-gradient(90deg, var(--teal-300), var(--teal-500)) !important;
        border-radius: 8px !important;
    }
    .stProgress > div {
        background: var(--gray-100) !important;
        border-radius: 8px !important;
    }

    /* ---- Tags ---- */
    .tag-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 0.7rem;
        font-weight: 700;
        color: white;
        letter-spacing: 0.02em;
    }

    /* ---- Dividers ---- */
    hr {
        border: none !important;
        border-top: 1px solid var(--gray-200) !important;
        margin: 20px 0 !important;
    }

    /* ---- Alerts ---- */
    .stAlert {
        border-radius: var(--card-radius) !important;
        font-size: 0.875rem !important;
    }

    /* ---- Checkbox (teal accent) ---- */
    .stCheckbox label {
        font-weight: 500 !important;
        font-size: 0.875rem !important;
    }

    /* ---- Caption ---- */
    .stCaption, [data-testid="stCaptionContainer"] {
        color: var(--gray-400) !important;
        font-size: 0.75rem !important;
    }

    /* ---- Multiselect / Select chips ---- */
    .stMultiSelect span[data-baseweb="tag"] {
        background: var(--teal-300) !important;
        border-radius: 8px !important;
    }

    /* ---- Slider (teal) ---- */
    .stSlider div[data-baseweb="slider"] div[role="slider"] {
        background: var(--teal-300) !important;
    }
</style>
""", unsafe_allow_html=True)


# Initialize database
init_db()


# ============================================================
# Sidebar Navigation
# ============================================================

with st.sidebar:
    st.markdown(
        '<div style="padding:8px 0 4px">'
        '<div class="nav-title">LEAD SCRAPER</div>'
        '<div class="nav-subtitle">KL &amp; Selangor Business Leads</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.divider()

    page = st.radio(
        "Navigate",
        [
            "Dashboard",
            "Results",
            "Settings",
            "History",
            "Import / Export",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    count = get_company_count()
    st.markdown(
        f'<div class="db-count">{count:,} companies in database</div>',
        unsafe_allow_html=True,
    )


# ============================================================
# Shared helpers
# ============================================================

def _get_concurrency() -> int:
    return int(get_setting("concurrency", "3"))


def _no_phone_mask(df: pd.DataFrame) -> pd.Series:
    """Boolean mask: True where phone is missing or empty."""
    return df["phone"].isna() | (df["phone"].astype(str).str.strip() == "")


def _apply_phone_filter(df: pd.DataFrame, filter_value: str) -> pd.DataFrame:
    """Apply a phone-type filter and return filtered DataFrame."""
    if filter_value in ("Mobile (01x)", "Mobile only"):
        return df[df["phone_type"] == "mobile"]
    if filter_value in ("Landline (0x)", "Landline only"):
        return df[df["phone_type"] == "landline"]
    if filter_value in ("Has Phone", "Has phone"):
        return df[~_no_phone_mask(df)]
    if filter_value in ("No Phone", "No phone"):
        return df[_no_phone_mask(df)]
    return df


# ============================================================
# Page: Dashboard
# ============================================================

def _run_scraping(queries: list[str], gm_max: int, use_gmaps: bool,
                  use_jobstreet: bool, js_locations: list[str] | None,
                  js_pages: int, use_hiredly: bool, hi_max: int,
                  concurrency: int, skip_large: bool = False,
                  skip_blocklist: list[str] | None = None):
    """Execute scraping with progress display."""
    all_results: list[dict] = []
    progress = st.progress(0)
    status = st.empty()

    def update_progress(msg, pct):
        status.text(msg)
        progress.progress(min(pct, 1.0))

    sources_used: list[str] = []

    if use_gmaps and queries:
        sources_used.append("google_maps")
        status.text(f"Google Maps: {len(queries)} queries ({concurrency} tabs)...")
        gm_results = scrape_google_maps(
            queries, gm_max, update_progress, concurrency,
            skip_large=skip_large, skip_blocklist=skip_blocklist,
        )
        all_results.extend(gm_results)
        status.text(f"Google Maps: {len(gm_results)} companies found")

    if use_jobstreet and js_locations:
        sources_used.append("jobstreet")
        status.text("Scraping JobStreet...")
        js_results = scrape_jobstreet(js_locations, js_pages, update_progress, concurrency)
        all_results.extend(js_results)
        status.text(f"JobStreet: {len(js_results)} companies found")

    if use_hiredly:
        sources_used.append("hiredly")
        status.text("Scraping Hiredly...")
        hi_results = scrape_hiredly(hi_max, update_progress, concurrency)
        all_results.extend(hi_results)
        status.text(f"Hiredly: {len(hi_results)} companies found")

    if all_results:
        merged = merge_results(all_results)
        areas_count = len(queries) if use_gmaps else 0
        session_id = create_session(
            ", ".join(sources_used),
            f"Queries: {areas_count} | Concurrency: {concurrency}",
            len(merged),
        )
        save_companies(merged, session_id)
        progress.progress(1.0)

        total = len(merged)
        mobile = sum(1 for c in merged if c.get("phone_type") == "mobile")
        landline = sum(1 for c in merged if c.get("phone_type") == "landline")

        st.success(f"Done! {total} unique companies saved to database.")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total", total)
        c2.metric("Mobile", mobile)
        c3.metric("Landline", landline)
        c4.metric("No Phone", total - mobile - landline)
    else:
        st.warning("No results found. Try different settings.")


def page_dashboard():
    st.header("Dashboard")
    concurrency = _get_concurrency()
    blocklist = json.loads(get_setting("skip_blocklist", json.dumps(SKIP_LARGE_COMPANIES)))

    # ---- Quick Search ----
    st.subheader("Quick Search")
    st.caption("Google Maps search — type what you want, e.g. 'restaurant in Bangsar', 'IT company Cyberjaya'")

    quick_query = st.text_input(
        "Search query",
        placeholder="restaurant in Bangsar, IT company in Cyberjaya",
        label_visibility="collapsed",
        key="quick_search",
    )
    quick_go = st.button("Search", type="primary", use_container_width=True, key="quick_go")

    skip_large_quick = st.checkbox(
        "Skip large / chain companies",
        value=True,
        key="skip_large_quick",
        help="Skip banks, fast food, telcos, MNCs, convenience stores, etc. Edit the list in Settings > Blocklist.",
    )

    if quick_go and quick_query.strip():
        queries = [q.strip() for q in quick_query.split(",") if q.strip()]
        _run_scraping(
            queries=queries, gm_max=500,
            use_gmaps=True, use_jobstreet=False,
            js_locations=None, js_pages=0,
            use_hiredly=False, hi_max=0,
            concurrency=concurrency,
            skip_large=skip_large_quick, skip_blocklist=blocklist,
        )
        return

    # ---- Advanced Options ----
    st.divider()
    show_advanced = st.checkbox("Show Advanced Options", key="show_adv")
    if show_advanced:
        # Source selection
        st.subheader("Data Sources")
        src1, src2, src3 = st.columns(3)
        with src1:
            use_gmaps = st.checkbox("Google Maps", value=True, help="Best for phone numbers")
        with src2:
            use_jobstreet = st.checkbox("JobStreet", help="Companies actively hiring")
        with src3:
            use_hiredly = st.checkbox("Hiredly", help="Malaysian hiring platform")

        # Concurrency
        st.subheader("Speed")
        concurrency = st.slider(
            "Concurrent Tabs",
            min_value=1, max_value=10, value=concurrency,
            help="Number of browser tabs working simultaneously. "
                 "Higher = faster but uses more RAM. Recommended: 3-5.",
            key="dash_concurrency",
        )

        # Filter
        st.subheader("Filter")
        skip_large_adv = st.checkbox(
            "Skip large/chain companies (banks, fast food, telcos, MNCs, etc.)",
            value=True,
            key="skip_large_adv",
        )

        # Google Maps options
        gm_areas_kl = []
        gm_areas_sel = []
        gm_keywords = []
        gm_custom = ""
        gm_max = 500
        if use_gmaps:
            st.subheader("Search Areas")
            area1, area2 = st.columns(2)

            kl_areas = json.loads(get_setting("kl_areas", json.dumps(KL_AREAS)))
            sel_areas = json.loads(get_setting("selangor_areas", json.dumps(SELANGOR_AREAS)))

            with area1:
                kl_all = st.checkbox("Select All KL")
                gm_areas_kl = st.multiselect(
                    "KL Areas", kl_areas,
                    default=kl_areas if kl_all else kl_areas[:5],
                )
            with area2:
                sel_all = st.checkbox("Select All Selangor")
                gm_areas_sel = st.multiselect(
                    "Selangor Areas", sel_areas,
                    default=sel_areas if sel_all else sel_areas[:5],
                )

            st.subheader("Keywords")
            keywords = json.loads(get_setting("keywords", json.dumps(BUSINESS_KEYWORDS)))
            gm_keywords = st.multiselect("Business Types", keywords, default=["company"])
            gm_custom = st.text_area(
                "Custom Queries (one per line)",
                placeholder="restaurant in Bangsar\nIT company in Cyberjaya",
            )

            default_max = int(get_setting("default_max_results", "500"))
            gm_max = st.slider("Max Results", 50, 10000, default_max, step=50)

        # JobStreet options
        js_locations = ["Kuala Lumpur", "Selangor"]
        js_pages = 5
        if use_jobstreet:
            st.subheader("JobStreet")
            js_locations = st.multiselect(
                "Locations",
                ["Kuala Lumpur", "Selangor", "Penang", "Johor"],
                default=["Kuala Lumpur", "Selangor"],
            )
            js_pages = st.slider("Max Pages per Location", 1, 50, 5)

        # Hiredly options
        hi_max = 50
        if use_hiredly:
            st.subheader("Hiredly")
            hi_max = st.slider("Max Companies", 10, 500, 50, step=10)

        # Start Scraping
        st.divider()
        if st.button("Start Scraping", type="primary", use_container_width=True):
            queries: list[str] = []
            if use_gmaps:
                for kw in gm_keywords:
                    for area in gm_areas_kl + gm_areas_sel:
                        queries.append(f"{kw} in {area}")
                if gm_custom:
                    queries.extend(
                        q.strip() for q in gm_custom.strip().splitlines() if q.strip()
                    )

            _run_scraping(
                queries=queries, gm_max=gm_max,
                use_gmaps=use_gmaps, use_jobstreet=use_jobstreet,
                js_locations=js_locations, js_pages=js_pages,
                use_hiredly=use_hiredly, hi_max=hi_max,
                concurrency=concurrency,
                skip_large=skip_large_adv, skip_blocklist=blocklist,
            )


# ============================================================
# Page: Results
# ============================================================

def page_results():
    st.header("Results Database")

    df = get_all_companies()
    if df.empty:
        st.info("No data yet. Go to **Dashboard** to start scraping, or **Import** data.")
        return

    # --- Filters ---
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        search = st.text_input("Search", placeholder="Name, phone, address...")
    with f2:
        phone_filter = st.selectbox(
            "Phone Type", ["All", "Mobile (01x)", "Landline (0x)", "Has Phone", "No Phone"]
        )
    with f3:
        source_options = sorted({
            s.strip()
            for sources in df["sources"].dropna()
            for s in str(sources).split(",")
            if s.strip()
        })
        source_filter = st.selectbox("Source", ["All"] + source_options)
    with f4:
        tags_list = [t["name"] for t in get_tags()]
        tag_filter = st.selectbox("Tag", ["All"] + tags_list)

    # Apply filters
    filtered = df.copy()
    if search:
        q = search.lower()
        searchable = ["name", "phone", "address", "category", "website", "tags", "notes"]
        mask = pd.Series(False, index=filtered.index)
        for col in searchable:
            if col in filtered.columns:
                mask = mask | filtered[col].astype(str).str.lower().str.contains(q, na=False)
        filtered = filtered[mask]
    filtered = _apply_phone_filter(filtered, phone_filter)
    if source_filter != "All":
        filtered = filtered[
            filtered["sources"].astype(str).str.contains(source_filter, na=False)
        ]
    if tag_filter != "All":
        filtered = filtered[
            filtered["tags"].astype(str).str.contains(tag_filter, na=False)
        ]

    # --- Stats ---
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Showing", f"{len(filtered):,}")
    c2.metric("Mobile", len(filtered[filtered["phone_type"] == "mobile"]))
    c3.metric("Landline", len(filtered[filtered["phone_type"] == "landline"]))
    c4.metric("No Phone", int(_no_phone_mask(filtered).sum()))

    # --- Column visibility ---
    all_cols = [
        "name", "phone", "phone_type", "website_phone", "website_phone2",
        "website", "address", "category", "tags", "notes", "sources",
    ]
    default_vis = ["name", "phone", "phone_type", "website_phone", "website_phone2", "website", "address", "category", "tags", "sources"]
    visible = st.multiselect(
        "Visible Columns", all_cols, default=default_vis, key="vis_cols"
    )
    if not visible:
        visible = default_vis

    # --- Data Editor ---
    display_cols = ["id"] + [c for c in visible if c in filtered.columns]
    display_df = filtered[display_cols].reset_index(drop=True)

    edited = st.data_editor(
        display_df,
        use_container_width=True,
        height=500,
        hide_index=True,
        disabled=["id", "sources"],
        column_config={
            "id": st.column_config.NumberColumn("ID", width="small"),
            "name": st.column_config.TextColumn("Company", width="large"),
            "phone": st.column_config.TextColumn("Phone (Maps)", width="medium"),
            "phone_type": st.column_config.SelectboxColumn(
                "Type", options=["mobile", "landline", ""], width="small"
            ),
            "website_phone": st.column_config.TextColumn("Phone (Website)", width="medium"),
            "website_phone2": st.column_config.TextColumn("Phone 2 (Website)", width="medium"),
            "website": st.column_config.LinkColumn("Website", width="medium"),
            "address": st.column_config.TextColumn("Address", width="large"),
            "category": st.column_config.TextColumn("Industry", width="medium"),
            "tags": st.column_config.TextColumn("Tags", width="medium"),
            "notes": st.column_config.TextColumn("Notes", width="large"),
            "sources": st.column_config.TextColumn("Sources", width="small"),
        },
        key="results_editor",
    )

    # --- Action buttons ---
    st.divider()
    a1, a2, a3, a4 = st.columns(4)

    with a1:
        if st.button("Save Changes", type="primary", use_container_width=True):
            n = update_companies_from_df(edited)
            st.success(f"Saved {n} rows")
            time.sleep(0.5)
            st.rerun()

    with a2:
        if st.button("Remove Duplicates", use_container_width=True):
            removed = deduplicate_companies()
            st.success(f"Removed {removed} duplicate(s)")
            time.sleep(0.5)
            st.rerun()

    with a3:
        tag_to_add = st.selectbox(
            "Quick Tag", ["(select)"] + tags_list, key="bulk_tag",
            label_visibility="collapsed",
        )

    with a4:
        if st.button("Tag All Visible", use_container_width=True):
            if tag_to_add and tag_to_add != "(select)":
                ids = [int(r["id"]) for _, r in filtered.iterrows()]
                tagged = bulk_append_tag(ids, tag_to_add)
                st.success(f"Tagged {tagged} companies with '{tag_to_add}'")
                time.sleep(0.5)
                st.rerun()

    # --- Website Phone Verification ---
    st.divider()
    st.subheader("Verify Phones from Company Websites")
    st.caption(
        "Visit each company's website to find phone numbers. "
        "Results are stored in the 'Phone (Website)' column for comparison."
    )

    companies_with_web = get_companies_with_website()
    already_verified = sum(1 for c in companies_with_web if c.get("website_phone"))
    concurrency = _get_concurrency()

    v1, v2, v3 = st.columns(3)
    v1.metric("Has Website", len(companies_with_web))
    v2.metric("Already Verified", already_verified)
    v3.metric("Not Yet Verified", len(companies_with_web) - already_verified)

    verify_scope = st.radio(
        "Verify scope",
        ["Not yet verified only", "All with website (re-check)"],
        horizontal=True,
        key="verify_scope",
    )

    if st.button(
        "Start Website Verification",
        type="primary",
        use_container_width=True,
        key="verify_btn",
    ):
        to_verify = (
            [c for c in companies_with_web if not c.get("website_phone")]
            if "Not yet" in verify_scope
            else companies_with_web
        )

        if not to_verify:
            st.info("No companies to verify.")
        else:
            progress = st.progress(0)
            status = st.empty()

            def verify_progress(msg, pct):
                status.text(msg)
                progress.progress(min(pct, 1.0))

            results = scrape_website_phones(to_verify, verify_progress, concurrency)
            saved = update_website_phones(results)
            progress.progress(1.0)

            found = sum(1 for r in results if r.get("website_phone"))
            st.success(
                f"Done! Checked {len(results)} websites. "
                f"Found phones on {found}. Updated {saved} records."
            )
            time.sleep(1)
            st.rerun()


# ============================================================
# Page: Settings
# ============================================================

def page_settings():
    st.header("Settings")

    tab_areas, tab_keywords, tab_defaults, tab_blocklist, tab_tags = st.tabs(
        ["Search Areas", "Keywords", "Defaults", "Blocklist", "Tags"]
    )

    # --- Search Areas ---
    with tab_areas:
        st.subheader("KL Areas")
        kl_current = json.loads(get_setting("kl_areas", json.dumps(KL_AREAS)))
        kl_text = st.text_area(
            "One area per line",
            value="\n".join(kl_current),
            height=250,
            key="kl_areas_edit",
        )
        if st.button("Save KL Areas"):
            areas = [a.strip() for a in kl_text.strip().splitlines() if a.strip()]
            save_setting("kl_areas", json.dumps(areas))
            st.success(f"Saved {len(areas)} KL areas")

        st.divider()

        st.subheader("Selangor Areas")
        sel_current = json.loads(get_setting("selangor_areas", json.dumps(SELANGOR_AREAS)))
        sel_text = st.text_area(
            "One area per line",
            value="\n".join(sel_current),
            height=250,
            key="sel_areas_edit",
        )
        if st.button("Save Selangor Areas"):
            areas = [a.strip() for a in sel_text.strip().splitlines() if a.strip()]
            save_setting("selangor_areas", json.dumps(areas))
            st.success(f"Saved {len(areas)} Selangor areas")

    # --- Keywords ---
    with tab_keywords:
        st.subheader("Business Keywords")
        kw_current = json.loads(get_setting("keywords", json.dumps(BUSINESS_KEYWORDS)))
        kw_text = st.text_area(
            "One keyword per line",
            value="\n".join(kw_current),
            height=300,
            key="kw_edit",
        )
        if st.button("Save Keywords"):
            kws = [k.strip() for k in kw_text.strip().splitlines() if k.strip()]
            save_setting("keywords", json.dumps(kws))
            st.success(f"Saved {len(kws)} keywords")

    # --- Defaults ---
    with tab_defaults:
        st.subheader("Default Settings")
        default_max = int(get_setting("default_max_results", "500"))
        new_max = st.number_input("Default Max Results", 50, 10000, default_max, step=50)

        st.divider()
        st.subheader("Concurrency")
        st.caption(
            "Number of browser tabs working simultaneously. "
            "Higher = faster but uses more RAM. Recommended: 3-5."
        )
        current_conc = _get_concurrency()
        new_conc = st.slider("Concurrent Tabs", 1, 10, current_conc, key="settings_conc")

        if st.button("Save Defaults"):
            save_setting("default_max_results", str(new_max))
            save_setting("concurrency", str(new_conc))
            st.success("Defaults saved")

    # --- Blocklist ---
    with tab_blocklist:
        st.subheader("Skip Large/Chain Companies")
        st.caption(
            "Companies matching these keywords will be skipped during Google Maps scraping. "
            "One keyword per line. Matching is case-insensitive (partial match)."
        )
        bl_current = json.loads(get_setting("skip_blocklist", json.dumps(SKIP_LARGE_COMPANIES)))
        bl_text = st.text_area(
            "Blocklist (one per line)",
            value="\n".join(bl_current),
            height=400,
            key="blocklist_edit",
        )
        bl1, bl2 = st.columns(2)
        with bl1:
            if st.button("Save Blocklist"):
                items = [b.strip() for b in bl_text.strip().splitlines() if b.strip()]
                save_setting("skip_blocklist", json.dumps(items))
                st.success(f"Saved {len(items)} blocklist entries")
        with bl2:
            if st.button("Reset to Default"):
                save_setting("skip_blocklist", json.dumps(SKIP_LARGE_COMPANIES))
                st.success("Reset to default blocklist")
                time.sleep(0.5)
                st.rerun()

    # --- Tags ---
    with tab_tags:
        st.subheader("Custom Tags")
        st.caption("Manage tags for labeling companies (e.g. called, interested, do not call)")

        tags = get_tags()
        if tags:
            for tag in tags:
                t1, t2, t3 = st.columns([3, 1, 1])
                with t1:
                    safe_name = html_mod.escape(tag["name"])
                    st.markdown(
                        f'<span class="tag-badge" style="background:{tag["color"]}">'
                        f'{safe_name}</span>',
                        unsafe_allow_html=True,
                    )
                with t2:
                    st.color_picker(
                        "Color", tag["color"], key=f"tc_{tag['id']}",
                        label_visibility="collapsed",
                    )
                with t3:
                    if st.button("Delete", key=f"td_{tag['id']}"):
                        delete_tag(tag["name"])
                        st.rerun()
        else:
            st.info("No tags yet. Add one below.")

        st.divider()
        nc1, nc2, nc3 = st.columns([3, 1, 1])
        with nc1:
            new_tag_name = st.text_input("New Tag Name", key="new_tag")
        with nc2:
            new_tag_color = st.color_picker("Color", "#6B7280", key="new_tag_color")
        with nc3:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Add Tag") and new_tag_name:
                if add_tag(new_tag_name.strip(), new_tag_color):
                    st.success(f"Added '{new_tag_name}'")
                    st.rerun()
                else:
                    st.warning("Tag already exists")


# ============================================================
# Page: History
# ============================================================

def page_history():
    st.header("Scraping History")

    sessions = get_sessions()
    if not sessions:
        st.info("No scraping sessions yet. Go to **Dashboard** to start.")
        return

    for s in sessions:
        st.markdown(
            f'<div style="background:white;border-radius:15px;padding:16px 22px;'
            f'margin-bottom:12px;box-shadow:0px 3.5px 5.5px rgba(0,0,0,0.02)">'
            f'<div style="font-weight:700;font-size:0.9rem;color:#2D3748">'
            f'{html_mod.escape(s["sources"])} &mdash; {s["result_count"]} results</div>'
            f'<div style="font-size:0.75rem;color:#A0AEC0;margin-top:4px">'
            f'{s["created_at"][:16]} &nbsp; | &nbsp; {html_mod.escape(s["query_info"])}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

        h1, h2, h3 = st.columns([2, 2, 1])
        with h1:
            if st.button("View Details", key=f"load_{s['id']}", use_container_width=True):
                st.session_state["history_session_id"] = s["id"]
                df = get_session_companies(s["id"])
                st.dataframe(
                    df[["name", "phone", "phone_type", "website", "address", "category"]],
                    use_container_width=True,
                    height=300,
                )
        with h3:
            if st.button("Delete", key=f"del_{s['id']}", use_container_width=True):
                deleted = delete_session(s["id"])
                st.success(f"Deleted session and {deleted} companies")
                time.sleep(0.5)
                st.rerun()


# ============================================================
# Page: Import / Export
# ============================================================

def page_import_export():
    st.header("Import / Export")

    tab_export, tab_import = st.tabs(["Export", "Import"])

    # --- Export ---
    with tab_export:
        st.subheader("Export Data")
        df = get_all_companies()

        if df.empty:
            st.info("No data to export.")
        else:
            # Filter options
            st.caption("Apply filters before exporting")
            ex_phone = st.selectbox(
                "Phone Filter",
                ["All", "Mobile only", "Landline only", "Has phone", "No phone"],
                key="ex_phone",
            )

            export_df = _apply_phone_filter(df.copy(), ex_phone)

            # Column selection
            export_cols = [
                "name", "phone", "phone_type", "website_phone", "website",
                "address", "category", "company_size", "tags", "notes", "sources",
            ]
            selected_cols = st.multiselect(
                "Columns to export", export_cols, default=export_cols[:9], key="ex_cols"
            )

            st.metric("Rows to export", f"{len(export_df):,}")

            e1, e2 = st.columns(2)
            with e1:
                # Excel export
                excel_buf = io.BytesIO()
                export_df[selected_cols].to_excel(
                    excel_buf, index=False, engine="openpyxl"
                )
                st.download_button(
                    "Download Excel (.xlsx)",
                    data=excel_buf.getvalue(),
                    file_name="leads.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

            with e2:
                # CSV export
                csv_data = export_df[selected_cols].to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "Download CSV",
                    data=csv_data,
                    file_name="leads.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

    # --- Import ---
    with tab_import:
        st.subheader("Import Data")
        st.caption(
            "Upload an Excel (.xlsx) or CSV file. "
            "Expects columns: name/Company, phone, website, address, category/Industry. "
            "Duplicates are skipped automatically."
        )

        uploaded = st.file_uploader(
            "Choose file", type=["xlsx", "xls", "csv"], key="import_file"
        )

        if uploaded is not None:
            try:
                if uploaded.name.endswith(".csv"):
                    import_df = pd.read_csv(uploaded)
                else:
                    import_df = pd.read_excel(uploaded, engine="openpyxl")

                if len(import_df) > MAX_IMPORT_ROWS:
                    st.warning(f"File has {len(import_df):,} rows. Only the first {MAX_IMPORT_ROWS:,} will be imported.")
                    import_df = import_df.head(MAX_IMPORT_ROWS)
                st.write(f"**Preview:** {len(import_df)} rows, {len(import_df.columns)} columns")
                st.dataframe(import_df.head(10), use_container_width=True, height=300)

                if st.button("Import Data", type="primary"):
                    imported, skipped = import_companies_from_df(import_df)
                    st.success(
                        f"Imported {imported} new companies. "
                        f"Skipped {skipped} (duplicates or empty)."
                    )
            except Exception as e:
                st.error(f"Error reading file: {e}")


# ============================================================
# Router
# ============================================================

if "Dashboard" in page:
    page_dashboard()
elif "Results" in page:
    page_results()
elif "Settings" in page:
    page_settings()
elif "History" in page:
    page_history()
elif "Import" in page:
    page_import_export()
