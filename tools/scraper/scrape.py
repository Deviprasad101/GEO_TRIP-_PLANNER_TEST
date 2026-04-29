"""
Tirupati GeoTrip — optional Selenium scraper → CSV matching HTML PAGES format.

Header (exact):
  name of the place,category,latitude,longitude,description,timings,Place,Category,Timings

1) pip install -r requirements.txt
2) Edit DEFAULT_URL and scrape_data() selectors for your real listing site.
3) python scrape.py
4) Output defaults to output/scraped_tirupati_places.csv — copy into HTML PAGES/ for the site (same name).

Respect robots.txt and each site's Terms of Use; add delays between requests.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import pandas as pd
from geopy.geocoders import Nominatim
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"


def get_output_csv_path() -> Path:
    return OUTPUT_DIR / os.environ.get("OUTPUT_CSV_NAME", "scraped_tirupati_places.csv")

# Default: Wikipedia temple list (built-in wikitable parser — reliable rows).
# TripAdvisor etc. need custom Selenium selectors in scrape_data(); .card template will yield 0 rows.
DEFAULT_URL = "https://en.wikipedia.org/wiki/List_of_Hindu_temples_in_Tirupati"

CSV_COLUMNS = [
    "name of the place",
    "category",
    "latitude",
    "longitude",
    "description",
    "timings",
    "Place",
    "Category",
    "Timings",
]

# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def start_driver() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    return driver


# ---------------------------------------------------------------------------
# Geocoding (Nominatim — 1 req/s policy)
# ---------------------------------------------------------------------------
_geolocator: Nominatim | None = None


def _get_geolocator() -> Nominatim:
    global _geolocator
    if _geolocator is None:
        _geolocator = Nominatim(user_agent="geo_trip_planner_scraper/1.0 (educational)")
    return _geolocator


def get_coordinates(place: str) -> tuple[str, str]:
    """Return (latitude, longitude) as strings, or ('', '') if not found."""
    if not (place or "").strip():
        return "", ""
    geo = _get_geolocator()
    try:
        time.sleep(1.1)
        location = geo.geocode(place + ", Tirupati, Andhra Pradesh, India")
        if location:
            return str(location.latitude), str(location.longitude)
    except Exception:
        pass
    return "", ""


def _is_wikipedia_url(url: str) -> bool:
    u = (url or "").lower()
    return "wikipedia.org" in u


def scrape_wikipedia_list_table(url: str) -> list[dict]:
    """
    Fetch a Wikipedia page and parse the main wikitable whose header contains
    'Name of the temple' (List of Hindu temples in Tirupati layout).
    Uses HTTP only (no browser). Follow Wikipedia / Wikimedia User-Agent policy.
    """
    import requests
    from bs4 import BeautifulSoup

    headers = {
        "User-Agent": (
            "GeoTripPlannerScraper/1.0 (local CSV generator for Tirupati trip planner; "
            "respectful queries per https://foundation.wikimedia.org/wiki/Policy:User-Agent_policy)"
        )
    }
    r = requests.get(url, headers=headers, timeout=45)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    data: list[dict] = []

    for table in soup.select("table.wikitable"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        header_text = " ".join(c.get_text(" ", strip=True).lower() for c in header_cells)
        if "name of the temple" not in header_text:
            continue

        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < 6:
                continue
            name = cells[0].get_text(" ", strip=True)
            deity = cells[1].get_text(" ", strip=True)
            location = cells[2].get_text(" ", strip=True)
            timeline = cells[4].get_text(" ", strip=True)
            desc = cells[5].get_text(" ", strip=True)
            if not name:
                continue

            detail_parts = []
            if deity:
                detail_parts.append("Deity: " + deity)
            if location:
                detail_parts.append("Location: " + location)
            if timeline:
                detail_parts.append("Timeline: " + timeline)
            blurb = ". ".join(detail_parts)
            if desc:
                full_desc = blurb + ". " + desc if blurb else desc
            else:
                full_desc = blurb

            lat_s, lng_s = "", ""
            if os.environ.get("GEOCODE_WIKI") == "1":
                q = f"{name}, {location}, Tirupati, Andhra Pradesh, India"
                lat_s, lng_s = get_coordinates(q)

            data.append(
                {
                    "name of the place": name,
                    "category": "Temple",
                    "latitude": lat_s,
                    "longitude": lng_s,
                    "description": full_desc[:2000],
                    "timings": "",
                    "Place": "",
                    "Category": "",
                    "Timings": "",
                }
            )
        break

    return data


def coords_from_maps_url(href: str) -> tuple[str, str]:
    """Parse lat,lng from common Google Maps query patterns."""
    if not href:
        return "", ""
    m = re.search(r"[@?](-?\d+\.?\d*),(-?\d+\.?\d*)", href)
    if m:
        return m.group(1), m.group(2)
    m2 = re.search(r"q=(-?\d+\.?\d*),(-?\d+\.?\d*)", href, re.I)
    if m2:
        return m2.group(1), m2.group(2)
    return "", ""


# ---------------------------------------------------------------------------
# Scraping (customize selectors for your site)
# ---------------------------------------------------------------------------


def scrape_data(driver: webdriver.Chrome, url: str) -> list[dict]:
    driver.get(url)
    wait = WebDriverWait(driver, 10)
    data: list[dict] = []

    # --- Template: list pages with .card > .title / .desc (edit for your site) ---
    try:
        elements = wait.until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, "card"))
        )
        for el in elements:
            try:
                name = el.find_element(By.CLASS_NAME, "title").text.strip()
                desc = el.find_element(By.CLASS_NAME, "desc").text.strip()
            except NoSuchElementException:
                continue

            lat_s, lng_s = "", ""
            try:
                link_el = el.find_element(By.CSS_SELECTOR, "a[href*='google'], a[href*='maps']")
                lat_s, lng_s = coords_from_maps_url(link_el.get_attribute("href") or "")
            except NoSuchElementException:
                pass
            if not lat_s:
                lat_s, lng_s = get_coordinates(name)

            data.append(
                {
                    "name of the place": name,
                    "category": "Temple",
                    "latitude": lat_s,
                    "longitude": lng_s,
                    "description": desc,
                    "timings": "",
                    "Place": "",
                    "Category": "",
                    "Timings": "",
                }
            )
    except TimeoutException:
        pass

    # --- Demo fallback: example.com has h1 + p (no .card) so the script still runs ---
    if not data and "example.com" in (url or "").lower():
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        h1 = driver.find_element(By.TAG_NAME, "h1")
        try:
            p = driver.find_element(By.TAG_NAME, "p")
            desc = p.text.strip()
        except NoSuchElementException:
            desc = ""
        name = h1.text.strip() or "Example row"
        data.append(
            {
                "name of the place": name + " (demo — replace URL & selectors)",
                "category": "Sightseeing",
                "latitude": "13.6288",
                "longitude": "79.4192",
                "description": desc or "Replace DEFAULT_URL and scrape_data() for real Tirupati listings.",
                "timings": "",
                "Place": "",
                "Category": "",
                "Timings": "",
            }
        )

    return data


def remove_duplicates(rows: list[dict]) -> list[dict]:
    seen: set[tuple[str, str, str]] = set()
    unique: list[dict] = []
    for d in rows:
        key = (
            str(d.get("name of the place") or "").strip().lower(),
            str(d.get("latitude") or "").strip(),
            str(d.get("longitude") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(d)
    return unique


def save_csv(data: list[dict]) -> Path | None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = get_output_csv_path()
    if not data:
        print(
            "ERROR: No rows to save — CSV not written (would only contain a header). "
            "Use Wikipedia URL for built-in table parsing, or fix scrape_data() selectors."
        )
        if out_path.is_file():
            print(f"Existing file left unchanged: {out_path}")
        return None
    df = pd.DataFrame(data)
    for col in CSV_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    df = df[CSV_COLUMNS]
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path


def _existing_html_csv_path() -> Path | None:
    """Prefer scraped_tirupati_places.csv, then legacy tirupati_places_with_hospitals.csv."""
    html_dir = ROOT.parent.parent / "HTML PAGES"
    for name in ("scraped_tirupati_places.csv", "tirupati_places_with_hospitals.csv"):
        p = html_dir / name
        if p.is_file():
            return p
    return None


def _merge_with_existing_html_csv(new_rows: list[dict]) -> list[dict]:
    """Append wiki rows to existing HTML PAGES CSV if present; duplicate names keep newer (wiki)."""
    merge_path = _existing_html_csv_path()
    if merge_path is None:
        return new_rows
    try:
        old = pd.read_csv(merge_path, encoding="utf-8", keep_default_na=False)
    except Exception:
        return new_rows
    if not new_rows:
        return old.to_dict("records")
    for col in CSV_COLUMNS:
        if col not in old.columns:
            old[col] = ""
    old = old[CSV_COLUMNS]
    fresh = pd.DataFrame(new_rows)
    for col in CSV_COLUMNS:
        if col not in fresh.columns:
            fresh[col] = ""
    fresh = fresh[CSV_COLUMNS]
    combined = pd.concat([old, fresh], ignore_index=True)
    combined.drop_duplicates(subset=["name of the place"], keep="last", inplace=True)
    return combined.to_dict("records")


def main() -> None:
    url = os.environ.get("SCRAPE_URL", DEFAULT_URL)

    if _is_wikipedia_url(url):
        print("Wikipedia URL detected: using table parser (no Selenium).")
        data = scrape_wikipedia_list_table(url)
        if os.environ.get("MERGE_HTML_CSV", "1") == "1":
            data = _merge_with_existing_html_csv(data)
        data = remove_duplicates(data)
        out = save_csv(data)
        if out:
            print(f"Wrote {len(data)} row(s) -> {out}")
            print("Place this file in HTML PAGES/ (same filename) for the trip planner to load it.")
        if os.environ.get("GEOCODE_WIKI") != "1":
            print("Tip: set GEOCODE_WIKI=1 to fill lat/lng via Nominatim (slow, ~1s per row).")
        return

    driver = start_driver()
    try:
        data = scrape_data(driver, url)
        data = remove_duplicates(data)
        out = save_csv(data)
        if out:
            print(f"Wrote {len(data)} row(s) -> {out}")
            print("Place this file in HTML PAGES/ (same filename) for the trip planner to load it.")
    finally:
        driver.quit()


def test_open() -> None:
    driver = start_driver()
    try:
        driver.get("https://example.com")
        print("Page title:", driver.title)
    finally:
        driver.quit()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        test_open()
    else:
        main()
