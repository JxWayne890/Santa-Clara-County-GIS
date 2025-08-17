#!/usr/bin/env python3
"""
Santa Clara County Property Scraper
-----------------------------------

Scrapes parcel + assessment details for Santa Clara County by APN or street address.

Implements:
  * search_property(query) -> RawSearchResult (raw payloads + resolved APN)
  * parse_property_page(html) -> dict (normalized fields)
  * save_to_json(data, filename)

Notes:
- Owner names are typically NOT published online by the Santa Clara Assessor.
- If the Assessor site changes markup or adds a CAPTCHA, use --demo to validate
  the parser against an embedded sample page.

Tested with Python 3.10+.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
import typing as t
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup


# ------------------------------
# Config & Endpoints
# ------------------------------

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

# ArcGIS REST: parcels layer (ID 0) in SCCProperty MapServer
ARCGIS_PARCEL_QUERY = (
    "https://mapservices.sccgov.org/arcgis/rest/services/property/SCCProperty/MapServer/0/query"
)

# ArcGIS REST: County address locator (geocoder)
ARCGIS_ADDRESS_GEOCODE = (
    "https://mapservices.sccgov.org/arcgis/rest/services/locators/SCCSearchAddress/GeocodeServer/findAddressCandidates"
)

# Assessor "APN redirect" to land on the parcel details page
ASSESSOR_APN_REDIRECT = "https://www.sccassessor.org/index.php/apn-redirect?ApnValue={apn}"

# Clerk-Recorder search landing page (deep links by doc # are not guaranteed)
RECORDER_SEARCH_LANDING = (
    "https://clerkrecorder.santaclaracounty.gov/official-records/records-search"
)

# Simple friendly rate limit between remote calls
RATE_LIMIT_SECONDS = 1.2


# ------------------------------
# Utilities
# ------------------------------

def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def is_probable_apn(text: str) -> bool:
    """
    Santa Clara APNs are typically 3-2-3 digits with dashes (e.g., 123-45-678),
    but can appear without dashes. This check is permissive.
    """
    s = re.sub(r"[\s-]", "", text)
    return bool(re.fullmatch(r"\d{8,12}", s))


def normalize_apn(text: str) -> str:
    """Return dashed APN if confidently derivable; else return stripped input."""
    digits = re.sub(r"\D", "", text)
    if len(digits) == 9:  # common SCC format
        return f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"
    return text.strip()


def classify_owner_type(name: str | None) -> str | None:
    if not name:
        return None
    n = name.upper()
    if "TRUST" in n or n.endswith(" TR"):
        return "trust"
    if "LLC" in n:
        return "llc"
    if any(k in n for k in [" INC", " CORP", " CORPORATION", " CO.", " COMPANY"]):
        return "corporation"
    if any(k in n for k in [" LP", " L.P.", " LTD", " LIMITED", " PARTNERSHIP"]):
        return "partnership"
    return "individual"


def rate_limit():
    time.sleep(RATE_LIMIT_SECONDS)


# ------------------------------
# HTTP session
# ------------------------------

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    # requests doesn't support a global timeout on Session; pass per-call
    return s


# ------------------------------
# Core I/O dataclasses
# ------------------------------

@dataclass
class RawSearchResult:
    apn: str | None
    situs: dict[str, t.Any] | None
    parcel_feature: dict[str, t.Any] | None
    assessor_html: str | None


# ------------------------------
# ArcGIS helpers
# ------------------------------

def arcgis_query_parcel_by_apn(apn: str, *, session: requests.Session) -> dict[str, t.Any] | None:
    params = {
        "f": "json",
        "where": f"APN='{apn}'",
        "outFields": "*",
        "returnGeometry": "false",
    }
    try:
        rate_limit()
        r = session.get(ARCGIS_PARCEL_QUERY, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
        feats = js.get("features") or []
        logging.debug("Parcel-by-APN returned %d feature(s)", len(feats))
        return feats[0] if feats else None
    except requests.RequestException as e:
        logging.warning("Parcel-by-APN request failed: %s", e)
        return None


def arcgis_query_parcel_by_apn_like(bare_digits: str, *, session: requests.Session) -> dict[str, t.Any] | None:
    # Fallback: match APN using LIKE (some servers won't allow SQL REPLACE)
    params = {
        "f": "json",
        "where": f"APN LIKE '%{bare_digits}%'",
        "outFields": "*",
        "returnGeometry": "false",
    }
    try:
        rate_limit()
        r = session.get(ARCGIS_PARCEL_QUERY, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
        feats = js.get("features") or []
        logging.debug("Parcel-by-APN-like returned %d feature(s)", len(feats))
        return feats[0] if feats else None
    except requests.RequestException as e:
        logging.warning("Parcel-by-APN-like request failed: %s", e)
        return None


def arcgis_geocode_address(address: str, *, session: requests.Session) -> dict[str, t.Any] | None:
    """
    Geocode an address. Returns the top candidate (with .location, .attributes, .spatialReference),
    or None if nothing matches.
    """
    params = {
        "f": "json",
        "SingleLine": address,
        "outFields": "*",          # ask for attributes (may include APN)
        "maxLocations": 5,
        # "outSR": 4326,           # optional: force WGS84; leaving off is fine
    }
    try:
        rate_limit()
        r = session.get(ARCGIS_ADDRESS_GEOCODE, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
        cands = js.get("candidates") or []
        # be a bit permissive
        cands = [c for c in cands if c.get("score", 0) >= 70]
        cands.sort(key=lambda c: c.get("score", 0), reverse=True)
        logging.debug("Geocoder returned %d viable candidate(s)", len(cands))
        if not cands:
            return None
        top = cands[0]
        # Patches to ensure expected keys exist
        top.setdefault("location", {})
        top.setdefault("attributes", {})
        if "spatialReference" not in top:
            top["spatialReference"] = {"wkid": 4326}
        return top
    except requests.RequestException as e:
        logging.warning("Geocode request failed: %s", e)
        return None


def arcgis_query_parcel_by_point(x: float, y: float, *, wkid: int = 4326, session: requests.Session) -> dict[str, t.Any] | None:
    params = {
        "f": "json",
        "geometry": json.dumps({"x": x, "y": y, "spatialReference": {"wkid": wkid}}),
        "geometryType": "esriGeometryPoint",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "*",
        "returnGeometry": "false",
        "inSR": wkid,
    }
    try:
        rate_limit()
        r = session.get(ARCGIS_PARCEL_QUERY, params=params, timeout=30)
        r.raise_for_status()
        js = r.json()
        feats = js.get("features") or []
        logging.debug("Parcel-by-point returned %d feature(s)", len(feats))
        return feats[0] if feats else None
    except requests.RequestException as e:
        logging.warning("Parcel-by-point request failed: %s", e)
        return None


def extract_situs_from_feature(feature: dict[str, t.Any] | None) -> dict[str, t.Any] | None:
    if not feature:
        return None
    a = feature.get("attributes") or {}

    def tval(k: str) -> str | None:
        v = a.get(k)
        return v.strip() if isinstance(v, str) else (str(v).strip() if v is not None else None)

    street_parts = [
        tval("SITUS_HOUSE_NUMBER") or "",
        tval("SITUS_HOUSE_NUMBER_SUFFIX") or "",
        tval("SITUS_STREET_DIRECTION") or "",
        tval("SITUS_STREET_NAME") or "",
        tval("SITUS_STREET_TYPE") or "",
        (f"Unit {tval('SITUS_UNIT_NUMBER')}" if tval("SITUS_UNIT_NUMBER") else ""),
    ]
    street = " ".join([s for s in street_parts if s]).replace("  ", " ").strip()

    return {
        "apn": tval("APN"),
        "street": street or None,
        "city": tval("SITUS_CITY_NAME"),
        "state": tval("SITUS_STATE_CODE") or "CA",
        "zip": (tval("SITUS_ZIP_CODE") or None),
    }


# ------------------------------
# 1) search_property(query)
# ------------------------------

def search_property(query: str, *, session: requests.Session | None = None) -> RawSearchResult:
    """
    Resolve APN from address or validate APN, fetch parcel attributes from ArcGIS,
    then GET the assessor HTML via the APN-redirect endpoint.

    Returns RawSearchResult containing low-level payloads (for parse_property_page).
    """
    session = session or build_session()
    query = query.strip()
    logging.info("search_property: %s", query)

    # APN path
    if is_probable_apn(query):
        apn_dashed = normalize_apn(query)
        resolved_apn = apn_dashed
        logging.debug("Detected APN; normalized to %s", apn_dashed)

        feature = arcgis_query_parcel_by_apn(resolved_apn, session=session)
        if not feature:
            bare = re.sub(r"\D", "", query)
            logging.debug("Primary APN query empty; retry LIKE on bare %s", bare)
            feature = arcgis_query_parcel_by_apn_like(bare, session=session)
        situs = extract_situs_from_feature(feature) if feature else None

    # Address path
    else:
        logging.debug("Detected street address; geocoding…")
        geo = arcgis_geocode_address(query, session=session)
        if not geo:
            logging.warning("No geocode candidates found for address.")
            return RawSearchResult(apn=None, situs=None, parcel_feature=None, assessor_html=None)

        logging.debug("Raw geocode candidate: %r", geo)

        # First try: APN directly from geocoder attributes
        apn_from_geo = (geo.get("attributes") or {}).get("APN") or (geo.get("attributes") or {}).get("apn")
        feature = None
        resolved_apn = None
        if apn_from_geo:
            apn_norm = normalize_apn(str(apn_from_geo))
            logging.debug("Geocoder provided APN: %s -> %s", apn_from_geo, apn_norm)
            feature = arcgis_query_parcel_by_apn(apn_norm, session=session)
            if not feature:
                # try LIKE on bare digits as a fallback
                bare = re.sub(r"\D", "", apn_norm)
                feature = arcgis_query_parcel_by_apn_like(bare, session=session)
            if feature:
                resolved_apn = feature["attributes"].get("APN")

        # Second try: spatial intersect if APN wasn’t present / didn’t match
        if not feature:
            x = geo["location"].get("x")
            y = geo["location"].get("y")
            in_wkid = (geo.get("spatialReference") or {}).get("wkid", 4326)
            logging.debug("Using spatial intersect with point x=%s y=%s wkid=%s", x, y, in_wkid)
            if x is not None and y is not None:
                feature = arcgis_query_parcel_by_point(x, y, wkid=in_wkid, session=session)
                resolved_apn = feature["attributes"]["APN"] if feature else None

        situs = extract_situs_from_feature(feature) if feature else None

    # Hit assessor page (may not include owner names by policy)
    assessor_html = None
    if resolved_apn:
        assessor_url = ASSESSOR_APN_REDIRECT.format(apn=quote_plus(resolved_apn))
        logging.debug("GET assessor via %s", assessor_url)
        try:
            rate_limit()
            r = session.get(assessor_url, allow_redirects=True, timeout=30)
            r.raise_for_status()
            assessor_html = r.text
        except requests.RequestException as e:
            logging.warning("Assessor page request failed: %s", e)

    return RawSearchResult(
        apn=resolved_apn,
        situs=situs,
        parcel_feature=feature,
        assessor_html=assessor_html,
    )


# ------------------------------
# 2) parse_property_page(html)
# ------------------------------

_LABEL_ALIASES = {
    "apn": [r"\bAPN\b", r"Assessor'?s?\s+Parcel\s+Number"],
    "situs_address": [r"Situs\s+Address", r"Property\s+Address"],
    "mailing_address": [r"Mailing\s+Address"],
    "owner_name": [r"Owner\s*Name", r"Assessee\s*Name"],
    "homeowner_exemption": [r"Homeowner'?s?\s+Exemption"],
    "use_code": [r"Use\s*Code"],
    "year_built": [r"Year\s*Built"],
    "living_sqft": [r"Living\s*Area", r"Building\s*Area", r"Sq\s*Ft"],
    "lot_sqft": [r"Lot\s*(Area|Size|Square\s*Feet)"],
    "doc_number": [r"Document\s*(No\.?|Number)|Doc\s*#"],
    "recording_date": [r"(Recording|Transfer)\s*Date"],
    "sales_price": [r"(Sales|Indicated)\s*(Price|Net\s*Value)"],
    "assessed_land": [r"Land\s*Value"],
    "assessed_impr": [r"(Improvement|Improvements)\s*Value"],
    "assessed_total": [r"Total\s*(Assessed\s*)?Value"],
    "assessed_year": [r"(Roll|Tax|Assessment)\s*Year"],
}

def parse_property_page(html: str) -> dict[str, t.Any]:
    """
    Heuristic parser for the Assessor property page HTML. The site is legacy,
    table-heavy, and can change; we therefore look for labels by regex and
    harvest adjacent cells/siblings.
    """
    soup = BeautifulSoup(html, "lxml")

    # Normalize table rows into label/value pairs
    kv: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) == 2:
            k = cells[0].get_text(" ", strip=True)
            v = cells[1].get_text(" ", strip=True)
            if k and v:
                kv.setdefault(k, v)
        elif len(cells) >= 2:
            k = cells[0].get_text(" ", strip=True)
            v = " ".join(c.get_text(" ", strip=True) for c in cells[1:])
            if k and v:
                kv.setdefault(k, v)

    def find_first(patterns: list[str]) -> str | None:
        for pat in patterns:
            # exact label hit in kv
            for k, v in kv.items():
                if re.search(pat, k, re.I):
                    if v:
                        return v
            # loose search in whole doc (label preceding value)
            lab = soup.find(string=re.compile(pat, re.I))
            if lab:
                sib = lab.parent.find_next_sibling()
                if sib:
                    txt = sib.get_text(" ", strip=True)
                    if txt:
                        return txt
        return None

    # Helpers
    def normalize_money(val: str | None) -> int | None:
        if not val:
            return None
        m = re.search(r"\$?\s*([\d,]+)", val)
        if not m:
            return None
        return int(m.group(1).replace(",", ""))

    def safe_int(s: str | None) -> int | None:
        if not s:
            return None
        m = re.search(r"(\d{4})", s)
        return int(m.group(1)) if m else None

    def split_city_state_zip(full: str | None) -> tuple[str | None, str | None, str | None]:
        if not full:
            return None, None, None
        m = re.search(r"^\s*([^,]+)\s*,\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", full)
        if m:
            return m.group(1).strip(), m.group(2), m.group(3)
        return None, None, None

    # Owner mailing address (if present)
    mailing_block = find_first(_LABEL_ALIASES["mailing_address"])
    mailing_street = mailing_city = mailing_state = mailing_zip = None
    if mailing_block:
        lines = [ln.strip() for ln in re.split(r"[\n\r]+", mailing_block) if ln.strip()]
        if lines:
            c, st, z = split_city_state_zip(lines[-1])
            mailing_city, mailing_state, mailing_zip = c, st, z
            if len(lines) >= 2:
                mailing_street = lines[-2]

    # Situs address (some pages provide it again in assessor HTML)
    situs_full = find_first(_LABEL_ALIASES["situs_address"])
    situs_street = situs_city = situs_state = situs_zip = None
    if situs_full:
        parts = [s.strip() for s in re.split(r"[\n\r]+", situs_full) if s.strip()]
        if len(parts) == 1:
            if "," in parts[0]:
                street, rest = parts[0].split(",", 1)
                situs_street = street.strip()
                c, st, z = split_city_state_zip(rest.strip())
                situs_city, situs_state, situs_zip = c, st or "CA", z
            else:
                situs_street = parts[0]
        elif len(parts) >= 2:
            situs_street = parts[0]
            c, st, z = split_city_state_zip(parts[1])
            situs_city, situs_state, situs_zip = c, st or "CA", z

    # Owner name (often suppressed on SCC site)
    owner_name = find_first(_LABEL_ALIASES["owner_name"])

    # Homeowner's exemption
    hx_text = find_first(_LABEL_ALIASES["homeowner_exemption"])
    homeowner_exemption = None
    if hx_text:
        homeowner_exemption = bool(re.search(r"\b(yes|y|true)\b", hx_text, re.I))

    # Property characteristics
    use_code = find_first(_LABEL_ALIASES["use_code"])
    year_built = safe_int(find_first(_LABEL_ALIASES["year_built"]))
    living_sqft = normalize_money(find_first(_LABEL_ALIASES["living_sqft"]))  # treat as int
    lot_sqft = normalize_money(find_first(_LABEL_ALIASES["lot_sqft"]))

    # Transfer/document block
    doc_no = find_first(_LABEL_ALIASES["doc_number"])
    rec_date = find_first(_LABEL_ALIASES["recording_date"])
    price = normalize_money(find_first(_LABEL_ALIASES["sales_price"]))

    # Assessed values
    land = normalize_money(find_first(_LABEL_ALIASES["assessed_land"]))
    impr = normalize_money(find_first(_LABEL_ALIASES["assessed_impr"]))
    total = normalize_money(find_first(_LABEL_ALIASES["assessed_total"]))
    year = None
    ytxt = find_first(_LABEL_ALIASES["assessed_year"])
    if ytxt:
        m = re.search(r"(\d{4})", ytxt)
        year = int(m.group(1)) if m else None

    return {
        "apn": find_first(_LABEL_ALIASES["apn"]),
        "situs_address": {
            "street": situs_street,
            "city": situs_city,
            "state": situs_state,
            "zip": situs_zip,
        },
        "owner_names": [owner_name] if owner_name else [],
        "owner_mailing_address": {
            "street": mailing_street,
            "city": mailing_city,
            "state": mailing_state,
            "zip": mailing_zip,
        },
        "owner_type": classify_owner_type(owner_name),
        "last_transfer": {
            "recording_date": rec_date,
            "doc_number": doc_no,
            "price": price,
        },
        "assessed_values": {
            "land": land,
            "improvements": impr,
            "total": total,
            "year": year,
        },
        "homeowner_exemption": homeowner_exemption,
        "property_details": {
            "use_code": use_code,
            "year_built": year_built,
            "living_sqft": living_sqft,
            "lot_sqft": lot_sqft,
        },
        "recorder_doc_url": (RECORDER_SEARCH_LANDING if doc_no else None),
    }


# ------------------------------
# 3) save_to_json(data, filename)
# ------------------------------

def save_to_json(data: dict[str, t.Any], filename: str | Path) -> None:
    p = Path(filename)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info("Wrote %s", p)


# ------------------------------
# CLI glue
# ------------------------------

DEMO_HTML = r"""
<html><body>
<table>
<tr><th>APN</th><td>235-12-003</td></tr>
<tr><th>Situs Address</th><td>123 Sample St<br>San Jose, CA 95123</td></tr>
<tr><th>Mailing Address</th><td>JANE DOE TRUST<br>PO BOX 1234<br>San Jose, CA 95123</td></tr>
<tr><th>Homeowner's Exemption</th><td>Yes</td></tr>
<tr><th>Use Code</th><td>SFR</td></tr>
<tr><th>Year Built</th><td>1978</td></tr>
<tr><th>Living Area</th><td>1,234</td></tr>
<tr><th>Lot Area</th><td>6,098</td></tr>
<tr><th>Document Number</th><td>12345678</td></tr>
<tr><th>Recording Date</th><td>06/15/2021</td></tr>
<tr><th>Sales Price</th><td>$1,250,000</td></tr>
</table>
<table>
<tr><th>Roll Year</th><td>2024</td></tr>
<tr><th>Land Value</th><td>$900,000</td></tr>
<tr><th>Improvements Value</th><td>$450,000</td></tr>
<tr><th>Total Assessed Value</th><td>$1,350,000</td></tr>
</table>
</body></html>
"""

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Santa Clara County parcel/assessment details by APN or street address."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--apn", help="APN (e.g., 235-12-003 or 23512003)")
    src.add_argument("--address", help="Street address (e.g., '70 W Hedding St, San Jose, CA')")
    parser.add_argument("-o", "--output", default="output.json", help="Write JSON to this path (default: output.json)")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (-v, -vv)")
    parser.add_argument("--demo", action="store_true", help="Parse a built-in sample HTML (no network)")
    args = parser.parse_args()

    setup_logging(args.verbose)

    if args.demo:
        logging.info("Running in --demo mode (no network calls)")
        parsed = parse_property_page(DEMO_HTML)
        if not parsed.get("apn") and args.apn:
            parsed["apn"] = normalize_apn(args.apn)
        save_to_json(parsed, args.output)
        return

    session = build_session()

    query = args.apn if args.apn else args.address
    assert query is not None

    raw = search_property(query, session=session)

    # Seed final record with GIS-derived APN/situs skeleton
    result: dict[str, t.Any] = {
        "apn": raw.apn,
        "situs_address": raw.situs or {"street": None, "city": None, "state": None, "zip": None},
        "owner_names": [],
        "owner_mailing_address": {"street": None, "city": None, "state": None, "zip": None},
        "owner_type": None,
        "last_transfer": {"recording_date": None, "doc_number": None, "price": None},
        "assessed_values": {"land": None, "improvements": None, "total": None, "year": None},
        "homeowner_exemption": None,
        "property_details": {"use_code": None, "year_built": None, "living_sqft": None, "lot_sqft": None},
        "recorder_doc_url": None,
    }

    if not raw.apn and not raw.assessor_html:
        logging.error("No records found (neither APN resolution nor Assessor page available).")
        save_to_json(result, args.output)
        sys.exit(2)

    if raw.assessor_html:
        parsed = parse_property_page(raw.assessor_html)

        # Merge: prefer assessor values when present; fallback to GIS situs
        result["apn"] = result["apn"] or parsed.get("apn")
        p_situs = parsed.get("situs_address") or {}
        if p_situs.get("street") and p_situs.get("city"):
            result["situs_address"] = p_situs
        result["owner_names"] = parsed.get("owner_names") or []
        result["owner_mailing_address"] = parsed.get("owner_mailing_address") or result["owner_mailing_address"]
        result["owner_type"] = parsed.get("owner_type")
        result["last_transfer"] = parsed.get("last_transfer") or result["last_transfer"]
        result["assessed_values"] = parsed.get("assessed_values") or result["assessed_values"]
        result["homeowner_exemption"] = parsed.get("homeowner_exemption")
        result["property_details"] = parsed.get("property_details") or result["property_details"]
        result["recorder_doc_url"] = parsed.get("recorder_doc_url")

    # Normalize structure (ensure nested keys exist)
    result = {
        "apn": result.get("apn"),
        "situs_address": {
            "street": (result.get("situs_address") or {}).get("street"),
            "city": (result.get("situs_address") or {}).get("city"),
            "state": (result.get("situs_address") or {}).get("state"),
            "zip": (result.get("situs_address") or {}).get("zip"),
        },
        "owner_names": result.get("owner_names") or [],
        "owner_mailing_address": {
            "street": (result.get("owner_mailing_address") or {}).get("street"),
            "city": (result.get("owner_mailing_address") or {}).get("city"),
            "state": (result.get("owner_mailing_address") or {}).get("state"),
            "zip": (result.get("owner_mailing_address") or {}).get("zip"),
        },
        "owner_type": result.get("owner_type"),
        "last_transfer": {
            "recording_date": (result.get("last_transfer") or {}).get("recording_date"),
            "doc_number": (result.get("last_transfer") or {}).get("doc_number"),
            "price": (result.get("last_transfer") or {}).get("price"),
        },
        "assessed_values": {
            "land": (result.get("assessed_values") or {}).get("land"),
            "improvements": (result.get("assessed_values") or {}).get("improvements"),
            "total": (result.get("assessed_values") or {}).get("total"),
            "year": (result.get("assessed_values") or {}).get("year"),
        },
        "homeowner_exemption": result.get("homeowner_exemption"),
        "property_details": {
            "use_code": (result.get("property_details") or {}).get("use_code"),
            "year_built": (result.get("property_details") or {}).get("year_built"),
            "living_sqft": (result.get("property_details") or {}).get("living_sqft"),
            "lot_sqft": (result.get("property_details") or {}).get("lot_sqft"),
        },
        "recorder_doc_url": result.get("recorder_doc_url"),
    }

    save_to_json(result, args.output)


if __name__ == "__main__":
    main()
