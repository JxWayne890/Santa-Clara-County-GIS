# Santa Clara County Property Scraper

Scrapes property data for **Santa Clara County, CA** by **APN** or **street address**.

- Resolves address → APN with the County’s ArcGIS **Address Locator**, then intersects the parcel layer for **situs**.
- Pulls the Assessor’s parcel page via the official **APN redirect** and parses **assessed values**, **homeowner’s exemption**, **last transfer (doc#, date, price if present)**, and **basic characteristics**.

> ⚠️ **Owner names**: The Santa Clara County Assessor generally does **not** publish assessee (owner) names online. Expect `owner_names` to be empty unless a name appears in a mailing block.

---

## Features

- Input: `--apn "235-12-003"` **or** `--address "70 W Hedding St, San Jose, CA"`.
- Output: normalized JSON with:
  - `apn`
  - `situs_address` (street, city, state, zip)
  - `owner_names` (usually empty)
  - `owner_mailing_address` (street, city, state, zip) — if present online
  - `owner_type` (heuristic: individual/trust/LLC/corp/partnership)
  - `last_transfer` (recording_date, doc_number, price)
  - `assessed_values` (land, improvements, total, year)
  - `homeowner_exemption` (boolean)
  - `property_details` (use_code, year_built, living_sqft, lot_sqft)
  - `recorder_doc_url` (landing page; deep links often unavailable)

---

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
