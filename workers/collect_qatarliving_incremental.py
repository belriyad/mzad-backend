#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def check_dns(host="api.mzadqatar.com"):
    try:
        socket.setdefaulttimeout(5)
        socket.gethostbyname(host)
    except socket.gaierror as e:
        import sys
        print(f"[FATAL] DNS resolution failed: {e}. Check /etc/resolv.conf", file=sys.stderr)
        sys.exit(1)

check_dns()


API_URL = "https://bo-prod.qatarliving.com/vehicles"
WEB_BASE = "https://www.qatarliving.com"
IMAGE_BASE = "https://files.qatarliving.com"

DB_PATH = Path("/home/briyad/data/mzad-qatar/mzad_local.db")
TABLE_NAME = "car_listings_api_10000"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_int(value) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if not digits:
        return None
    try:
        return int(digits)
    except ValueError:
        return None


def build_full_url(alias: str) -> str:
    value = str(alias or "").strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if not value.startswith("/"):
        value = f"/{value}"
    return f"{WEB_BASE}{value}"


def build_image_url(uri: str) -> str:
    value = str(uri or "").strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"{IMAGE_BASE}/{value.lstrip('/')}"


def fetch_page(cur_page: int, per_page: int, cookie: str, if_modified_since: str = "") -> dict:
    url = f"{API_URL}?cur_page={cur_page}&per_page={per_page}"
    cmd = [
        "curl",
        "-sS",
        "--compressed",
        "--connect-timeout",
        "10",
        "--max-time",
        "30",
        url,
        "-H",
        "User-Agent: okhttp/4.12.0",
        "-H",
        "Accept-Encoding: gzip, deflate, br",
    ]
    if cookie:
        cmd.extend(["-H", f"Cookie: adminjs={cookie}"])
    if if_modified_since:
        cmd.extend(["-H", f"If-Modified-Since: {if_modified_since}"])
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)


def normalize_row(item: dict) -> dict:
    make = ((item.get("vehicleMake") or {}).get("makeName") or "").strip()
    class_name = ((item.get("vehicleModel") or {}).get("modelName") or "").strip()
    model = ((item.get("vehicleTrim") or {}).get("trimName") or "").strip()
    year = parse_int((item.get("year") or {}).get("yearName"))
    km = parse_int(item.get("milage"))
    fuel = ((item.get("fuelType") or {}).get("fuelTypeName") or "").strip()
    cylinder = parse_int((item.get("cylinder") or {}).get("cylinderName"))
    seller_name = ((item.get("user") or {}).get("username") or "").strip()
    seller_phone = str(item.get("contactMobile_1") or "").strip()
    seller_whatsapp = str(item.get("contactWhatsapp_1") or "").strip()
    is_showroom = bool(item.get("isShowroom"))
    city = ((item.get("location") or {}).get("locationName") or "").strip()
    alias = ((item.get("urls") or [{}])[0].get("urlAlias") or "").strip()
    url = build_full_url(alias)
    image_urls = [
        build_image_url((img.get("image") or {}).get("uri"))
        for img in (item.get("images") or [])
        if (img.get("image") or {}).get("uri")
    ]
    image_urls = [u for u in image_urls if u]
    title = " ".join(
        [part for part in [str(year) if year else "", make, class_name, model] if str(part).strip()]
    ).strip()
    if not title:
        title = f"QatarLiving Vehicle {item.get('adId')}"

    properties = {
        "engine_size": item.get("engineSize"),
        "is_brand_new": item.get("isBrandNew"),
        "is_promoted": item.get("isPromoted"),
        "installments_available": item.get("installmentsAvailable"),
        "status": item.get("status"),
        "is_showroom": is_showroom,
        "source": "qatarliving",
        "raw": item,
    }

    source_id = str(item.get("adId") or "").strip()
    product_id = f"ql_{source_id}" if source_id else ""
    return {
        "product_id": product_id,
        "title": title,
        "price_qar": parse_int(item.get("price")),
        "make": make,
        "class_name": class_name,
        "model": model,
        "manufacture_year": year,
        "km": km,
        "car_type": "",
        "gear_type": "",
        "fuel_type": fuel,
        "warranty_status": "New" if item.get("isBrandNew") else "Used",
        "cylinder_count": cylinder,
        "seller_name": seller_name,
        "seller_phone": seller_phone,
        "seller_type": "dealer" if is_showroom else "private",
        "seller_whatsapp": seller_whatsapp,
        "seller_user_id": "",
        "is_company": "1" if is_showroom else "0",
        "city": city,
        "url": url,
        "main_image_url": image_urls[0] if image_urls else "",
        "image_urls_json": json.dumps(image_urls, ensure_ascii=False),
        "all_image_urls_json": json.dumps(image_urls, ensure_ascii=False),
        "properties_json": json.dumps(properties, ensure_ascii=False),
        "comments_count": 0,
        "comments_json": "[]",
        "description": "",
        "advertise_time_formatted": "",
        "date_of_advertise": "",
    }


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        create table if not exists {TABLE_NAME} (
            product_id text primary key,
            title text,
            price_qar integer,
            make text,
            class_name text,
            model text,
            manufacture_year integer,
            km integer,
            car_type text,
            gear_type text,
            fuel_type text,
            warranty_status text,
            cylinder_count integer,
            seller_name text,
            seller_phone text,
            seller_type text,
            seller_whatsapp text,
            seller_user_id text,
            is_company text,
            city text,
            url text,
            main_image_url text,
            image_urls_json text,
            all_image_urls_json text,
            properties_json text,
            comments_count integer,
            comments_json text,
            description text,
            advertise_time_formatted text,
            date_of_advertise text
        )
        """
    )
    conn.execute(
        """
        create table if not exists collection_runs (
            id integer primary key autoincrement,
            started_at text not null,
            finished_at text,
            pages_scanned integer default 0,
            new_rows integer default 0,
            notes text
        )
        """
    )


def upsert_listing(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute(
        f"""
        insert into {TABLE_NAME}
        (product_id, title, price_qar, make, class_name, model, manufacture_year, km, car_type, gear_type,
         fuel_type, warranty_status, cylinder_count, seller_name, seller_phone, seller_type, seller_whatsapp, seller_user_id, is_company,
         city, url, main_image_url, image_urls_json, all_image_urls_json, properties_json, comments_count, comments_json, description,
         advertise_time_formatted, date_of_advertise)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(product_id) do update set
          title=excluded.title,
          price_qar=excluded.price_qar,
          make=excluded.make,
          class_name=excluded.class_name,
          model=excluded.model,
          manufacture_year=excluded.manufacture_year,
          km=excluded.km,
          car_type=excluded.car_type,
          gear_type=excluded.gear_type,
          fuel_type=excluded.fuel_type,
          warranty_status=excluded.warranty_status,
          cylinder_count=excluded.cylinder_count,
          seller_name=excluded.seller_name,
          seller_phone=excluded.seller_phone,
          seller_type=excluded.seller_type,
          seller_whatsapp=excluded.seller_whatsapp,
          seller_user_id=excluded.seller_user_id,
          is_company=excluded.is_company,
          city=excluded.city,
          url=excluded.url,
          main_image_url=excluded.main_image_url,
          image_urls_json=excluded.image_urls_json,
          all_image_urls_json=excluded.all_image_urls_json,
          properties_json=excluded.properties_json,
          comments_count=excluded.comments_count,
          comments_json=excluded.comments_json,
          description=excluded.description,
          advertise_time_formatted=excluded.advertise_time_formatted,
          date_of_advertise=excluded.date_of_advertise
        """,
        (
            row["product_id"],
            row["title"],
            row["price_qar"],
            row["make"],
            row["class_name"],
            row["model"],
            row["manufacture_year"],
            row["km"],
            row["car_type"],
            row["gear_type"],
            row["fuel_type"],
            row["warranty_status"],
            row["cylinder_count"],
            row["seller_name"],
            row["seller_phone"],
            row["seller_type"],
            row["seller_whatsapp"],
            row["seller_user_id"],
            row["is_company"],
            row["city"],
            row["url"],
            row["main_image_url"],
            row["image_urls_json"],
            row["all_image_urls_json"],
            row["properties_json"],
            row["comments_count"],
            row["comments_json"],
            row["description"],
            row["advertise_time_formatted"],
            row["date_of_advertise"],
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental collector for QatarLiving vehicles API")
    parser.add_argument(
        "--cookie",
        default=os.environ.get("QATARLIVING_ADMINJS", ""),
        help="adminjs cookie value (without 'adminjs='). Falls back to QATARLIVING_ADMINJS env var.",
    )
    parser.add_argument("--if-modified-since", default="")
    parser.add_argument("--max-pages", type=int, default=120)
    parser.add_argument("--per-page", type=int, default=20)
    parser.add_argument("--stale-pages", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.2)
    args = parser.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA wal_autocheckpoint=500")
    ensure_tables(conn)

    run_id = conn.execute(
        "insert into collection_runs(started_at, pages_scanned, new_rows, notes) values (?, 0, 0, ?)",
        (utc_now_iso(), "incremental_qatarliving"),
    ).lastrowid
    conn.commit()

    existing_ids = {str(r[0]) for r in conn.execute(f"select product_id from {TABLE_NAME} where product_id like 'ql_%'").fetchall()}
    stale_pages = 0
    pages_scanned = 0
    new_rows = 0

    try:
        for page in range(1, args.max_pages + 1):
            payload = fetch_page(page, args.per_page, args.cookie, args.if_modified_since)
            items = payload.get("adsCar") or []
            pages_scanned += 1
            page_new = 0

            for item in items:
                row = normalize_row(item)
                pid = row["product_id"]
                if not pid:
                    continue
                is_new = pid not in existing_ids
                upsert_listing(conn, row)
                if is_new:
                    existing_ids.add(pid)
                    page_new += 1
                    new_rows += 1

            conn.commit()
            print(f"source=qatarliving page={page} items={len(items)} new_rows_page={page_new} total_new={new_rows}")

            if page_new == 0:
                stale_pages += 1
            else:
                stale_pages = 0
            if stale_pages >= args.stale_pages:
                break
            if args.sleep > 0:
                time.sleep(args.sleep)
    finally:
        conn.execute(
            "update collection_runs set finished_at = ?, pages_scanned = ?, new_rows = ?, notes = ? where id = ?",
            (utc_now_iso(), pages_scanned, new_rows, f"incremental_qatarliving;per_page={args.per_page}", run_id),
        )
        conn.commit()
        conn.close()

    print(f"source=qatarliving new_rows={new_rows}")


if __name__ == "__main__":
    main()
