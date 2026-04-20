#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import socket
import sqlite3
import cloudscraper
import time
from datetime import datetime, timezone
from pathlib import Path

from mzad_push import get_vapid_settings, send_web_push, webpush_is_available
from mzad_whatsapp import send_whatsapp_message, whatsapp_is_configured


def check_dns(host="api.mzadqatar.com"):
    try:
        socket.setdefaulttimeout(5)
        socket.gethostbyname(host)
    except socket.gaierror as e:
        import sys
        print(f"[FATAL] DNS resolution failed: {e}. Check /etc/resolv.conf", file=sys.stderr)
        sys.exit(1)

check_dns()


API_URL = "https://api.mzadqatar.com/get-category-data"
DETAIL_URL = "https://api.mzadqatar.com/get-product-details"
COMMENTS_URL = "https://api.mzadqatar.com/get-product-comments"
TOKEN = (
    "Bearer Guest/User/Token/6SP4ZCaQdXwr0XPoB7hrhNysc2Y4yXwI4fA2sbENioAFuGE0FDfKRkuR7bcDQztmEKmxGkBM41Sht6zQLkNTlYeKUyBHfv7nun5BHmwQ87kYr69UfHL/Guest/User/Token"
)
_HEADERS = {
    "Content-Type": "application/json",
    "Guest-Token": TOKEN,
    "X-Requested-With": "XMLHttpRequest",
    "Accept-Language": "en",
    "Accept": "application/json, text/plain, */*",
}
_scraper = cloudscraper.create_scraper()
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


def prop(props: list[dict], key: str) -> str:
    for item in props or []:
        if key in item:
            v = item[key]
            return "" if v is None else str(v)
    return ""


def prop_any(props: list[dict], keys: list[str]) -> str:
    for key in keys:
        val = prop(props, key)
        if val:
            return val
    return ""


def is_car(item: dict) -> bool:
    props = item.get("properties", [])
    category = prop(props, "Category").lower()
    return category == "cars"


def fetch_page(page: int, number_per_page: int) -> dict:
    body = {
        "advertiseResolution": "large",
        "countOfRow": 2,
        "isAdsSupported": True,
        "numberperpage": number_per_page,
        "lastupdatetime": 0,
        "page": page,
        "productId": 1,
        "isNewUpdate": 1,
        "searchStr": "",
        "productsLang": "en",
        "categoryAdvertiseTypeId": "0",
        "userIpAddress": "127.0.0.1",
        "userDeviceModel": "local",
        "userDeviceLanguage": "en",
        "SetAdvertiseLanguage": "en",
        "userDeviceType": "Local",
        "versionNumber": "1.0",
        "userMacAddress": "",
    }
    resp = _scraper.post(API_URL, json=body, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_product_details(product_id: str) -> dict:
    body = {"productId": str(product_id), "id": str(product_id), "productsLang": "en"}
    resp = _scraper.post(DETAIL_URL, json=body, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_product_comments(product_id: str) -> dict:
    body = {
        "productId": str(product_id),
        "id": str(product_id),
        "productsLang": "en",
        "page": 1,
        "numberperpage": 100,
    }
    resp = _scraper.post(COMMENTS_URL, json=body, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def normalize_row(item: dict) -> dict:
    props = item.get("properties", [])
    image_urls = item.get("productImages") or []
    return {
        "product_id": str(item.get("productId", "")).strip(),
        "title": str(item.get("productName", "")).strip(),
        "price_qar": parse_int(item.get("productPrice")),
        "make": prop(props, "Motor type").strip(),
        "class_name": prop(props, "Class").strip(),
        "model": prop(props, "Model").strip(),
        "manufacture_year": parse_int(prop(props, "Manufacture Year")),
        "km": parse_int(prop(props, "Km")),
        "car_type": prop(props, "Car Type").strip(),
        "gear_type": prop(props, "Gear Type").strip(),
        "fuel_type": prop(props, "Fuel Type").strip(),
        "warranty_status": prop_any(
            props,
            ["Guarantee", "Warranty", "Warranty Status", "Car Warranty", "Warranty status"],
        ).strip(),
        "cylinder_count": parse_int(
            prop_any(
                props,
                ["Number of Cylinders", "Cylinders", "Cylinder", "Cylinder Count", "Cylender", "Cylender Count"],
            )
        ),
        "seller_name": prop_any(props, ["Seller Name", "Owner Name", "Contact Name"]).strip() or str(item.get("ownerName", "")).strip(),
        "seller_phone": prop_any(props, ["Seller Phone", "Phone", "Mobile", "Contact Number"]).strip() or str(item.get("ownerPhone", "")).strip(),
        "seller_type": prop_any(props, ["Seller Type", "Owner Type"]).strip(),
        "city": prop(props, "City").strip(),
        "url": str(item.get("productUrl", "")).strip(),
        "main_image_url": str(item.get("productMainImage", "")).strip(),
        "image_urls_json": json.dumps(image_urls, ensure_ascii=False),
        "advertise_time_formatted": str(item.get("advertiseTimeFormatted", "")).strip(),
        "date_of_advertise": str(item.get("dateOfAdvertise", "")).strip(),
    }


def enrich_row_with_detail(row: dict) -> dict:
    pid = str(row.get("product_id", "")).strip()
    if not pid:
        return row
    details = fetch_product_details(pid)
    comments = fetch_product_comments(pid)
    detail_images = details.get("productImages") or []
    props = details.get("properties") or []
    row["seller_whatsapp"] = str(details.get("userWhatsApp", "")).strip()
    row["seller_user_id"] = str(details.get("userId", "")).strip()
    row["is_company"] = str(details.get("isCompany", "")).strip()
    row["description"] = str(details.get("productDescription", "")).strip()
    row["all_image_urls_json"] = json.dumps(detail_images, ensure_ascii=False)
    row["properties_json"] = json.dumps(props, ensure_ascii=False)
    row["comments_count"] = parse_int(comments.get("commentsCount")) or 0
    row["comments_json"] = json.dumps(comments.get("productComments") or [], ensure_ascii=False)

    # Keep normalized display fields aligned with detail payload when available.
    if details.get("productMainImage"):
        row["main_image_url"] = str(details.get("productMainImage", "")).strip()
    if details.get("productPrice"):
        row["price_qar"] = parse_int(details.get("productPrice"))
    if details.get("productName"):
        row["title"] = str(details.get("productName", "")).strip()
    if details.get("dateOfAdvertise"):
        row["date_of_advertise"] = str(details.get("dateOfAdvertise", "")).strip()
    if details.get("cityName"):
        row["city"] = str(details.get("cityName", "")).strip()
    if not row.get("seller_name"):
        row["seller_name"] = str(details.get("userName", "")).strip()
    if not row.get("seller_phone"):
        row["seller_phone"] = str(details.get("userNumber", "")).strip()
    return row


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
            city text,
            url text,
            main_image_url text,
            image_urls_json text,
            advertise_time_formatted text,
            date_of_advertise text
        )
        """
    )
    existing_cols = {
        str(r[1]) for r in conn.execute(f"pragma table_info({TABLE_NAME})").fetchall()
    }
    if "warranty_status" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column warranty_status text")
    if "cylinder_count" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column cylinder_count integer")
    if "seller_name" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column seller_name text")
    if "seller_phone" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column seller_phone text")
    if "seller_type" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column seller_type text")
    if "seller_whatsapp" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column seller_whatsapp text")
    if "seller_user_id" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column seller_user_id text")
    if "is_company" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column is_company text")
    if "description" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column description text")
    if "all_image_urls_json" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column all_image_urls_json text")
    if "properties_json" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column properties_json text")
    if "comments_count" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column comments_count integer")
    if "comments_json" not in existing_cols:
        conn.execute(f"alter table {TABLE_NAME} add column comments_json text")
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
    conn.execute(
        """
        create table if not exists user_alerts (
            id integer primary key autoincrement,
            user_key text not null,
            make text,
            class_name text,
            model text,
            city text,
            search_text text,
            min_year integer,
            max_year integer,
            min_price_qar integer,
            max_price_qar integer,
            min_km integer,
            max_km integer,
            deals_only integer default 0,
            min_discount_pct real,
            max_discount_pct real,
            min_peer_count integer,
            max_peer_count integer,
            active integer default 1,
            created_at text not null,
            updated_at text not null
        )
        """
    )
    conn.execute(
        """
        create table if not exists user_notifications (
            id integer primary key autoincrement,
            user_key text not null,
            alert_id integer not null,
            product_id text not null,
            title text,
            price_qar integer,
            city text,
            url text,
            main_image_url text,
            listing_date text,
            message text,
            is_read integer default 0,
            created_at text not null,
            foreign key(alert_id) references user_alerts(id),
            unique(alert_id, product_id)
        )
        """
    )
    conn.execute(
        """
        create table if not exists web_push_subscriptions (
            id integer primary key autoincrement,
            user_key text not null,
            endpoint text not null unique,
            p256dh text,
            auth text,
            subscription_json text not null,
            active integer default 1,
            created_at text not null,
            updated_at text not null,
            last_success_at text,
            last_error text
        )
        """
    )
    conn.execute(
        """
        create table if not exists user_channels (
            user_key text primary key,
            whatsapp_number text,
            whatsapp_enabled integer default 0,
            updated_at text not null
        )
        """
    )
    conn.execute(
        """
        create table if not exists whatsapp_delivery_log (
            id integer primary key autoincrement,
            user_key text not null,
            product_id text not null,
            channel text not null default 'whatsapp',
            status text not null,
            provider_message_id text,
            error_text text,
            created_at text not null,
            unique(user_key, product_id, channel)
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
            row.get("seller_whatsapp"),
            row.get("seller_user_id"),
            row.get("is_company"),
            row["city"],
            row["url"],
            row["main_image_url"],
            row["image_urls_json"],
            row.get("all_image_urls_json") or "[]",
            row.get("properties_json") or "[]",
            parse_int(row.get("comments_count")) or 0,
            row.get("comments_json") or "[]",
            row.get("description"),
            row["advertise_time_formatted"],
            row["date_of_advertise"],
        ),
    )


def row_matches_alert(row: dict, alert: sqlite3.Row) -> bool:
    title = (row.get("title") or "").lower()
    make = (row.get("make") or "").lower()
    class_name = (row.get("class_name") or "").lower()
    model = (row.get("model") or "").lower()
    city = (row.get("city") or "").lower()
    price = row.get("price_qar")
    year = row.get("manufacture_year")
    km = row.get("km")

    if alert["make"] and make != str(alert["make"]).strip().lower():
        return False
    if alert["class_name"] and class_name != str(alert["class_name"]).strip().lower():
        return False
    if alert["model"] and model != str(alert["model"]).strip().lower():
        return False
    if alert["city"] and city != str(alert["city"]).strip().lower():
        return False
    if alert["search_text"] and str(alert["search_text"]).strip().lower() not in title:
        return False
    if alert["min_year"] is not None and (year is None or year < int(alert["min_year"])):
        return False
    if alert["max_year"] is not None and (year is None or year > int(alert["max_year"])):
        return False
    if alert["min_price_qar"] is not None and (price is None or price < int(alert["min_price_qar"])):
        return False
    if alert["max_price_qar"] is not None and (price is None or price > int(alert["max_price_qar"])):
        return False
    if alert["min_km"] is not None and (km is None or km < int(alert["min_km"])):
        return False
    if alert["max_km"] is not None and (km is None or km > int(alert["max_km"])):
        return False
    return True


def send_push_for_user(conn: sqlite3.Connection, user_key: str, row: dict, message: str) -> int:
    if not user_key:
        return 0
    if not get_vapid_settings() or not webpush_is_available():
        return 0
    sent = 0
    subscriptions = conn.execute(
        "select id, subscription_json from web_push_subscriptions where user_key = ? and active = 1",
        (user_key,),
    ).fetchall()
    for sub in subscriptions:
        subscription = json.loads(sub["subscription_json"])
        ok, err = send_web_push(
            subscription,
            {
                "title": "New Car Match",
                "body": message,
                "url": row.get("url") or "",
                "icon": row.get("main_image_url") or "",
            },
        )
        if ok:
            sent += 1
            conn.execute(
                "update web_push_subscriptions set last_success_at = ?, last_error = null, updated_at = ? where id = ?",
                (utc_now_iso(), utc_now_iso(), int(sub["id"])),
            )
        else:
            conn.execute(
                "update web_push_subscriptions set last_error = ?, updated_at = ? where id = ?",
                ((err or "push_send_failed")[:800], utc_now_iso(), int(sub["id"])),
            )
    return sent


def send_whatsapp_for_user(conn: sqlite3.Connection, user_key: str, row: dict, message: str) -> int:
    if not user_key or not whatsapp_is_configured():
        return 0

    target = conn.execute(
        "select whatsapp_number, whatsapp_enabled from user_channels where user_key = ?",
        (user_key,),
    ).fetchone()
    if not target or not target["whatsapp_number"] or int(target["whatsapp_enabled"] or 0) != 1:
        return 0

    # Prevent duplicate WhatsApp sends for the same user/product.
    existing = conn.execute(
        "select 1 from whatsapp_delivery_log where user_key = ? and product_id = ? and channel = 'whatsapp'",
        (user_key, row["product_id"]),
    ).fetchone()
    if existing:
        return 0

    body = f"{message}\n{row.get('url') or ''}".strip()
    ok, resp = send_whatsapp_message(str(target["whatsapp_number"]), body)
    conn.execute(
        """
        insert or ignore into whatsapp_delivery_log
        (user_key, product_id, channel, status, provider_message_id, error_text, created_at)
        values (?, ?, 'whatsapp', ?, ?, ?, ?)
        """,
        (
            user_key,
            row["product_id"],
            "sent" if ok else "failed",
            resp if ok else None,
            None if ok else (resp or "")[:800],
            utc_now_iso(),
        ),
    )
    return 1 if ok else 0


def create_notifications_for_new_row(conn: sqlite3.Connection, row: dict) -> tuple[int, int, int]:
    alerts = conn.execute("select * from user_alerts where active = 1").fetchall()
    created = 0
    pushes_sent = 0
    listing_date = row.get("date_of_advertise") or row.get("advertise_time_formatted") or ""
    whatsapp_sent = 0
    for alert in alerts:
        if not row_matches_alert(row, alert):
            continue
        price_label = "N/A" if row.get("price_qar") is None else f"{row['price_qar']:,} QAR"
        msg = f"New match: {row.get('title') or 'Car listing'} | {price_label} | {row.get('city') or 'Unknown city'}"
        cur = conn.execute(
            """
            insert or ignore into user_notifications
            (user_key, alert_id, product_id, title, price_qar, city, url, main_image_url, listing_date, message, is_read, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                str(alert["user_key"]),
                int(alert["id"]),
                row["product_id"],
                row.get("title"),
                row.get("price_qar"),
                row.get("city"),
                row.get("url"),
                row.get("main_image_url"),
                listing_date,
                msg,
                utc_now_iso(),
            ),
        )
        if cur.rowcount > 0:
            created += 1
            pushes_sent += send_push_for_user(conn, str(alert["user_key"]), row, msg)
            whatsapp_sent += send_whatsapp_for_user(conn, str(alert["user_key"]), row, msg)
    return created, pushes_sent, whatsapp_sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental Mzad API collector")
    parser.add_argument("--max-pages", type=int, default=80)
    parser.add_argument("--numberperpage", type=int, default=50)
    parser.add_argument("--stale-pages", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.2)
    parser.add_argument("--with-details", action="store_true", default=True)
    parser.add_argument("--details-for", choices=["all", "new"], default="all")
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
        (utc_now_iso(), "incremental"),
    ).lastrowid
    conn.commit()

    existing_ids = {str(r[0]) for r in conn.execute(f"select product_id from {TABLE_NAME}").fetchall()}
    stale_pages = 0
    new_rows = 0
    pages_scanned = 0
    created_notifications = 0
    push_messages_sent = 0
    whatsapp_messages_sent = 0

    try:
        for page in range(1, args.max_pages + 1):
            payload = fetch_page(page, args.numberperpage)
            products = payload.get("products", [])
            pages_scanned += 1
            page_new = 0

            for item in products:
                pid = str(item.get("productId", "")).strip()
                if not pid:
                    continue
                if not is_car(item):
                    continue
                row = normalize_row(item)
                if not row["url"]:
                    continue
                is_new = pid not in existing_ids
                if args.with_details and (args.details_for == "all" or is_new):
                    try:
                        row = enrich_row_with_detail(row)
                    except Exception as exc:  # noqa: BLE001
                        print(f"warn detail_fetch_failed product_id={pid} error={exc}")
                upsert_listing(conn, row)
                if is_new:
                    existing_ids.add(pid)
                    page_new += 1
                    new_rows += 1
                    created, pushed, wa_sent = create_notifications_for_new_row(conn, row)
                    created_notifications += created
                    push_messages_sent += pushed
                    whatsapp_messages_sent += wa_sent

            conn.commit()
            print(f"page={page} products={len(products)} new_rows_page={page_new} total_new={new_rows}")

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
            (
                utc_now_iso(),
                pages_scanned,
                new_rows,
                f"notifications={created_notifications},push_sent={push_messages_sent},whatsapp_sent={whatsapp_messages_sent}",
                run_id,
            ),
        )
        conn.commit()
        conn.close()

    print(f"new_rows={new_rows}")
    print(f"notifications_created={created_notifications}")
    print(f"push_sent={push_messages_sent}")
    print(f"whatsapp_sent={whatsapp_messages_sent}")


if __name__ == "__main__":
    main()
