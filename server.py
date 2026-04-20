#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
from bisect import bisect_left, bisect_right
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from statistics import median
from urllib.parse import parse_qs, urlparse


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
ROOT_DIR = APP_DIR.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from mzad_push import get_vapid_settings, send_web_push, webpush_is_available
from mzad_whatsapp import normalize_whatsapp_number, send_whatsapp_message, whatsapp_is_configured
from lib.price_history import record_daily_price_history_snapshot
from workers.alert_matcher import run_alert_matcher
from workers.spotted_worker import compute_spotted_cache

DB_PATH = Path("/home/briyad/data/mzad-qatar/mzad_local.db")
UPLOADS_DIR = Path("/home/briyad/data/mzad-qatar/uploads")
TABLE_NAME = "car_listings_api_10000"
DEAL_CACHE: dict[str, dict] | None = None
TOKEN_TTL_SECONDS = 3600
REFRESH_TTL_SECONDS = 30 * 24 * 3600


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def utc_now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def plus_seconds_iso(seconds: int) -> str:
    return datetime.fromtimestamp(utc_now_ts() + seconds, tz=timezone.utc).replace(microsecond=0).isoformat()


def ensure_notification_schema() -> None:
    with db_connect() as conn:
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
        if "expected_price_qar" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column expected_price_qar real")
        if "discount_qar" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column discount_qar real")
        if "discount_pct" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column discount_pct real")
        if "peer_count" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column peer_count integer")
        if "mileage_window_km" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column mileage_window_km integer")
        if "deal_score" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column deal_score real")
        if "deal_reason" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column deal_reason text")
        if "deal_last_computed_at" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column deal_last_computed_at text")
        if "is_approved" not in existing_cols:
            conn.execute(f"alter table {TABLE_NAME} add column is_approved integer default 0")
        conn.execute(f"update {TABLE_NAME} set is_approved = 0 where is_approved is null")

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
        conn.execute("create index if not exists idx_user_alerts_user_active on user_alerts(user_key, active)")
        conn.execute(
            "create index if not exists idx_user_notifications_user_read_created on user_notifications(user_key, is_read, created_at desc)"
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
            "create index if not exists idx_web_push_subscriptions_user_active on web_push_subscriptions(user_key, active)"
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
            create table if not exists users (
                id text primary key,
                role text not null default 'user',
                email text unique,
                phone text unique,
                password_hash text,
                password_salt text,
                full_name text,
                is_active integer default 1,
                created_at text not null,
                updated_at text not null
            )
            """
        )
        conn.execute(
            """
            create table if not exists user_profiles (
                user_id text primary key,
                full_name text,
                avatar_url text,
                preferred_language text,
                timezone text,
                city text,
                updated_at text not null,
                foreign key(user_id) references users(id)
            )
            """
        )
        conn.execute(
            """
            create table if not exists auth_tokens (
                token text primary key,
                user_id text not null,
                token_type text not null,
                expires_at text not null,
                revoked integer default 0,
                created_at text not null,
                foreign key(user_id) references users(id)
            )
            """
        )
        conn.execute("create index if not exists idx_auth_tokens_user_type on auth_tokens(user_id, token_type)")
        conn.execute(
            """
            create table if not exists password_reset_tokens (
                token text primary key,
                user_id text not null,
                expires_at text not null,
                used integer default 0,
                created_at text not null,
                foreign key(user_id) references users(id)
            )
            """
        )
        conn.execute(
            """
            create table if not exists uploads (
                id integer primary key autoincrement,
                user_id text,
                filename text not null,
                mime_type text,
                local_path text not null,
                public_url text not null,
                size_bytes integer,
                created_at text not null
            )
            """
        )
        conn.execute(
            """
            create table if not exists favorites (
                id integer primary key autoincrement,
                user_id text,
                user_key text,
                product_id text not null,
                created_at text not null,
                unique(user_id, product_id),
                unique(user_key, product_id)
            )
            """
        )
        conn.execute("create index if not exists idx_favorites_user on favorites(user_id, user_key, created_at desc)")
        conn.execute(
            """
            create table if not exists listing_price_history (
                id integer primary key autoincrement,
                product_id text not null,
                price_qar real not null,
                recorded_at text not null default (datetime('now')),
                source text
            )
            """
        )
        conn.execute(
            "create unique index if not exists uq_price_history_product_day on listing_price_history(product_id, date(recorded_at))"
        )
        conn.execute(
            "create index if not exists idx_price_history_product on listing_price_history(product_id, recorded_at desc)"
        )
        conn.execute(
            """
            create table if not exists deal_score_log (
                id integer primary key autoincrement,
                product_id text not null,
                deal_score real,
                discount_pct real,
                peer_count integer,
                computed_at text not null default (datetime('now'))
            )
            """
        )
        conn.execute(
            "create index if not exists idx_deal_log_product on deal_score_log(product_id, computed_at desc)"
        )
        conn.execute(
            """
            create table if not exists spotted_cache (
                id integer primary key autoincrement,
                product_id text not null unique,
                score integer not null default 0,
                reasons text,
                cached_at text not null default (datetime('now'))
            )
            """
        )
        conn.execute(
            """
            create table if not exists listing_views (
                product_id text not null,
                viewed_at text not null default (datetime('now')),
                user_key text,
                ip_hash text
            )
            """
        )
        conn.execute("create index if not exists idx_views_product on listing_views(product_id, viewed_at desc)")
        conn.commit()


def parse_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def hash_password(password: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()


def make_tokens(conn: sqlite3.Connection, user_id: str) -> dict:
    access = secrets.token_urlsafe(32)
    refresh = secrets.token_urlsafe(40)
    now = utc_now_iso()
    conn.execute(
        "insert into auth_tokens(token, user_id, token_type, expires_at, revoked, created_at) values (?, ?, 'access', ?, 0, ?)",
        (access, user_id, plus_seconds_iso(TOKEN_TTL_SECONDS), now),
    )
    conn.execute(
        "insert into auth_tokens(token, user_id, token_type, expires_at, revoked, created_at) values (?, ?, 'refresh', ?, 0, ?)",
        (refresh, user_id, plus_seconds_iso(REFRESH_TTL_SECONDS), now),
    )
    return {
        "access_token": access,
        "refresh_token": refresh,
        "token_type": "Bearer",
        "expires_in": TOKEN_TTL_SECONDS,
    }


def is_token_valid(row: sqlite3.Row | None) -> bool:
    if not row:
        return False
    if int(row["revoked"] or 0) == 1:
        return False
    try:
        exp = datetime.fromisoformat(str(row["expires_at"]))
    except ValueError:
        return False
    return exp.timestamp() > utc_now_ts()


def auth_user_from_headers(handler: SimpleHTTPRequestHandler) -> dict | None:
    auth = str(handler.headers.get("Authorization") or "").strip()
    token = ""
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
    if not token:
        token = str(handler.headers.get("X-Guest-Token") or "").strip()
    if not token:
        return None
    with db_connect() as conn:
        row = conn.execute(
            """
            select t.token, t.user_id, t.token_type, t.expires_at, t.revoked,
                   u.role, u.email, u.phone, u.full_name, u.is_active, u.created_at
            from auth_tokens t
            join users u on u.id = t.user_id
            where t.token = ? and t.token_type = 'access'
            """,
            (token,),
        ).fetchone()
        if not is_token_valid(row):
            return None
        if int(row["is_active"] or 0) != 1:
            return None
        # BE-008: track last_seen for non-guest users
        if row["role"] != "guest":
            try:
                conn.execute(
                    "UPDATE users SET last_seen=? WHERE id=?",
                    (utc_now_iso(), row["user_id"]),
                )
                conn.commit()
            except Exception:  # noqa: BLE001
                pass
    return {
        "id": row["user_id"],
        "role": row["role"],
        "email": row["email"],
        "phone": row["phone"],
        "full_name": row["full_name"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
    }


def require_auth(handler: SimpleHTTPRequestHandler, allow_guest: bool = True) -> dict:
    user = auth_user_from_headers(handler)
    if not user:
        raise PermissionError("Unauthorized")
    if not allow_guest and user.get("role") == "guest":
        raise PermissionError("Guest token not allowed")
    return user


def format_listing_date(raw_value) -> str:
    if raw_value is None:
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""
    if text.isdigit():
        # API stores date in epoch milliseconds.
        try:
            ms = int(text)
            dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            return dt.date().isoformat()
        except (OSError, OverflowError, ValueError):
            return text
    return text


def looks_like_non_vehicle_offer(title: str) -> bool:
    t = (title or "").lower()
    bad_tokens = [
        "we buy all types of cars",
        "buying all kinds of cars",
        "maintenance",
        "garage",
        "service",
        "repair",
        "required",
        "wanted",
        "مطلوب",
        "صيانة",
        "كراج",
    ]
    return any(token in t for token in bad_tokens)


def load_all_rows() -> list[dict]:
    with db_connect() as conn:
        rows = conn.execute(
            f"""
            select product_id, title, price_qar, make, class_name, model, manufacture_year, km, city, url, main_image_url
            from {TABLE_NAME}
            """
        ).fetchall()
    return [dict(r) for r in rows]


def build_deal_cache() -> dict[str, dict]:
    global DEAL_CACHE
    if DEAL_CACHE is not None:
        return DEAL_CACHE

    rows = load_all_rows()
    normalized: dict[str, dict] = {}
    group: dict[tuple[str, str, int], list[tuple[int, str, float]]] = {}

    for row in rows:
        pid = str(row.get("product_id", "")).strip()
        price = parse_float(row.get("price_qar"))
        km = parse_float(row.get("km"))
        year = parse_int(str(row.get("manufacture_year", "")), 0)
        make = (row.get("make") or "").strip()
        cls = (row.get("class_name") or "").strip()
        title = row.get("title") or ""

        normalized[pid] = {
            "product_id": pid,
            "price": price,
            "km": km,
            "year": year,
            "make": make,
            "class_name": cls,
            "title": title,
        }

        valid = (
            price is not None
            and km is not None
            and 1000 <= price <= 3_000_000
            and 0 <= km <= 600_000
            and 1980 <= year <= 2030
            and make
            and cls
            and not looks_like_non_vehicle_offer(title)
        )
        if not valid:
            continue
        key = (make.lower(), cls.lower(), year)
        group.setdefault(key, []).append((int(km), pid, float(price)))

    for key in group:
        group[key].sort(key=lambda x: x[0])

    cache: dict[str, dict] = {}
    for pid, row in normalized.items():
        price = row["price"]
        km = row["km"]
        year = row["year"]
        make = row["make"]
        cls = row["class_name"]
        if (
            price is None
            or km is None
            or not make
            or not cls
            or not (1980 <= year <= 2030)
            or looks_like_non_vehicle_offer(row["title"])
        ):
            continue

        key = (make.lower(), cls.lower(), year)
        entries = group.get(key, [])
        if not entries:
            continue
        kms = [x[0] for x in entries]

        window = max(15000, int(km * 0.20))
        left = bisect_left(kms, int(km - window))
        right = bisect_right(kms, int(km + window))
        peer_prices = [p for _, p_id, p in entries[left:right] if p_id != pid]

        if len(peer_prices) < 5:
            window = max(30000, int(km * 0.35))
            left = bisect_left(kms, int(km - window))
            right = bisect_right(kms, int(km + window))
            peer_prices = [p for _, p_id, p in entries[left:right] if p_id != pid]

        if len(peer_prices) < 5:
            continue

        expected = float(median(peer_prices))
        if expected <= 0:
            continue
        discount = expected - float(price)
        discount_pct = (discount / expected) * 100.0
        score_multiplier = min(len(peer_prices) / 15.0, 1.0)
        deal_score = discount_pct * score_multiplier
        cache[pid] = {
            "expected_price_qar": round(expected, 2),
            "discount_qar": round(discount, 2),
            "discount_pct": round(discount_pct, 2),
            "peer_count": len(peer_prices),
            "mileage_window_km": window,
            "deal_score": round(deal_score, 2),
            "deal_reason": (
                f"{round(discount_pct,2)}% vs median of {len(peer_prices)} peer cars "
                f"(same make/class/year, mileage ±{window} km)"
            ),
        }

    DEAL_CACHE = cache
    return cache


def persist_deal_cache(cache: dict[str, dict]) -> int:
    now = utc_now_iso()
    with db_connect() as conn:
        conn.execute(
            f"""
            update {TABLE_NAME}
            set expected_price_qar = null,
                discount_qar = null,
                discount_pct = null,
                peer_count = 0,
                mileage_window_km = null,
                deal_score = null,
                deal_reason = '',
                deal_last_computed_at = ?
            """,
            (now,),
        )
        rows = [
            (
                metrics.get("expected_price_qar"),
                metrics.get("discount_qar"),
                metrics.get("discount_pct"),
                metrics.get("peer_count", 0),
                metrics.get("mileage_window_km"),
                metrics.get("deal_score"),
                metrics.get("deal_reason", ""),
                now,
                pid,
            )
            for pid, metrics in cache.items()
        ]
        conn.executemany(
            f"""
            update {TABLE_NAME}
            set expected_price_qar = ?,
                discount_qar = ?,
                discount_pct = ?,
                peer_count = ?,
                mileage_window_km = ?,
                deal_score = ?,
                deal_reason = ?,
                deal_last_computed_at = ?
            where product_id = ?
            """,
            rows,
        )
        conn.executemany(
            """
            insert into deal_score_log (product_id, deal_score, discount_pct, peer_count, computed_at)
            values (?, ?, ?, ?, ?)
            """,
            [
                (
                    pid,
                    metrics.get("deal_score"),
                    metrics.get("discount_pct"),
                    metrics.get("peer_count", 0),
                    now,
                )
                for pid, metrics in cache.items()
            ],
        )
        conn.commit()
    return len(rows)


def recompute_deals_in_db() -> dict:
    global DEAL_CACHE
    DEAL_CACHE = None
    cache = build_deal_cache()
    updated = persist_deal_cache(cache)
    run_spotted_cache_background()
    return {"ok": True, "updated_rows": updated, "computed_at": utc_now_iso()}


def run_spotted_cache_background() -> None:
    """Refresh spotted_cache in a fire-and-forget worker thread."""

    def _runner() -> None:
        try:
            with db_connect() as conn:
                compute_spotted_cache(conn, TABLE_NAME)
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            print(f"spotted cache refresh failed: {exc}")

    threading.Thread(target=_runner, daemon=True).start()


def dispatch_listing_notification(user_key: str, listing: dict) -> None:
    """Dispatch WhatsApp and web-push notifications for one alert match.

    Touches tables: user_channels, web_push_subscriptions.
    """
    title = f"New match: {(listing.get('make') or '').strip()} {(listing.get('class_name') or '').strip()}".strip()
    body = (
        f"{listing.get('title') or ''} · QAR {int(float(listing.get('price_qar') or 0)):,} · "
        f"{listing.get('city') or 'Qatar'}"
    )
    deep_link = str(listing.get("url") or "")
    with db_connect() as conn:
        channel = conn.execute(
            "select whatsapp_number, whatsapp_enabled from user_channels where user_key = ?",
            (user_key,),
        ).fetchone()
        if channel and channel["whatsapp_number"] and int(channel["whatsapp_enabled"] or 0) == 1:
            try:
                send_whatsapp_message(str(channel["whatsapp_number"]), f"{title}\n{body}\n{deep_link}".strip())
            except Exception as exc:  # noqa: BLE001
                print(f"whatsapp dispatch failed for {user_key}: {exc}")

        subs = conn.execute(
            "select id, subscription_json from web_push_subscriptions where user_key = ? and active = 1",
            (user_key,),
        ).fetchall()
        for sub in subs:
            try:
                subscription = json.loads(sub["subscription_json"])
                ok, msg = send_web_push(
                    subscription,
                    {
                        "title": title,
                        "body": body,
                        "url": deep_link,
                    },
                )
                if ok:
                    conn.execute(
                        "update web_push_subscriptions set last_success_at = ?, last_error = null, updated_at = ? where id = ?",
                        (utc_now_iso(), utc_now_iso(), sub["id"]),
                    )
                else:
                    conn.execute(
                        "update web_push_subscriptions set last_error = ?, updated_at = ? where id = ?",
                        (str(msg)[:800], utc_now_iso(), sub["id"]),
                    )
            except Exception as exc:  # noqa: BLE001
                conn.execute(
                    "update web_push_subscriptions set last_error = ?, updated_at = ? where id = ?",
                    (str(exc)[:800], utc_now_iso(), sub["id"]),
                )
        conn.commit()


def run_alert_matcher_background() -> None:
    """Run alert matching in a fire-and-forget worker thread."""

    def _runner() -> None:
        try:
            with db_connect() as conn:
                run_alert_matcher(conn, TABLE_NAME, dispatch_listing_notification)
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            print(f"alert matcher failed: {exc}")

    threading.Thread(target=_runner, daemon=True).start()


def fetch_spotted() -> dict:
    """Return top spotted listings joined from spotted_cache + listings table.

    Endpoint: GET /api/spotted.
    Touches tables: spotted_cache, car_listings_api_10000.
    """
    with db_connect() as conn:
        rows = conn.execute(
            f"""
            select l.*, s.score, s.reasons as spot_reasons
            from spotted_cache s
            join {TABLE_NAME} l on l.product_id = s.product_id
            where coalesce(l.is_approved, 0) = 1
            order by s.score desc
            limit 30
            """
        ).fetchall()
    payload = [listing_from_row(r) or {} for r in rows]
    for row in payload:
        row["spot_reasons"] = row.get("spot_reasons") or "[]"
    return {"rows": payload}


def track_listing_view(handler: SimpleHTTPRequestHandler, product_id: str, payload: dict) -> dict:
    """Track a listing view with hourly dedupe per (product_id, ip_hash).

    Endpoint: POST /api/listings/{product_id}/view.
    Touches table: listing_views.
    """
    user_key = str(payload.get("user_key") or "").strip() or None
    ip = ""
    if getattr(handler, "client_address", None):
        ip = str(handler.client_address[0] or "")
    ip_hash = hashlib.sha256(ip.encode("utf-8")).hexdigest()[:16]
    with db_connect() as conn:
        recent = conn.execute(
            """
            select 1
            from listing_views
            where product_id = ?
              and ip_hash = ?
              and datetime(viewed_at) > datetime('now', '-1 hour')
            limit 1
            """,
            (product_id, ip_hash),
        ).fetchone()
        if not recent:
            conn.execute(
                "insert into listing_views (product_id, user_key, ip_hash, viewed_at) values (?, ?, ?, ?)",
                (product_id, user_key, ip_hash, utc_now_iso()),
            )
            conn.commit()
    return {"ok": True}


def fetch_price_history(product_id: str) -> dict:
    """Fetch listing price history with joined deal score snapshots by date.

    Endpoint: GET /api/listings/{product_id}/price-history (internal).
    Touches tables: listing_price_history, deal_score_log.
    """
    with db_connect() as conn:
        rows = conn.execute(
            """
            select
              ph.price_qar,
              ph.recorded_at,
              dl.deal_score,
              dl.discount_pct
            from listing_price_history ph
            left join deal_score_log dl
              on dl.product_id = ph.product_id
             and date(dl.computed_at) = date(ph.recorded_at)
            where ph.product_id = ?
            order by datetime(ph.recorded_at) asc
            """,
            (product_id,),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


def fetch_summary() -> dict:
    def _iso_duration_seconds(started_at: str | None, finished_at: str | None) -> int | None:
        if not started_at or not finished_at:
            return None
        try:
            started = datetime.fromisoformat(str(started_at))
            finished = datetime.fromisoformat(str(finished_at))
            seconds = int((finished - started).total_seconds())
            return seconds if seconds >= 0 else None
        except ValueError:
            return None

    with db_connect() as conn:
        total = conn.execute(f"select count(*) as c from {TABLE_NAME}").fetchone()["c"]
        makes = conn.execute(
            f"""
            select coalesce(make, '') as make, count(*) as c
            from {TABLE_NAME}
            group by make
            order by c desc, make asc
            limit 10
            """
        ).fetchall()
        with_phone = conn.execute(
            f"""
            select count(*) as c
            from {TABLE_NAME}
            where seller_phone is not null and trim(seller_phone) <> ''
            """
        ).fetchone()["c"]
        unique_phones = conn.execute(
            f"""
            select count(distinct trim(seller_phone)) as c
            from {TABLE_NAME}
            where seller_phone is not null and trim(seller_phone) <> ''
            """
        ).fetchone()["c"]
        approved_count = conn.execute(
            f"select count(*) as c from {TABLE_NAME} where coalesce(is_approved, 0) = 1"
        ).fetchone()["c"]
        pending_count = conn.execute(
            f"select count(*) as c from {TABLE_NAME} where coalesce(is_approved, 0) = 0"
        ).fetchone()["c"]
        source_rows = conn.execute(
            f"""
            select
              case when product_id like 'ql_%' then 'qatarliving' else 'mzad' end as source,
              count(*) as listings,
              count(case when seller_phone is not null and trim(seller_phone) <> '' then 1 end) as listings_with_phone,
              count(distinct case when seller_phone is not null and trim(seller_phone) <> '' then trim(seller_phone) end) as unique_phones
            from {TABLE_NAME}
            group by source
            order by source asc
            """
        ).fetchall()
        fresh_stock_rows = conn.execute(
            f"""
            select make, count(*) as count
            from {TABLE_NAME}
            where coalesce(is_approved, 0) = 1
              and km < 10000
              and manufacture_year >= ?
            group by make
            order by count desc
            limit 5
            """,
            (datetime.now(timezone.utc).year - 2,),
        ).fetchall()
        deal_dist_row = conn.execute(
            f"""
            select
              sum(case when discount_pct < -15 then 1 else 0 end) as hot_count,
              sum(case when discount_pct between -15 and -5 then 1 else 0 end) as good_count,
              sum(case when discount_pct between -5 and 5 then 1 else 0 end) as fair_count,
              sum(case when discount_pct > 5 then 1 else 0 end) as overpriced_count,
              sum(case when peer_count < 3 or peer_count is null then 1 else 0 end) as unscored_count
            from {TABLE_NAME}
            where coalesce(is_approved, 0) = 1
            """
        ).fetchone()
        avg_by_make_rows = conn.execute(
            f"""
            select make, round(avg(price_qar), 0) as avg_price, count(*) as listing_count
            from {TABLE_NAME}
            where coalesce(is_approved, 0) = 1 and peer_count >= 3
            group by make
            order by listing_count desc
            limit 10
            """
        ).fetchall()

        recent_runs: list[sqlite3.Row] = []
        run_stats = {
            "totalRuns": 0,
            "totalNewRows": 0,
            "totalPagesScanned": 0,
            "lastFinishedAt": None,
            "avgNewRowsPerRun": 0.0,
        }
        try:
            recent_runs = conn.execute(
                """
                select id, started_at, finished_at, pages_scanned, new_rows, notes
                from collection_runs
                order by id desc
                limit 25
                """
            ).fetchall()
            totals = conn.execute(
                """
                select count(*) as total_runs,
                       coalesce(sum(new_rows), 0) as total_new_rows,
                       coalesce(sum(pages_scanned), 0) as total_pages_scanned,
                       max(finished_at) as last_finished_at
                from collection_runs
                """
            ).fetchone()
            total_runs = int(totals["total_runs"] or 0)
            total_new_rows = int(totals["total_new_rows"] or 0)
            total_pages_scanned = int(totals["total_pages_scanned"] or 0)
            run_stats = {
                "totalRuns": total_runs,
                "totalNewRows": total_new_rows,
                "totalPagesScanned": total_pages_scanned,
                "lastFinishedAt": totals["last_finished_at"],
                "avgNewRowsPerRun": round((total_new_rows / total_runs), 2) if total_runs else 0.0,
            }
        except sqlite3.OperationalError:
            # collection_runs may not exist on a fresh DB.
            pass

    runs_payload = [
        {
            "id": int(row["id"]),
            "startedAt": row["started_at"],
            "finishedAt": row["finished_at"],
            "durationSeconds": _iso_duration_seconds(row["started_at"], row["finished_at"]),
            "pagesScanned": int(row["pages_scanned"] or 0),
            "newRows": int(row["new_rows"] or 0),
            "notes": row["notes"] or "",
        }
        for row in recent_runs
    ]

    return {
        "totalListings": total,
        "topMakes": [{"name": row["make"] or "Unknown", "count": row["c"]} for row in makes],
        "phoneStats": {
            "listingsWithPhone": int(with_phone or 0),
            "uniquePhoneNumbers": int(unique_phones or 0),
            "coveragePct": round((float(with_phone) / float(total) * 100.0), 2) if total else 0.0,
        },
        "approvalStats": {
            "approved": int(approved_count or 0),
            "pending": int(pending_count or 0),
        },
        "sourceStats": [
            {
                "source": str(row["source"]),
                "listings": int(row["listings"] or 0),
                "listingsWithPhone": int(row["listings_with_phone"] or 0),
                "uniquePhoneNumbers": int(row["unique_phones"] or 0),
            }
            for row in source_rows
        ],
        "collectionRunStats": run_stats,
        "collectionRuns": runs_payload,
        "freshStock": [{"make": r["make"], "count": int(r["count"] or 0)} for r in fresh_stock_rows],
        "dealDistribution": {
            "hot_count": int((deal_dist_row["hot_count"] if deal_dist_row else 0) or 0),
            "good_count": int((deal_dist_row["good_count"] if deal_dist_row else 0) or 0),
            "fair_count": int((deal_dist_row["fair_count"] if deal_dist_row else 0) or 0),
            "overpriced_count": int((deal_dist_row["overpriced_count"] if deal_dist_row else 0) or 0),
            "unscored_count": int((deal_dist_row["unscored_count"] if deal_dist_row else 0) or 0),
        },
        "avgPriceByMake": [
            {
                "make": r["make"],
                "avg_price": int(r["avg_price"] or 0),
                "listing_count": int(r["listing_count"] or 0),
            }
            for r in avg_by_make_rows
        ],
    }


def fetch_ranges() -> dict:
    rows = load_all_rows()
    deal_cache = build_deal_cache()

    years = [parse_int(str(r.get("manufacture_year", "")), 0) for r in rows]
    years = [y for y in years if 1980 <= y <= 2035]
    prices = [parse_float(r.get("price_qar")) for r in rows]
    prices = [p for p in prices if p is not None and p >= 0]
    kms = [parse_float(r.get("km")) for r in rows]
    kms = [k for k in kms if k is not None and 0 <= k <= 1_000_000]
    discounts = [m["discount_pct"] for m in deal_cache.values() if m.get("discount_pct") is not None]
    peers = [m["peer_count"] for m in deal_cache.values() if m.get("peer_count") is not None]

    def mm(values, fallback_min, fallback_max):
        if not values:
            return {"min": fallback_min, "max": fallback_max}
        return {"min": int(min(values)), "max": int(max(values))}

    def percentile(values, q):
        if not values:
            return None
        arr = sorted(values)
        idx = int((len(arr) - 1) * q)
        return arr[max(0, min(idx, len(arr) - 1))]

    price_min = percentile(prices, 0.02)
    price_max = percentile(prices, 0.98)
    if price_min is None or price_max is None:
        price_range = mm(prices, 0, 1_000_000)
    else:
        price_range = {"min": int(max(0, price_min)), "max": int(max(price_min + 1, price_max))}

    discount_min = percentile(discounts, 0.02)
    discount_max = percentile(discounts, 0.98)
    if discount_min is None or discount_max is None:
        discount_range = mm(discounts, -50, 100)
    else:
        discount_range = {
            "min": int(max(-80, discount_min)),
            "max": int(min(100, max(discount_min + 1, discount_max))),
        }

    km_min = percentile(kms, 0.02)
    km_max = percentile(kms, 0.98)
    if km_min is None or km_max is None:
        km_range = mm(kms, 0, 400000)
    else:
        km_range = {"min": int(max(0, km_min)), "max": int(min(600000, max(km_min + 1, km_max)))}

    return {
        "year": mm(years, 2000, 2030),
        "price": price_range,
        "km": km_range,
        "discount_pct": discount_range,
        "peer_count": mm(peers, 1, 50),
    }


def fetch_listings(
    params: dict[str, list[str]],
    include_unapproved: bool = False,
    approval_filter: str = "1",
    include_views: bool = False,
) -> dict:
    search = params.get("search", [""])[0].strip().lower()
    make = params.get("make", [""])[0].strip()
    class_name = params.get("class_name", [""])[0].strip()
    model = params.get("model", [""])[0].strip()
    city = params.get("city", [""])[0].strip()
    source = params.get("source", ["all"])[0].strip().lower()
    sort = params.get("sort", ["price_desc"])[0]
    limit = max(1, min(parse_int(params.get("limit", ["100"])[0], 100), 1000))
    deals_only = params.get("deals_only", ["0"])[0].strip() == "1"
    min_discount_pct = parse_float(params.get("min_discount_pct", ["0"])[0]) or 0.0
    max_discount_pct = parse_float(params.get("max_discount_pct", ["1000"])[0])
    if max_discount_pct is None:
        max_discount_pct = 1000.0
    min_peer_count = max(1, parse_int(params.get("min_peer_count", ["5"])[0], 5))
    max_peer_count = max(min_peer_count, parse_int(params.get("max_peer_count", ["1000"])[0], 1000))
    min_year = parse_int(params.get("min_year", ["0"])[0], 0)
    max_year = parse_int(params.get("max_year", ["9999"])[0], 9999)
    min_price = parse_float(params.get("min_price", ["0"])[0]) or 0.0
    max_price = parse_float(params.get("max_price", ["999999999"])[0])
    if max_price is None:
        max_price = 999999999.0
    min_km = parse_float(params.get("min_km", ["0"])[0]) or 0.0
    max_km = parse_float(params.get("max_km", ["999999999"])[0])
    if max_km is None:
        max_km = 999999999.0

    where_parts = []
    values: list = []

    if search:
        where_parts.append(
            "(lower(title) like ? or lower(make) like ? or lower(class_name) like ? or lower(city) like ?)"
        )
        token = f"%{search}%"
        values.extend([token, token, token, token])

    if make:
        where_parts.append("make = ?")
        values.append(make)
    if class_name:
        where_parts.append("class_name = ?")
        values.append(class_name)
    if model:
        where_parts.append("model = ?")
        values.append(model)
    if city:
        where_parts.append("city = ?")
        values.append(city)
    if source == "qatarliving":
        where_parts.append("product_id like 'ql_%'")
    elif source == "mzad":
        where_parts.append("product_id not like 'ql_%'")
    if approval_filter == "0":
        where_parts.append("coalesce(is_approved, 0) = 0")
    elif approval_filter == "1":
        where_parts.append("coalesce(is_approved, 0) = 1")
    elif approval_filter == "any":
        pass
    elif not include_unapproved:
        # Safe fallback for invalid values on public requests.
        where_parts.append("coalesce(is_approved, 0) = 1")

    where_sql = f"where {' and '.join(where_parts)}" if where_parts else ""
    views_sql = (
        ", (select count(*) from listing_views lv where lv.product_id = l.product_id) as view_count"
        if include_views
        else ""
    )
    query = (
        f"select product_id, title, price_qar, make, class_name, model, manufacture_year, km, warranty_status, cylinder_count, "
        f"seller_name, seller_phone, seller_type, seller_whatsapp, seller_user_id, is_company, city, url, "
        f"main_image_url, image_urls_json, all_image_urls_json, properties_json, comments_count, comments_json, description, "
        f"expected_price_qar, discount_qar, discount_pct, peer_count, mileage_window_km, deal_score, deal_reason, deal_last_computed_at, is_approved, "
        f"date_of_advertise, advertise_time_formatted{views_sql} "
        f"from {TABLE_NAME} l {where_sql}"
    )

    with db_connect() as conn:
        rows = [dict(row) for row in conn.execute(query, values).fetchall()]
        makes = conn.execute(
            f"select distinct make from {TABLE_NAME} where make is not null and make <> '' order by make asc"
        ).fetchall()
        classes = conn.execute(
            f"select distinct class_name from {TABLE_NAME} where class_name is not null and class_name <> '' order by class_name asc"
        ).fetchall()
        models = conn.execute(
            f"select distinct model from {TABLE_NAME} where model is not null and model <> '' order by model asc"
        ).fetchall()
        cities = conn.execute(
            f"select distinct city from {TABLE_NAME} where city is not null and city <> '' order by city asc"
        ).fetchall()

    for row in rows:
        row["listing_date"] = format_listing_date(row.get("date_of_advertise"))
        year_raw = row.get("manufacture_year")
        row["manufacture_year"] = "" if year_raw is None else str(year_raw)
        date_raw = row.get("date_of_advertise")
        row["date_of_advertise_ms"] = int(str(date_raw)) if str(date_raw).isdigit() else 0
        # Normalize nullable deal fields for stable API shape.
        row["peer_count"] = parse_int(row.get("peer_count"), 0) or 0
        row["deal_reason"] = row.get("deal_reason") or ""
        row["is_approved"] = bool(parse_int(row.get("is_approved"), 0))
        if "view_count" in row:
            row["view_count"] = parse_int(row.get("view_count"), 0) or 0

    if deals_only:
        rows = [
            r
            for r in rows
            if r["deal_score"] is not None
            and r["discount_pct"] >= min_discount_pct
            and r["discount_pct"] <= max_discount_pct
            and r["peer_count"] >= min_peer_count
            and r["peer_count"] <= max_peer_count
        ]

    rows = [
        r
        for r in rows
        if (
            parse_int(str(r.get("manufacture_year", "")), 0) >= min_year
            and parse_int(str(r.get("manufacture_year", "")), 0) <= max_year
            and (parse_float(r.get("price_qar")) or 0) >= min_price
            and (parse_float(r.get("price_qar")) or 0) <= max_price
            and (parse_float(r.get("km")) or 0) >= min_km
            and (parse_float(r.get("km")) or 0) <= max_km
        )
    ]

    if not deals_only:
        rows = [
            r
            for r in rows
            if (
                (r["discount_pct"] is None or (r["discount_pct"] >= min_discount_pct and r["discount_pct"] <= max_discount_pct))
                and r["peer_count"] >= min_peer_count
                and r["peer_count"] <= max_peer_count
            )
        ]

    if sort == "deal_desc":
        rows.sort(
            key=lambda r: (
                -999999 if r["deal_score"] is None else r["deal_score"],
                -999999 if r["discount_pct"] is None else r["discount_pct"],
            ),
            reverse=True,
        )
    elif sort == "listing_date_desc":
        rows.sort(key=lambda r: parse_int(str(r.get("date_of_advertise_ms", 0)), 0), reverse=True)
    elif sort == "listing_date_asc":
        rows.sort(key=lambda r: parse_int(str(r.get("date_of_advertise_ms", 0)), 0))
    elif sort == "discount_desc":
        rows.sort(
            key=lambda r: (-999999 if r["discount_pct"] is None else r["discount_pct"]),
            reverse=True,
        )
    elif sort == "discount_asc":
        rows.sort(
            key=lambda r: (999999 if r["discount_pct"] is None else r["discount_pct"]),
        )
    elif sort == "price_asc":
        rows.sort(key=lambda r: (float("inf") if parse_float(r["price_qar"]) is None else parse_float(r["price_qar"])))
    elif sort == "year_desc":
        rows.sort(key=lambda r: parse_int(str(r.get("manufacture_year", "")), 0), reverse=True)
    elif sort == "km_asc":
        rows.sort(key=lambda r: (float("inf") if parse_float(r["km"]) is None else parse_float(r["km"])))
    elif sort == "title_asc":
        rows.sort(key=lambda r: (r.get("title") or "").lower())
    else:
        rows.sort(key=lambda r: (-1 if parse_float(r["price_qar"]) is None else parse_float(r["price_qar"])), reverse=True)

    return {
        "rows": rows[:limit],
        "makes": [r["make"] for r in makes],
        "classes": [r["class_name"] for r in classes],
        "models": [r["model"] for r in models],
        "cities": [r["city"] for r in cities],
    }


def estimate_value(params: dict[str, list[str]]) -> dict:
    make = params.get("make", [""])[0].strip()
    class_name = params.get("class_name", [""])[0].strip()
    model = params.get("model", [""])[0].strip()
    year = parse_int(params.get("year", ["0"])[0], 0)
    km = parse_float(params.get("km", ["0"])[0]) or 0.0

    if not make or not class_name or year <= 0:
        raise ValueError("make, class_name, and year are required")

    def percentile(values: list[float], q: float) -> float:
        arr = sorted(values)
        if not arr:
            return 0.0
        idx = int((len(arr) - 1) * q)
        return float(arr[max(0, min(idx, len(arr) - 1))])

    query = f"""
        select price_qar, km, manufacture_year, model
        from {TABLE_NAME}
        where make = ?
          and class_name = ?
          and price_qar is not null
          and km is not null
          and manufacture_year is not null
          and price_qar between 1000 and 3000000
          and km between 0 and 700000
    """
    vals: list = [make, class_name]
    if model:
        query += " and model = ?"
        vals.append(model)

    with db_connect() as conn:
        rows = conn.execute(query, vals).fetchall()

    base = [
        dict(price=float(r["price_qar"]), km=float(r["km"]), year=int(r["manufacture_year"]))
        for r in rows
    ]

    if not base:
        return {"ok": False, "reason": "no_comparables"}

    year_window = 0
    km_window = max(15000.0, km * 0.2)

    def filter_peers(yw: int, kw: float) -> list[dict]:
        return [
            r
            for r in base
            if abs(int(r["year"]) - year) <= yw and abs(float(r["km"]) - km) <= kw
        ]

    peers = filter_peers(year_window, km_window)
    if len(peers) < 8:
        year_window = 1
        km_window = max(30000.0, km * 0.35)
        peers = filter_peers(year_window, km_window)
    if len(peers) < 8:
        year_window = 2
        km_window = max(45000.0, km * 0.5)
        peers = filter_peers(year_window, km_window)

    if len(peers) < 4:
        return {"ok": False, "reason": "not_enough_peers", "peer_count": len(peers)}

    prices = [float(p["price"]) for p in peers]
    low = percentile(prices, 0.2)
    fair = percentile(prices, 0.5)
    high = percentile(prices, 0.8)

    return {
        "ok": True,
        "make": make,
        "class_name": class_name,
        "model": model,
        "year": year,
        "km": int(km),
        "peer_count": len(peers),
        "year_window": year_window,
        "km_window": int(km_window),
        "low_price_qar": round(low, 0),
        "fair_price_qar": round(fair, 0),
        "high_price_qar": round(high, 0),
    }


def fetch_listing_peers(product_id: str) -> dict:
    """Return comparable peers and pricing baseline for one approved listing.

    Endpoint: GET /api/listings/{product_id}/peers.
    Touches table: car_listings_api_10000.
    """
    with db_connect() as conn:
        row = conn.execute(
            f"""
            select product_id, title, price_qar, make, class_name, model, manufacture_year, km,
                   main_image_url, city, discount_pct, deal_last_computed_at
            from {TABLE_NAME}
            where product_id = ? and coalesce(is_approved, 0) = 1
            """,
            (product_id,),
        ).fetchone()
        if not row:
            raise ValueError("listing not found")
        target = dict(row)

        make = str(target.get("make") or "").strip()
        class_name = str(target.get("class_name") or "").strip()
        year = parse_int(target.get("manufacture_year"), 0)
        km = parse_float(target.get("km"))
        price = parse_float(target.get("price_qar"))
        if not make or not class_name or year <= 0 or km is None or price is None:
            raise ValueError("listing has insufficient data for peers")

        peers_rows = conn.execute(
            f"""
            select product_id, title, price_qar, km, manufacture_year, main_image_url, city, discount_pct
            from {TABLE_NAME}
            where coalesce(is_approved, 0) = 1
              and product_id <> ?
              and make = ?
              and class_name = ?
              and manufacture_year = ?
              and price_qar is not null
              and km is not null
              and price_qar between 1000 and 3000000
              and km between 0 and 700000
            """,
            (product_id, make, class_name, year),
        ).fetchall()

    peers_base = [dict(r) for r in peers_rows]
    windows = [
        max(15000, int(km * 0.20)),
        max(30000, int(km * 0.35)),
        max(45000, int(km * 0.50)),
    ]
    selected: list[dict] = []
    window_used = windows[-1]
    for w in windows:
        candidate = [
            p for p in peers_base
            if abs((parse_float(p.get("km")) or 0.0) - km) <= w
        ]
        if candidate:
            selected = candidate
            window_used = w
        if len(candidate) >= 5:
            selected = candidate
            window_used = w
            break

    if not selected:
        return {
            "product_id": product_id,
            "computed_at": utc_now_iso(),
            "peer_count": 0,
            "expected_price_qar": None,
            "mileage_window_km": window_used,
            "peers": [],
        }

    prices = [float(parse_float(p.get("price_qar")) or 0.0) for p in selected if parse_float(p.get("price_qar")) is not None]
    expected = float(median(prices)) if prices else 0.0

    peers_payload = []
    for p in selected:
        peer_price = parse_float(p.get("price_qar")) or 0.0
        peer_km = parse_int(p.get("km"), 0)
        peer_discount_pct = ((expected - peer_price) / expected * 100.0) if expected > 0 else None
        peer_pid = str(p.get("product_id") or "")
        peers_payload.append(
            {
                "product_id": peer_pid,
                "title": p.get("title") or "",
                "price_qar": peer_price,
                "km": peer_km,
                "manufacture_year": parse_int(p.get("manufacture_year"), 0),
                "main_image_url": p.get("main_image_url") or "",
                "city": p.get("city") or "",
                "discount_pct": round(peer_discount_pct, 2) if peer_discount_pct is not None else None,
                "source": "qatarliving" if peer_pid.startswith("ql_") else "mzad",
            }
        )

    peers_payload.sort(key=lambda x: (abs((x.get("km") or 0) - int(km)), x.get("price_qar") or 0))

    return {
        "product_id": product_id,
        "computed_at": utc_now_iso(),
        "peer_count": len(peers_payload),
        "expected_price_qar": round(expected, 0) if expected > 0 else None,
        "mileage_window_km": window_used,
        "peers": peers_payload,
    }


def load_json_body(handler: SimpleHTTPRequestHandler) -> dict:
    content_length = parse_int(handler.headers.get("Content-Length"), 0)
    if content_length <= 0:
        return {}
    raw = handler.rfile.read(content_length)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def row_to_user(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    return {
        "id": row["id"],
        "role": row["role"],
        "email": row["email"],
        "phone": row["phone"],
        "full_name": row["full_name"],
        "is_active": bool(row["is_active"]),
        "created_at": row["created_at"],
    }


def auth_guest_login() -> dict:
    now = utc_now_iso()
    user_id = f"guest_{secrets.token_hex(8)}"
    with db_connect() as conn:
        conn.execute(
            """
            insert into users(id, role, full_name, is_active, created_at, updated_at)
            values (?, 'guest', ?, 1, ?, ?)
            """,
            (user_id, "Guest User", now, now),
        )
        tokens = make_tokens(conn, user_id)
        conn.commit()
    return tokens


def auth_register(payload: dict) -> tuple[dict, int]:
    email = str(payload.get("email", "")).strip().lower()
    password = str(payload.get("password", ""))
    full_name = str(payload.get("full_name", "")).strip()
    phone = str(payload.get("phone", "")).strip() or None
    allowed_roles = {'user', 'dealer'}
    role = str(payload.get('role', 'user')).strip().lower()
    if role not in allowed_roles:
        role = 'user'
    if not email or "@" not in email:
        raise ValueError("valid email is required")
    if len(password) < 8:
        raise ValueError("password must be at least 8 chars")
    if not full_name:
        raise ValueError("full_name is required")

    now = utc_now_iso()
    user_id = f"user_{secrets.token_hex(8)}"
    salt = secrets.token_hex(8)
    pwh = hash_password(password, salt)
    with db_connect() as conn:
        exists = conn.execute("select 1 from users where email = ?", (email,)).fetchone()
        if exists:
            raise ValueError("email already exists")
        conn.execute(
            """
            insert into users(id, role, email, phone, password_hash, password_salt, full_name, is_active, created_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (user_id, role, email, phone, pwh, salt, full_name, now, now),
        )
        conn.execute(
            """
            insert into user_profiles(user_id, full_name, preferred_language, timezone, updated_at)
            values (?, ?, 'en', 'UTC', ?)
            """,
            (user_id, full_name, now),
        )
        tokens = make_tokens(conn, user_id)
        conn.commit()
    return tokens, HTTPStatus.CREATED


def auth_login(payload: dict) -> dict:
    login = str(payload.get("login", "")).strip().lower()
    password = str(payload.get("password", ""))
    if not login or not password:
        raise ValueError("login and password are required")
    with db_connect() as conn:
        row = conn.execute(
            """
            select id, role, email, phone, password_hash, password_salt, is_active
            from users
            where lower(coalesce(email,'')) = ? or lower(coalesce(phone,'')) = ?
            limit 1
            """,
            (login, login),
        ).fetchone()
        if not row or int(row["is_active"] or 0) != 1:
            raise PermissionError("Invalid credentials")
        if row["role"] == "guest":
            raise PermissionError("Guest account cannot use password login")
        expected = hash_password(password, str(row["password_salt"]))
        if expected != str(row["password_hash"] or ""):
            raise PermissionError("Invalid credentials")
        tokens = make_tokens(conn, row["id"])
        conn.commit()
    return tokens


def auth_refresh(payload: dict) -> dict:
    refresh_token = str(payload.get("refresh_token", "")).strip()
    if not refresh_token:
        raise ValueError("refresh_token is required")
    with db_connect() as conn:
        row = conn.execute(
            "select token, user_id, token_type, expires_at, revoked from auth_tokens where token = ? and token_type = 'refresh'",
            (refresh_token,),
        ).fetchone()
        if not is_token_valid(row):
            raise PermissionError("Invalid refresh token")
        conn.execute("update auth_tokens set revoked = 1 where token = ?", (refresh_token,))
        tokens = make_tokens(conn, row["user_id"])
        conn.commit()
    return tokens


def auth_logout(handler: SimpleHTTPRequestHandler) -> dict:
    auth = str(handler.headers.get("Authorization") or "").strip()
    token = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    if not token:
        token = str(handler.headers.get("X-Guest-Token") or "").strip()
    if token:
        with db_connect() as conn:
            conn.execute("update auth_tokens set revoked = 1 where token = ?", (token,))
            conn.commit()
    return {"ok": True}


def find_user_by_login(conn: sqlite3.Connection, login: str) -> sqlite3.Row | None:
    l = (login or "").strip().lower()
    if not l:
        return None
    return conn.execute(
        """
        select id, role, email, phone, password_hash, password_salt, is_active, full_name, created_at
        from users
        where lower(coalesce(email,'')) = ? or lower(coalesce(phone,'')) = ?
        limit 1
        """,
        (l, l),
    ).fetchone()


def auth_change_password(handler: SimpleHTTPRequestHandler, payload: dict) -> dict:
    user = require_auth(handler, allow_guest=False)
    old_password = str(payload.get("old_password", ""))
    new_password = str(payload.get("new_password", ""))
    if len(new_password) < 8:
        raise ValueError("new_password must be at least 8 chars")
    with db_connect() as conn:
        row = conn.execute(
            "select id, password_hash, password_salt from users where id = ?",
            (user["id"],),
        ).fetchone()
        if not row:
            raise ValueError("user not found")
        expected = hash_password(old_password, str(row["password_salt"] or ""))
        if expected != str(row["password_hash"] or ""):
            raise PermissionError("Invalid current password")
        salt = secrets.token_hex(8)
        pwh = hash_password(new_password, salt)
        conn.execute(
            "update users set password_hash = ?, password_salt = ?, updated_at = ? where id = ?",
            (pwh, salt, utc_now_iso(), user["id"]),
        )
        # revoke old access/refresh tokens for security
        conn.execute("update auth_tokens set revoked = 1 where user_id = ?", (user["id"],))
        tokens = make_tokens(conn, user["id"])
        conn.commit()
    return tokens


def auth_request_reset(payload: dict) -> dict:
    login = str(payload.get("login", "")).strip()
    if not login:
        raise ValueError("login is required")
    with db_connect() as conn:
        user = find_user_by_login(conn, login)
        if not user or int(user["is_active"] or 0) != 1 or user["role"] == "guest":
            return {"ok": True}
        token = secrets.token_urlsafe(32)
        conn.execute(
            """
            insert into password_reset_tokens(token, user_id, expires_at, used, created_at)
            values (?, ?, ?, 0, ?)
            """,
            (token, user["id"], plus_seconds_iso(3600), utc_now_iso()),
        )
        conn.commit()
    # In production this should be sent via email/SMS, returned here for local/dev use.
    return {"ok": True, "reset_token": token}


def auth_reset_password(payload: dict) -> dict:
    token = str(payload.get("reset_token", "")).strip()
    new_password = str(payload.get("new_password", ""))
    if not token:
        raise ValueError("reset_token is required")
    if len(new_password) < 8:
        raise ValueError("new_password must be at least 8 chars")
    with db_connect() as conn:
        row = conn.execute(
            "select token, user_id, expires_at, used from password_reset_tokens where token = ?",
            (token,),
        ).fetchone()
        if not row:
            raise PermissionError("Invalid reset token")
        if int(row["used"] or 0) == 1:
            raise PermissionError("Reset token already used")
        try:
            exp = datetime.fromisoformat(str(row["expires_at"]))
        except ValueError:
            raise PermissionError("Invalid reset token") from None
        if exp.timestamp() <= utc_now_ts():
            raise PermissionError("Reset token expired")
        salt = secrets.token_hex(8)
        pwh = hash_password(new_password, salt)
        conn.execute(
            "update users set password_hash = ?, password_salt = ?, updated_at = ? where id = ?",
            (pwh, salt, utc_now_iso(), row["user_id"]),
        )
        conn.execute("update password_reset_tokens set used = 1 where token = ?", (token,))
        conn.execute("update auth_tokens set revoked = 1 where user_id = ?", (row["user_id"],))
        tokens = make_tokens(conn, row["user_id"])
        conn.commit()
    return tokens


def auth_admin_bootstrap(payload: dict) -> tuple[dict, int]:
    email = str(payload.get("email", "")).strip().lower()
    password = str(payload.get("password", ""))
    full_name = str(payload.get("full_name", "")).strip() or "Admin"
    if not email or "@" not in email:
        raise ValueError("valid email is required")
    if len(password) < 8:
        raise ValueError("password must be at least 8 chars")
    now = utc_now_iso()
    with db_connect() as conn:
        admins = conn.execute("select count(*) as c from users where role = 'admin' and is_active = 1").fetchone()["c"]
        if int(admins) > 0:
            raise PermissionError("Admin already exists")
        user_id = f"admin_{secrets.token_hex(8)}"
        salt = secrets.token_hex(8)
        pwh = hash_password(password, salt)
        conn.execute(
            """
            insert into users(id, role, email, password_hash, password_salt, full_name, is_active, created_at, updated_at)
            values (?, 'admin', ?, ?, ?, ?, 1, ?, ?)
            """,
            (user_id, email, pwh, salt, full_name, now, now),
        )
        conn.execute(
            """
            insert into user_profiles(user_id, full_name, preferred_language, timezone, updated_at)
            values (?, ?, 'en', 'UTC', ?)
            """,
            (user_id, full_name, now),
        )
        tokens = make_tokens(conn, user_id)
        conn.commit()
    return tokens, HTTPStatus.CREATED


def me(handler: SimpleHTTPRequestHandler) -> dict:
    return require_auth(handler, allow_guest=True)


def get_profile(handler: SimpleHTTPRequestHandler) -> dict:
    user = require_auth(handler, allow_guest=False)
    with db_connect() as conn:
        row = conn.execute(
            "select user_id, full_name, avatar_url, preferred_language, timezone, city, updated_at from user_profiles where user_id = ?",
            (user["id"],),
        ).fetchone()
    if row:
        return dict(row)
    return {
        "user_id": user["id"],
        "full_name": user.get("full_name") or "",
        "avatar_url": "",
        "preferred_language": "en",
        "timezone": "UTC",
        "city": "",
        "updated_at": utc_now_iso(),
    }


def upsert_profile(handler: SimpleHTTPRequestHandler, payload: dict) -> dict:
    user = require_auth(handler, allow_guest=False)
    now = utc_now_iso()
    with db_connect() as conn:
        current = conn.execute(
            "select user_id, full_name, avatar_url, preferred_language, timezone, city, updated_at from user_profiles where user_id = ?",
            (user["id"],),
        ).fetchone()
        base = dict(current) if current else {
            "full_name": user.get("full_name") or "",
            "avatar_url": "",
            "preferred_language": "en",
            "timezone": "UTC",
            "city": "",
        }
        for key in ["full_name", "avatar_url", "preferred_language", "timezone", "city"]:
            if key in payload and payload.get(key) is not None:
                base[key] = str(payload.get(key)).strip()
        conn.execute(
            """
            insert into user_profiles(user_id, full_name, avatar_url, preferred_language, timezone, city, updated_at)
            values (?, ?, ?, ?, ?, ?, ?)
            on conflict(user_id) do update set
              full_name=excluded.full_name,
              avatar_url=excluded.avatar_url,
              preferred_language=excluded.preferred_language,
              timezone=excluded.timezone,
              city=excluded.city,
              updated_at=excluded.updated_at
            """,
            (user["id"], base["full_name"], base["avatar_url"], base["preferred_language"], base["timezone"], base["city"], now),
        )
        conn.execute(
            "update users set full_name = ?, updated_at = ? where id = ?",
            (base["full_name"], now, user["id"]),
        )
        conn.commit()
    return get_profile(handler)


def require_admin(handler: SimpleHTTPRequestHandler) -> dict:
    user = require_auth(handler, allow_guest=False)
    if user.get("role") != "admin":
        raise PermissionError("Admin required")
    return user


def list_users(handler: SimpleHTTPRequestHandler) -> dict:
    require_admin(handler)
    with db_connect() as conn:
        rows = conn.execute(
            "select id, role, email, phone, full_name, is_active, created_at from users order by created_at desc"
        ).fetchall()
    return {"rows": [row_to_user(r) for r in rows]}


def get_user_by_id(handler: SimpleHTTPRequestHandler, user_id: str) -> dict:
    require_admin(handler)
    with db_connect() as conn:
        row = conn.execute(
            "select id, role, email, phone, full_name, is_active, created_at from users where id = ?",
            (user_id,),
        ).fetchone()
    if not row:
        raise ValueError("user not found")
    return row_to_user(row) or {}


def patch_user(handler: SimpleHTTPRequestHandler, user_id: str, payload: dict) -> dict:
    require_admin(handler)
    updates = []
    values: list = []
    if "role" in payload:
        role = str(payload.get("role", "")).strip()
        if role not in {"guest", "user", "dealer", "admin"}:
            raise ValueError("invalid role")
        updates.append("role = ?")
        values.append(role)
    if "is_active" in payload:
        updates.append("is_active = ?")
        values.append(1 if bool(payload.get("is_active")) else 0)
    if not updates:
        return get_user_by_id(handler, user_id)
    updates.append("updated_at = ?")
    values.append(utc_now_iso())
    values.append(user_id)
    with db_connect() as conn:
        conn.execute(f"update users set {', '.join(updates)} where id = ?", values)
        conn.commit()
    return get_user_by_id(handler, user_id)


def put_user(handler: SimpleHTTPRequestHandler, user_id: str, payload: dict) -> dict:
    require_admin(handler)
    role = str(payload.get("role", "")).strip()
    is_active_raw = payload.get("is_active")
    if role not in {"guest", "user", "dealer", "admin"}:
        raise ValueError("invalid role")
    if is_active_raw is None:
        raise ValueError("is_active is required")
    is_active = 1 if bool(is_active_raw) else 0
    with db_connect() as conn:
        conn.execute(
            "update users set role = ?, is_active = ?, updated_at = ? where id = ?",
            (role, is_active, utc_now_iso(), user_id),
        )
        conn.commit()
    return get_user_by_id(handler, user_id)


def delete_user(handler: SimpleHTTPRequestHandler, user_id: str) -> None:
    require_admin(handler)
    with db_connect() as conn:
        conn.execute("update users set is_active = 0, updated_at = ? where id = ?", (utc_now_iso(), user_id))
        conn.commit()


def listing_from_row(row: sqlite3.Row | None) -> dict | None:
    if not row:
        return None
    d = dict(row)
    d["listing_date"] = format_listing_date(d.get("date_of_advertise"))
    d["peer_count"] = parse_int(d.get("peer_count"), 0) or 0
    d["deal_reason"] = d.get("deal_reason") or ""
    d["is_approved"] = bool(parse_int(d.get("is_approved"), 0))
    if "view_count" in d:
        d["view_count"] = parse_int(d.get("view_count"), 0) or 0
    return d


def create_listing(handler: SimpleHTTPRequestHandler, payload: dict) -> tuple[dict, int]:
    require_auth(handler, allow_guest=False)
    pid = str(payload.get("product_id", "")).strip() or f"manual_{int(datetime.now(timezone.utc).timestamp()*1000)}_{secrets.token_hex(2)}"
    now_ms = str(int(datetime.now(timezone.utc).timestamp() * 1000))
    with db_connect() as conn:
        conn.execute(
            f"""
            insert into {TABLE_NAME}
            (product_id, title, price_qar, make, class_name, model, manufacture_year, km, car_type, gear_type, fuel_type,
             warranty_status, cylinder_count, seller_name, seller_phone, seller_type, seller_whatsapp, seller_user_id, is_company,
             city, url, main_image_url, image_urls_json, all_image_urls_json, properties_json, comments_count, comments_json, description,
             advertise_time_formatted, date_of_advertise, is_approved)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pid,
                str(payload.get("title", "")).strip(),
                parse_int(payload.get("price_qar"), 0) or None,
                str(payload.get("make", "")).strip(),
                str(payload.get("class_name", "")).strip(),
                str(payload.get("model", "")).strip(),
                parse_int(payload.get("manufacture_year"), 0) or None,
                parse_int(payload.get("km"), 0) or None,
                str(payload.get("car_type", "")).strip() or None,
                str(payload.get("gear_type", "")).strip() or None,
                str(payload.get("fuel_type", "")).strip() or None,
                str(payload.get("warranty_status", "")).strip() or None,
                parse_int(payload.get("cylinder_count"), 0) or None,
                str(payload.get("seller_name", "")).strip() or None,
                str(payload.get("seller_phone", "")).strip() or None,
                str(payload.get("seller_type", "")).strip() or None,
                str(payload.get("seller_whatsapp", "")).strip() or None,
                str(payload.get("seller_user_id", "")).strip() or None,
                str(payload.get("is_company", "")).strip() or None,
                str(payload.get("city", "")).strip() or None,
                str(payload.get("url", "")).strip() or None,
                str(payload.get("main_image_url", "")).strip() or None,
                payload.get("image_urls_json") if isinstance(payload.get("image_urls_json"), str) else "[]",
                payload.get("all_image_urls_json") if isinstance(payload.get("all_image_urls_json"), str) else "[]",
                payload.get("properties_json") if isinstance(payload.get("properties_json"), str) else "[]",
                parse_int(payload.get("comments_count"), 0) or 0,
                payload.get("comments_json") if isinstance(payload.get("comments_json"), str) else "[]",
                str(payload.get("description", "")).strip() or None,
                str(payload.get("advertise_time_formatted", "manual")).strip(),
                str(payload.get("date_of_advertise", now_ms)).strip(),
                0,
            ),
        )
        row = conn.execute(
            f"select * from {TABLE_NAME} where product_id = ?",
            (pid,),
        ).fetchone()
        conn.commit()
    global DEAL_CACHE
    DEAL_CACHE = None
    recompute_deals_in_db()
    return listing_from_row(row) or {}, HTTPStatus.CREATED


def get_listing(product_id: str, include_unapproved: bool = False) -> dict:
    with db_connect() as conn:
        if include_unapproved:
            row = conn.execute(
                f"""
                select l.*,
                       (select count(*) from listing_views lv where lv.product_id = l.product_id) as view_count
                from {TABLE_NAME} l
                where l.product_id = ?
                """,
                (product_id,),
            ).fetchone()
        else:
            row = conn.execute(
                f"""
                select l.*,
                       (select count(*) from listing_views lv where lv.product_id = l.product_id) as view_count
                from {TABLE_NAME} l
                where l.product_id = ? and coalesce(l.is_approved, 0) = 1
                """,
                (product_id,),
            ).fetchone()
    if not row:
        raise ValueError("listing not found")
    return listing_from_row(row) or {}


def set_listing_approval(handler: SimpleHTTPRequestHandler, product_id: str, payload: dict) -> dict:
    require_admin(handler)
    approved = 1 if str(payload.get("is_approved", "0")).lower() in {"1", "true", "yes"} else 0
    with db_connect() as conn:
        cur = conn.execute(
            f"update {TABLE_NAME} set is_approved = ? where product_id = ?",
            (approved, product_id),
        )
        if cur.rowcount <= 0:
            raise ValueError("listing not found")
        row = conn.execute(f"select * from {TABLE_NAME} where product_id = ?", (product_id,)).fetchone()
        conn.commit()
    return {"ok": True, "listing": listing_from_row(row)}


def update_listing(handler: SimpleHTTPRequestHandler, product_id: str, payload: dict, replace: bool = False) -> dict:
    require_auth(handler, allow_guest=False)
    with db_connect() as conn:
        current = conn.execute(f"select * from {TABLE_NAME} where product_id = ?", (product_id,)).fetchone()
        if not current:
            raise ValueError("listing not found")
        row = dict(current)
        fields = [
            "title", "price_qar", "make", "class_name", "model", "manufacture_year", "km", "car_type",
            "gear_type", "fuel_type", "warranty_status", "cylinder_count", "seller_name", "seller_phone", "seller_type",
            "seller_whatsapp", "seller_user_id", "is_company", "city", "url", "main_image_url",
            "image_urls_json", "all_image_urls_json", "properties_json", "comments_count", "comments_json", "description",
            "advertise_time_formatted", "date_of_advertise",
        ]
        if replace:
            for f in fields:
                row[f] = payload.get(f)
        else:
            for f in fields:
                if f in payload:
                    row[f] = payload.get(f)
        conn.execute(
            f"""
            update {TABLE_NAME}
            set title=?, price_qar=?, make=?, class_name=?, model=?, manufacture_year=?, km=?, car_type=?, gear_type=?, fuel_type=?,
                warranty_status=?, cylinder_count=?, seller_name=?, seller_phone=?, seller_type=?, seller_whatsapp=?, seller_user_id=?, is_company=?,
                city=?, url=?, main_image_url=?, image_urls_json=?, all_image_urls_json=?, properties_json=?, comments_count=?, comments_json=?, description=?,
                advertise_time_formatted=?, date_of_advertise=?
            where product_id=?
            """,
            (
                str(row.get("title") or "").strip(),
                parse_int(row.get("price_qar"), 0) or None,
                str(row.get("make") or "").strip(),
                str(row.get("class_name") or "").strip(),
                str(row.get("model") or "").strip(),
                parse_int(row.get("manufacture_year"), 0) or None,
                parse_int(row.get("km"), 0) or None,
                str(row.get("car_type") or "").strip() or None,
                str(row.get("gear_type") or "").strip() or None,
                str(row.get("fuel_type") or "").strip() or None,
                str(row.get("warranty_status") or "").strip() or None,
                parse_int(row.get("cylinder_count"), 0) or None,
                str(row.get("seller_name") or "").strip() or None,
                str(row.get("seller_phone") or "").strip() or None,
                str(row.get("seller_type") or "").strip() or None,
                str(row.get("seller_whatsapp") or "").strip() or None,
                str(row.get("seller_user_id") or "").strip() or None,
                str(row.get("is_company") or "").strip() or None,
                str(row.get("city") or "").strip() or None,
                str(row.get("url") or "").strip() or None,
                str(row.get("main_image_url") or "").strip() or None,
                row.get("image_urls_json") if isinstance(row.get("image_urls_json"), str) else "[]",
                row.get("all_image_urls_json") if isinstance(row.get("all_image_urls_json"), str) else "[]",
                row.get("properties_json") if isinstance(row.get("properties_json"), str) else "[]",
                parse_int(row.get("comments_count"), 0) or 0,
                row.get("comments_json") if isinstance(row.get("comments_json"), str) else "[]",
                str(row.get("description") or "").strip() or None,
                str(row.get("advertise_time_formatted") or "").strip(),
                str(row.get("date_of_advertise") or "").strip(),
                product_id,
            ),
        )
        updated = conn.execute(f"select * from {TABLE_NAME} where product_id = ?", (product_id,)).fetchone()
        conn.commit()
    global DEAL_CACHE
    DEAL_CACHE = None
    recompute_deals_in_db()
    return listing_from_row(updated) or {}


def delete_listing(handler: SimpleHTTPRequestHandler, product_id: str) -> None:
    require_auth(handler, allow_guest=False)
    with db_connect() as conn:
        conn.execute(f"delete from {TABLE_NAME} where product_id = ?", (product_id,))
        conn.commit()
    global DEAL_CACHE
    DEAL_CACHE = None
    recompute_deals_in_db()


def fetch_alerts(user_key: str) -> dict:
    if not user_key:
        return {"rows": []}
    with db_connect() as conn:
        rows = conn.execute(
            """
            select id, user_key, make, class_name, model, city, search_text,
                   min_year, max_year, min_price_qar, max_price_qar, min_km, max_km,
                   deals_only, min_discount_pct, max_discount_pct, min_peer_count, max_peer_count,
                   active, created_at, updated_at
            from user_alerts
            where user_key = ?
            order by id desc
            """,
            (user_key,),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


def create_alert(payload: dict) -> dict:
    user_key = str(payload.get("user_key", "")).strip()
    if not user_key:
        raise ValueError("user_key is required")
    now = utc_now_iso()
    with db_connect() as conn:
        cur = conn.execute(
            """
            insert into user_alerts
            (user_key, make, class_name, model, city, search_text, min_year, max_year,
             min_price_qar, max_price_qar, min_km, max_km, deals_only, min_discount_pct,
             max_discount_pct, min_peer_count, max_peer_count, active, created_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_key,
                str(payload.get("make", "")).strip() or None,
                str(payload.get("class_name", "")).strip() or None,
                str(payload.get("model", "")).strip() or None,
                str(payload.get("city", "")).strip() or None,
                str(payload.get("search_text", "")).strip() or None,
                parse_int(payload.get("min_year"), 0) or None,
                parse_int(payload.get("max_year"), 0) or None,
                parse_int(payload.get("min_price_qar"), 0) or None,
                parse_int(payload.get("max_price_qar"), 0) or None,
                parse_int(payload.get("min_km"), 0) or None,
                parse_int(payload.get("max_km"), 0) or None,
                1 if str(payload.get("deals_only", "0")) in {"1", "true", "True"} else 0,
                parse_float(payload.get("min_discount_pct")),
                parse_float(payload.get("max_discount_pct")),
                parse_int(payload.get("min_peer_count"), 0) or None,
                parse_int(payload.get("max_peer_count"), 0) or None,
                1,
                now,
                now,
            ),
        )
        conn.commit()
        alert_id = cur.lastrowid
    return {"ok": True, "alert_id": alert_id}


def set_alert_active(payload: dict) -> dict:
    alert_id = parse_int(payload.get("alert_id"), 0)
    active = 1 if str(payload.get("active", "1")) in {"1", "true", "True"} else 0
    if alert_id <= 0:
        raise ValueError("alert_id is required")
    with db_connect() as conn:
        conn.execute(
            "update user_alerts set active = ?, updated_at = ? where id = ?",
            (active, utc_now_iso(), alert_id),
        )
        conn.commit()
    return {"ok": True}


def fetch_notifications(params: dict[str, list[str]]) -> dict:
    user_key = params.get("user_key", [""])[0].strip()
    unread_only = params.get("unread_only", ["0"])[0].strip() == "1"
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    if not user_key:
        return {"rows": []}
    where = "where user_key = ?"
    values: list = [user_key]
    if unread_only:
        where += " and is_read = 0"
    with db_connect() as conn:
        rows = conn.execute(
            f"""
            select id, user_key, alert_id, product_id, title, price_qar, city, url,
                   main_image_url, listing_date, message, is_read, created_at
            from user_notifications
            {where}
            order by datetime(created_at) desc, id desc
            limit ?
            """,
            (*values, limit),
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


def mark_notifications_read(payload: dict) -> dict:
    ids = payload.get("ids") or []
    user_key = str(payload.get("user_key", "")).strip()
    with db_connect() as conn:
        if ids and isinstance(ids, list):
            id_values = [parse_int(v, 0) for v in ids]
            id_values = [v for v in id_values if v > 0]
            if id_values:
                placeholders = ",".join(["?"] * len(id_values))
                conn.execute(
                    f"update user_notifications set is_read = 1 where id in ({placeholders})",
                    tuple(id_values),
                )
        elif user_key:
            conn.execute("update user_notifications set is_read = 1 where user_key = ?", (user_key,))
        else:
            raise ValueError("Provide ids or user_key")
        conn.commit()
    return {"ok": True}


def actor_from_request(handler: SimpleHTTPRequestHandler, payload_or_params: dict | None = None) -> tuple[str | None, str | None]:
    user = auth_user_from_headers(handler)
    user_id = user["id"] if user else None
    user_key = None
    src = payload_or_params or {}
    if isinstance(src, dict):
        if "user_key" in src and src.get("user_key"):
            if isinstance(src.get("user_key"), list):
                user_key = str(src.get("user_key")[0]).strip()
            else:
                user_key = str(src.get("user_key")).strip()
    return user_id, user_key


def create_upload(handler: SimpleHTTPRequestHandler, payload: dict) -> tuple[dict, int]:
    user = require_auth(handler, allow_guest=True)
    filename = str(payload.get("filename", "")).strip()
    content_b64 = str(payload.get("content_base64", "")).strip()
    mime_type = str(payload.get("mime_type", "")).strip() or "application/octet-stream"
    if not filename or not content_b64:
        raise ValueError("filename and content_base64 are required")
    try:
        data = base64.b64decode(content_b64, validate=True)
    except Exception as exc:  # noqa: BLE001
        raise ValueError("invalid base64 content") from exc

    safe = "".join(ch for ch in filename if ch.isalnum() or ch in {".", "_", "-"}).strip(".")
    if not safe:
        safe = "upload.bin"
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    stored = f"{int(datetime.now(timezone.utc).timestamp())}_{secrets.token_hex(4)}_{safe}"
    path = UPLOADS_DIR / stored
    path.write_bytes(data)

    public_url = f"/uploads/{stored}"
    with db_connect() as conn:
        cur = conn.execute(
            """
            insert into uploads(user_id, filename, mime_type, local_path, public_url, size_bytes, created_at)
            values (?, ?, ?, ?, ?, ?, ?)
            """,
            (user["id"], filename, mime_type, str(path), public_url, len(data), utc_now_iso()),
        )
        conn.commit()
        upload_id = cur.lastrowid

    return {
        "id": upload_id,
        "filename": filename,
        "mime_type": mime_type,
        "size_bytes": len(data),
        "url": public_url,
    }, HTTPStatus.CREATED


def create_favorite(handler: SimpleHTTPRequestHandler, payload: dict) -> tuple[dict, int]:
    product_id = str(payload.get("product_id", "")).strip()
    if not product_id:
        raise ValueError("product_id is required")
    user_id, user_key = actor_from_request(handler, payload)
    if not user_id and not user_key:
        raise PermissionError("Unauthorized")
    now = utc_now_iso()
    with db_connect() as conn:
        listing = conn.execute(f"select product_id from {TABLE_NAME} where product_id = ?", (product_id,)).fetchone()
        if not listing:
            raise ValueError("listing not found")
        cur = conn.execute(
            """
            insert or ignore into favorites(user_id, user_key, product_id, created_at)
            values (?, ?, ?, ?)
            """,
            (user_id, user_key, product_id, now),
        )
        if cur.rowcount == 0:
            row = conn.execute(
                "select id, user_id, user_key, product_id, created_at from favorites where coalesce(user_id,'') = coalesce(?, '') and coalesce(user_key,'') = coalesce(?, '') and product_id = ?",
                (user_id, user_key, product_id),
            ).fetchone()
        else:
            row = conn.execute(
                "select id, user_id, user_key, product_id, created_at from favorites where id = ?",
                (cur.lastrowid,),
            ).fetchone()
        conn.commit()
    return {"row": dict(row) if row else None}, HTTPStatus.CREATED


def get_favorites(handler: SimpleHTTPRequestHandler, params: dict[str, list[str]]) -> dict:
    user_id, user_key = actor_from_request(handler, params)
    if not user_id and not user_key:
        raise PermissionError("Unauthorized")
    where = []
    vals: list = []
    if user_id:
        where.append("user_id = ?")
        vals.append(user_id)
    if user_key:
        where.append("user_key = ?")
        vals.append(user_key)
    where_sql = " or ".join(where)
    with db_connect() as conn:
        rows = conn.execute(
            f"""
            select f.id, f.user_id, f.user_key, f.product_id, f.created_at,
                   l.title, l.price_qar, l.make, l.class_name, l.model, l.main_image_url, l.url
            from favorites f
            left join {TABLE_NAME} l on l.product_id = f.product_id
            where {where_sql}
            order by datetime(f.created_at) desc, f.id desc
            """,
            vals,
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


def delete_favorite(handler: SimpleHTTPRequestHandler, favorite_id: int, params: dict[str, list[str]]) -> None:
    user_id, user_key = actor_from_request(handler, params)
    if not user_id and not user_key:
        raise PermissionError("Unauthorized")
    with db_connect() as conn:
        row = conn.execute("select id, user_id, user_key from favorites where id = ?", (favorite_id,)).fetchone()
        if not row:
            raise ValueError("favorite not found")
        owned = False
        if user_id and row["user_id"] == user_id:
            owned = True
        if user_key and row["user_key"] == user_key:
            owned = True
        if not owned:
            raise PermissionError("Not owner of favorite")
        conn.execute("delete from favorites where id = ?", (favorite_id,))
        conn.commit()


def fetch_channels(user_key: str) -> dict:
    if not user_key:
        return {"whatsapp_configured": whatsapp_is_configured(), "row": None}
    with db_connect() as conn:
        row = conn.execute(
            "select user_key, whatsapp_number, whatsapp_enabled, updated_at from user_channels where user_key = ?",
            (user_key,),
        ).fetchone()
    return {
        "whatsapp_configured": whatsapp_is_configured(),
        "row": dict(row) if row else None,
    }


def upsert_channels(payload: dict) -> dict:
    user_key = str(payload.get("user_key", "")).strip()
    if not user_key:
        raise ValueError("user_key is required")
    whatsapp_number = normalize_whatsapp_number(str(payload.get("whatsapp_number", "")).strip())
    whatsapp_enabled = 1 if str(payload.get("whatsapp_enabled", "0")) in {"1", "true", "True"} else 0
    now = utc_now_iso()
    with db_connect() as conn:
        conn.execute(
            """
            insert into user_channels (user_key, whatsapp_number, whatsapp_enabled, updated_at)
            values (?, ?, ?, ?)
            on conflict(user_key) do update set
              whatsapp_number=excluded.whatsapp_number,
              whatsapp_enabled=excluded.whatsapp_enabled,
              updated_at=excluded.updated_at
            """,
            (user_key, whatsapp_number or None, whatsapp_enabled, now),
        )
        conn.commit()
    return {"ok": True}


def send_test_whatsapp(payload: dict) -> dict:
    user_key = str(payload.get("user_key", "")).strip()
    if not user_key:
        raise ValueError("user_key is required")
    if not whatsapp_is_configured():
        return {"ok": False, "reason": "whatsapp_not_configured"}
    with db_connect() as conn:
        row = conn.execute(
            "select whatsapp_number, whatsapp_enabled from user_channels where user_key = ?",
            (user_key,),
        ).fetchone()
    if not row or not row["whatsapp_number"] or not int(row["whatsapp_enabled"]):
        return {"ok": False, "reason": "channel_not_enabled"}
    ok, msg = send_whatsapp_message(
        str(row["whatsapp_number"]),
        "Mzad test: WhatsApp notifications are active for your saved alerts.",
    )
    return {"ok": ok, "message": msg}


def push_public_key_info() -> dict:
    settings = get_vapid_settings()
    return {
        "configured": bool(settings),
        "public_key": settings["public_key"] if settings else "",
        "webpush_library": webpush_is_available(),
    }


def fetch_push_status(user_key: str) -> dict:
    settings = get_vapid_settings()
    with db_connect() as conn:
        count = 0
        if user_key:
            count = conn.execute(
                "select count(*) as c from web_push_subscriptions where user_key = ? and active = 1",
                (user_key,),
            ).fetchone()["c"]
    return {
        "configured": bool(settings),
        "webpush_library": webpush_is_available(),
        "active_subscriptions": count,
        "public_key": settings["public_key"] if settings else "",
    }


def upsert_push_subscription(payload: dict) -> dict:
    user_key = str(payload.get("user_key", "")).strip()
    subscription = payload.get("subscription")
    if not user_key:
        raise ValueError("user_key is required")
    if not isinstance(subscription, dict):
        raise ValueError("subscription is required")
    endpoint = str(subscription.get("endpoint", "")).strip()
    keys = subscription.get("keys") or {}
    p256dh = str(keys.get("p256dh", "")).strip()
    auth = str(keys.get("auth", "")).strip()
    if not endpoint:
        raise ValueError("subscription.endpoint is required")
    now = utc_now_iso()
    with db_connect() as conn:
        conn.execute(
            """
            insert into web_push_subscriptions
            (user_key, endpoint, p256dh, auth, subscription_json, active, created_at, updated_at)
            values (?, ?, ?, ?, ?, 1, ?, ?)
            on conflict(endpoint) do update set
              user_key=excluded.user_key,
              p256dh=excluded.p256dh,
              auth=excluded.auth,
              subscription_json=excluded.subscription_json,
              active=1,
              updated_at=excluded.updated_at
            """,
            (
                user_key,
                endpoint,
                p256dh or None,
                auth or None,
                json.dumps(subscription, ensure_ascii=False),
                now,
                now,
            ),
        )
        conn.commit()
    return {"ok": True}


def deactivate_push_subscription(payload: dict) -> dict:
    endpoint = str(payload.get("endpoint", "")).strip()
    user_key = str(payload.get("user_key", "")).strip()
    if not endpoint and not user_key:
        raise ValueError("endpoint or user_key is required")
    with db_connect() as conn:
        if endpoint:
            conn.execute(
                "update web_push_subscriptions set active = 0, updated_at = ? where endpoint = ?",
                (utc_now_iso(), endpoint),
            )
        else:
            conn.execute(
                "update web_push_subscriptions set active = 0, updated_at = ? where user_key = ?",
                (utc_now_iso(), user_key),
            )
        conn.commit()
    return {"ok": True}


# ──────────────────────────────────────────────────────────────────────────────
# Instant-Offers DB schema
# ──────────────────────────────────────────────────────────────────────────────

def _migrate_offer_requests_add_pending(conn) -> None:
    """BE-002: Ensure offer_requests.status CHECK constraint includes 'pending'.
    SQLite doesn't support ALTER CONSTRAINT, so we use a table rename + recreate if needed.
    """
    table_sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='offer_requests'"
    ).fetchone()
    if not table_sql:
        return  # table doesn't exist yet, will be created correctly
    if "'pending'" in table_sql[0]:
        return  # already migrated

    # Recreate table with updated CHECK constraint (SQLite-compatible migration)
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("ALTER TABLE offer_requests RENAME TO _offer_requests_old")
    conn.execute(
        """
        CREATE TABLE offer_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_uid TEXT NOT NULL UNIQUE,
            customer_id TEXT NOT NULL,
            make TEXT NOT NULL,
            class_name TEXT NOT NULL,
            model TEXT,
            year INTEGER NOT NULL,
            km INTEGER NOT NULL,
            color TEXT,
            condition TEXT NOT NULL CHECK(condition IN ('excellent','good','fair','poor')),
            city TEXT NOT NULL,
            description TEXT,
            photo_urls_json TEXT,
            asking_price_qar INTEGER,
            contact_name TEXT,
            contact_phone TEXT,
            status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','under_offer','accepted','pending','rejected','expired','cancelled')),
            accepted_bid_id INTEGER,
            expires_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES users(id)
        )
        """
    )
    conn.execute("INSERT INTO offer_requests SELECT * FROM _offer_requests_old")
    conn.execute("DROP TABLE _offer_requests_old")
    conn.execute("PRAGMA foreign_keys=ON")


def ensure_instant_offer_schema() -> None:
    with db_connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS offer_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_uid TEXT NOT NULL UNIQUE,
                customer_id TEXT NOT NULL,
                make TEXT NOT NULL,
                class_name TEXT NOT NULL,
                model TEXT,
                year INTEGER NOT NULL,
                km INTEGER NOT NULL,
                color TEXT,
                condition TEXT NOT NULL CHECK(condition IN ('excellent','good','fair','poor')),
                city TEXT NOT NULL,
                description TEXT,
                photo_urls_json TEXT,
                asking_price_qar INTEGER,
                contact_name TEXT,
                contact_phone TEXT,
                status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','under_offer','accepted','pending','rejected','expired','cancelled')),
                accepted_bid_id INTEGER,
                expires_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(customer_id) REFERENCES users(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_or_customer ON offer_requests(customer_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_or_make_city ON offer_requests(make, city, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_or_status ON offer_requests(status, created_at DESC)")

        # BE-002: migrate offer_requests CHECK constraint to include 'pending' status
        _migrate_offer_requests_add_pending(conn)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS offer_bids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bid_uid TEXT NOT NULL UNIQUE,
                request_id INTEGER NOT NULL,
                dealer_id TEXT NOT NULL,
                amount_qar INTEGER NOT NULL,
                message TEXT,
                expires_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','accepted','rejected','withdrawn','expired')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(request_id) REFERENCES offer_requests(id),
                FOREIGN KEY(dealer_id) REFERENCES users(id),
                UNIQUE(request_id, dealer_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ob_request ON offer_bids(request_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ob_dealer ON offer_bids(dealer_id, created_at DESC)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS offer_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                sender_id TEXT NOT NULL,
                recipient_id TEXT NOT NULL,
                body TEXT NOT NULL,
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(request_id) REFERENCES offer_requests(id),
                FOREIGN KEY(sender_id) REFERENCES users(id),
                FOREIGN KEY(recipient_id) REFERENCES users(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_om_request ON offer_messages(request_id, created_at ASC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_om_recipient_read ON offer_messages(recipient_id, is_read, created_at DESC)")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dealer_offer_preferences (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dealer_id TEXT NOT NULL,
                makes_json TEXT,
                cities_json TEXT,
                min_year INTEGER,
                max_year INTEGER,
                max_km INTEGER,
                notify_push INTEGER NOT NULL DEFAULT 1,
                notify_whatsapp INTEGER NOT NULL DEFAULT 1,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(dealer_id) REFERENCES users(id),
                UNIQUE(dealer_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dop_dealer ON dealer_offer_preferences(dealer_id, active)")

        # BE-001: phone access requests
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS phone_access_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_uid TEXT NOT NULL,
                dealer_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                requested_at TEXT NOT NULL,
                responded_at TEXT,
                UNIQUE(request_uid, dealer_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_par_request ON phone_access_requests(request_uid)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_par_dealer ON phone_access_requests(dealer_id)")

        # BE-006: phone approval audit log
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS phone_approval_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_uid TEXT NOT NULL,
                dealer_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                ip TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_paa_request ON phone_approval_audit(request_uid)")

        # BE-007: document_visibility on offer_requests
        existing_or_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(offer_requests)").fetchall()}
        if "document_visibility" not in existing_or_cols:
            conn.execute(
                "ALTER TABLE offer_requests ADD COLUMN document_visibility TEXT NOT NULL DEFAULT 'all_dealers'"
            )
        # BE-009: contact_whatsapp on offer_requests (re-read cols after possible alter)
        existing_or_cols2 = {str(r[1]) for r in conn.execute("PRAGMA table_info(offer_requests)").fetchall()}
        if "contact_whatsapp" not in existing_or_cols2:
            conn.execute("ALTER TABLE offer_requests ADD COLUMN contact_whatsapp TEXT")

        # BE-008: last_seen on users
        existing_user_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(users)").fetchall()}
        if "last_seen" not in existing_user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")

        # BE-003: dealer subscriptions
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dealer_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dealer_id INTEGER NOT NULL,
                plan TEXT NOT NULL DEFAULT 'basic',
                amount_qar REAL NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                started_at TEXT NOT NULL,
                expires_at TEXT,
                next_billing TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ds_dealer ON dealer_subscriptions(dealer_id)")

        # BE-004: saved dealer filters
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dealer_saved_filters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dealer_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                makes_json TEXT,
                cities_json TEXT,
                min_year INTEGER,
                max_year INTEGER,
                max_km INTEGER,
                condition TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_dsf_dealer ON dealer_saved_filters(dealer_id)")

        conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Instant-Offers helpers
# ──────────────────────────────────────────────────────────────────────────────

def require_dealer(handler) -> dict:
    user = require_auth(handler, allow_guest=False)
    if user["role"] not in ("dealer", "admin"):
        raise PermissionError("Dealer account required")
    return user


def assert_can_message(conn, request_id: int, caller_id: str, customer_id: str) -> None:
    if caller_id == customer_id:
        return
    row = conn.execute(
        "SELECT 1 FROM offer_bids WHERE request_id=? AND dealer_id=? AND status != 'withdrawn'",
        (request_id, caller_id)
    ).fetchone()
    if not row:
        raise PermissionError("No active bid on this request")


def dispatch_platform_notification(user_id: str, title: str, body: str, url: str = "") -> None:
    """Send push + WhatsApp notification to a user by user_id."""
    try:
        with db_connect() as conn:
            channel = conn.execute(
                "SELECT whatsapp_number, whatsapp_enabled FROM user_channels WHERE user_key = ?",
                (user_id,)
            ).fetchone()
            if channel and channel["whatsapp_number"] and int(channel["whatsapp_enabled"] or 0) == 1:
                try:
                    send_whatsapp_message(channel["whatsapp_number"],
                                          f"{title}\n{body}\n{url}".strip())
                except Exception:
                    pass
            subs = conn.execute(
                "SELECT id, subscription_json FROM web_push_subscriptions "
                "WHERE user_key = ? AND active = 1", (user_id,)
            ).fetchall()
            for sub in subs:
                try:
                    send_web_push(json.loads(sub["subscription_json"]),
                                  {"title": title, "body": body, "url": url})
                except Exception:
                    pass
    except Exception:
        pass


def notify_dealers_of_new_request(request: dict) -> None:
    """Fan-out new offer request notification to matching dealers. Runs in background thread."""
    def _run():
        try:
            with db_connect() as conn:
                prefs = conn.execute(
                    "SELECT dealer_id, makes_json, cities_json, min_year, max_year, max_km "
                    "FROM dealer_offer_preferences WHERE active = 1"
                ).fetchall()
            for p in prefs:
                makes  = json.loads(p["makes_json"]  or "[]")
                cities = json.loads(p["cities_json"] or "[]")
                if makes  and request["make"] not in makes:   continue
                if cities and request["city"] not in cities:  continue
                if p["min_year"] and request["year"] < p["min_year"]: continue
                if p["max_year"] and request["year"] > p["max_year"]: continue
                if p["max_km"]   and request["km"]   > p["max_km"]:   continue
                dispatch_platform_notification(
                    p["dealer_id"],
                    title=f"New instant offer: {request['year']} {request['make']} {request['class_name']}",
                    body=f"{request['km']:,} km · {request['city']}",
                    url=f"/instant-offers/{request['request_uid']}"
                )
        except Exception:
            pass
    threading.Thread(target=_run, daemon=True).start()


# ──────────────────────────────────────────────────────────────────────────────
# Instant-Offers business logic
# ──────────────────────────────────────────────────────────────────────────────

def fetch_offer_comps(make: str, class_name: str, model: str, year: int, km: int) -> dict:
    """Return market comparables for an offer request."""
    with db_connect() as conn:
        rows = conn.execute(
            f"""
            SELECT product_id, title, price_qar, km, manufacture_year, city, main_image_url
            FROM {TABLE_NAME}
            WHERE make = ?
              AND class_name = ?
              AND manufacture_year BETWEEN ? AND ?
              AND km BETWEEN ? AND ?
              AND price_qar BETWEEN 1000 AND 3000000
              AND coalesce(is_approved, 0) = 1
            ORDER BY ABS(km - ?) ASC
            LIMIT 100
            """,
            (make, class_name, year - 2, year + 2, max(0, km - 30000), km + 30000, km),
        ).fetchall()

    if not rows:
        return {"count": 0, "median": None, "p25": None, "p75": None, "min": None, "max": None, "avg": None, "samples": []}

    prices = sorted([float(r["price_qar"]) for r in rows])
    count = len(prices)

    def percentile(arr, q):
        idx = int((len(arr) - 1) * q)
        return arr[max(0, min(idx, len(arr) - 1))]

    samples = []
    for r in rows[:5]:
        samples.append({
            "product_id": r["product_id"],
            "title": r["title"],
            "price_qar": r["price_qar"],
            "km": r["km"],
            "manufacture_year": r["manufacture_year"],
            "city": r["city"],
            "main_image_url": r["main_image_url"],
        })

    return {
        "count": count,
        "median": round(percentile(prices, 0.5), 0),
        "p25": round(percentile(prices, 0.25), 0),
        "p75": round(percentile(prices, 0.75), 0),
        "min": round(prices[0], 0),
        "max": round(prices[-1], 0),
        "avg": round(sum(prices) / count, 0),
        "samples": samples,
    }


def create_offer_request(handler, user, payload) -> tuple[dict, int]:
    """Create a new instant offer request."""
    make = str(payload.get("make", "")).strip()
    class_name = str(payload.get("class_name", "")).strip()
    year = parse_int(payload.get("year"), 0)
    km = parse_int(payload.get("km"), 0)
    condition = str(payload.get("condition", "")).strip()
    city = str(payload.get("city", "")).strip()

    if not make:
        raise ValueError("make is required")
    if not class_name:
        raise ValueError("class_name is required")
    if year <= 0:
        raise ValueError("year is required")
    if km < 0:
        raise ValueError("km is required")
    if condition not in ("excellent", "good", "fair", "poor"):
        raise ValueError("condition must be one of: excellent, good, fair, poor")
    if not city:
        raise ValueError("city is required")

    request_uid = "or_" + secrets.token_hex(12)
    now = utc_now_iso()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO offer_requests
            (request_uid, customer_id, make, class_name, model, year, km, color, condition, city,
             description, photo_urls_json, asking_price_qar, contact_name, contact_phone,
             status, expires_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?)
            """,
            (
                request_uid,
                user["id"],
                make,
                class_name,
                str(payload.get("model", "")).strip() or None,
                year,
                km,
                str(payload.get("color", "")).strip() or None,
                condition,
                city,
                str(payload.get("description", "")).strip() or None,
                payload.get("photo_urls_json") if isinstance(payload.get("photo_urls_json"), str) else None,
                parse_int(payload.get("asking_price_qar"), 0) or None,
                str(payload.get("contact_name", "")).strip() or None,
                str(payload.get("contact_phone", "")).strip() or None,
                str(payload.get("expires_at", "")).strip() or None,
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        conn.commit()

    request_dict = dict(row)
    notify_dealers_of_new_request(request_dict)
    return ({"request": request_dict}, HTTPStatus.CREATED)


def list_offer_requests_for_dealer(handler, user, params) -> dict:
    """Dealer view of open offer requests filtered by their preferences."""
    dealer_id = user["id"]
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    offset = max(0, parse_int(params.get("offset", ["0"])[0], 0))

    with db_connect() as conn:
        pref = conn.execute(
            "SELECT * FROM dealer_offer_preferences WHERE dealer_id = ? AND active = 1",
            (dealer_id,)
        ).fetchone()

        where_parts = [
            "r.id NOT IN (SELECT request_id FROM offer_bids WHERE dealer_id = ?)"
        ]
        vals: list = [dealer_id]

        # Apply preference defaults then allow param overrides
        make_filter = params.get("make", [""])[0].strip()
        city_filter = params.get("city", [""])[0].strip()
        min_year = parse_int(params.get("min_year", ["0"])[0], 0) or (pref["min_year"] if pref and pref["min_year"] else 0)
        max_year = parse_int(params.get("max_year", ["0"])[0], 0) or (pref["max_year"] if pref and pref["max_year"] else 0)
        max_km = parse_int(params.get("max_km", ["0"])[0], 0) or (pref["max_km"] if pref and pref["max_km"] else 0)
        status_filter = params.get("status", ["open"])[0].strip() or "open"

        if not make_filter and pref and pref["makes_json"]:
            makes_list = json.loads(pref["makes_json"] or "[]")
            if makes_list:
                placeholders = ",".join(["?"] * len(makes_list))
                where_parts.append(f"r.make IN ({placeholders})")
                vals.extend(makes_list)
        elif make_filter:
            where_parts.append("r.make = ?")
            vals.append(make_filter)

        if not city_filter and pref and pref["cities_json"]:
            cities_list = json.loads(pref["cities_json"] or "[]")
            if cities_list:
                placeholders = ",".join(["?"] * len(cities_list))
                where_parts.append(f"r.city IN ({placeholders})")
                vals.extend(cities_list)
        elif city_filter:
            where_parts.append("r.city = ?")
            vals.append(city_filter)

        if min_year > 0:
            where_parts.append("r.year >= ?")
            vals.append(min_year)
        if max_year > 0:
            where_parts.append("r.year <= ?")
            vals.append(max_year)
        if max_km > 0:
            where_parts.append("r.km <= ?")
            vals.append(max_km)

        where_parts.append("r.status = ?")
        vals.append(status_filter)

        where_sql = " AND ".join(where_parts)
        total = conn.execute(
            f"SELECT COUNT(*) as c FROM offer_requests r WHERE {where_sql}", vals
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT r.* FROM offer_requests r WHERE {where_sql} ORDER BY r.created_at DESC LIMIT ? OFFSET ?",
            (*vals, limit, offset),
        ).fetchall()

    return {"rows": [dict(r) for r in rows], "total": total}


def list_my_offer_requests(handler, user, params) -> dict:
    """Customer view of their own offer requests."""
    customer_id = user["id"]
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    offset = max(0, parse_int(params.get("offset", ["0"])[0], 0))
    status_filter = params.get("status", [""])[0].strip()

    where_parts = ["r.customer_id = ?"]
    vals: list = [customer_id]
    if status_filter:
        where_parts.append("r.status = ?")
        vals.append(status_filter)
    where_sql = " AND ".join(where_parts)

    with db_connect() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) as c FROM offer_requests r WHERE {where_sql}", vals
        ).fetchone()["c"]
        rows = conn.execute(
            f"""
            SELECT r.*,
                   (SELECT COUNT(*) FROM offer_bids b WHERE b.request_id = r.id AND b.status != 'withdrawn') as bid_count,
                   (SELECT COUNT(*) FROM offer_messages m WHERE m.request_id = r.id AND m.recipient_id = ? AND m.is_read = 0) as unread_messages
            FROM offer_requests r
            WHERE {where_sql}
            ORDER BY r.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (customer_id, *vals, limit, offset),
        ).fetchall()

    return {"rows": [dict(r) for r in rows], "total": total}


def get_offer_request_detail(handler, user, request_uid) -> dict:
    """Get full detail of an offer request."""
    caller_id = user["id"]
    caller_role = user.get("role", "user")

    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")

        req_dict = dict(req)
        customer_id = req_dict["customer_id"]

        # Access control
        is_customer = (caller_id == customer_id)
        is_admin = (caller_role == "admin")
        is_dealer_with_bid = False
        if not is_customer and not is_admin:
            bid_row = conn.execute(
                "SELECT 1 FROM offer_bids WHERE request_id = ? AND dealer_id = ? AND status != 'withdrawn'",
                (req_dict["id"], caller_id)
            ).fetchone()
            if bid_row:
                is_dealer_with_bid = True
            else:
                raise PermissionError("Access denied")

        # BE-005: expire stale bids before reading
        expire_stale_bids(conn)

        # Bids
        if is_customer or is_admin:
            bids = conn.execute(
                "SELECT * FROM offer_bids WHERE request_id = ? ORDER BY created_at ASC",
                (req_dict["id"],)
            ).fetchall()
        else:
            bids = conn.execute(
                "SELECT * FROM offer_bids WHERE request_id = ? AND dealer_id = ?",
                (req_dict["id"], caller_id)
            ).fetchall()

    # Hide contact phone unless viewer is customer or has accepted bid
    can_see_phone = is_customer or is_admin
    if not can_see_phone and is_dealer_with_bid:
        accepted_bid = next((b for b in bids if b["status"] == "accepted" and b["dealer_id"] == caller_id), None)
        if accepted_bid:
            can_see_phone = True
    if not can_see_phone:
        req_dict["contact_phone"] = None

    # BE-009: gate whatsapp_number to phone-approved dealers only
    with db_connect() as conn:
        dealer_phone_approved = False
        if not is_customer and not is_admin:
            par = conn.execute(
                "SELECT status FROM phone_access_requests WHERE request_uid=? AND dealer_id=?",
                (request_uid, caller_id),
            ).fetchone()
            dealer_phone_approved = bool(par and par["status"] == "approved")

        # BE-007: filter photos based on document_visibility
        visibility = req_dict.get("document_visibility", "all_dealers") or "all_dealers"
        if not (is_customer or is_admin):
            if visibility == "none":
                req_dict["photo_urls_json"] = None
            elif visibility == "approved_only" and not dealer_phone_approved:
                req_dict["photo_urls_json"] = None

    if not (is_customer or is_admin or dealer_phone_approved):
        req_dict["contact_whatsapp"] = None

    comps = fetch_offer_comps(
        req_dict["make"], req_dict["class_name"],
        req_dict.get("model") or "", req_dict["year"], req_dict["km"]
    )

    return {
        "request": req_dict,
        "bids": [format_bid(b) for b in bids],
        "market_comps": comps,
    }


def create_offer_bid(handler, user, request_uid, payload) -> tuple[dict, int]:
    """Dealer places a bid on an offer request."""
    dealer_id = user["id"]
    amount_qar = parse_int(payload.get("amount_qar"), 0)
    if amount_qar <= 0:
        raise ValueError("amount_qar is required and must be positive")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["status"] in ("accepted", "pending", "expired", "cancelled"):
            raise ValueError(f"Bidding is closed for this request (status: {req['status']})")

        existing = conn.execute(
            "SELECT 1 FROM offer_bids WHERE request_id = ? AND dealer_id = ? AND status != 'withdrawn'",
            (req["id"], dealer_id)
        ).fetchone()
        if existing:
            raise ValueError("You already have an active bid on this request")

        bid_uid = "ob_" + secrets.token_hex(12)
        conn.execute(
            """
            INSERT INTO offer_bids
            (bid_uid, request_id, dealer_id, amount_qar, message, expires_at, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                bid_uid,
                req["id"],
                dealer_id,
                amount_qar,
                str(payload.get("message", "")).strip() or None,
                str(payload.get("expires_at", "")).strip() or None,
                now,
                now,
            ),
        )

        if req["status"] == "open":
            conn.execute(
                "UPDATE offer_requests SET status = 'under_offer', updated_at = ? WHERE id = ?",
                (now, req["id"])
            )

        bid_row = conn.execute("SELECT * FROM offer_bids WHERE bid_uid = ?", (bid_uid,)).fetchone()
        customer_id = req["customer_id"]
        conn.commit()

    bid_dict = dict(bid_row)

    def _notify():
        dispatch_platform_notification(
            customer_id,
            title="New bid on your offer request",
            body=f"QAR {amount_qar:,} from a dealer",
            url=f"/instant-offers/{request_uid}"
        )
    threading.Thread(target=_notify, daemon=True).start()

    return ({"bid": bid_dict}, HTTPStatus.CREATED)


def patch_offer_bid(handler, user, bid_uid, payload) -> dict:
    """Dealer updates their pending bid."""
    dealer_id = user["id"]
    now = utc_now_iso()

    with db_connect() as conn:
        bid = conn.execute(
            "SELECT b.*, r.customer_id, r.request_uid FROM offer_bids b JOIN offer_requests r ON r.id = b.request_id WHERE b.bid_uid = ?",
            (bid_uid,)
        ).fetchone()
        if not bid:
            raise ValueError("bid not found")
        if bid["dealer_id"] != dealer_id:
            raise PermissionError("Not your bid")
        if bid["status"] != "pending":
            raise ValueError(f"Cannot edit bid with status '{bid['status']}'")

        updates = ["updated_at = ?"]
        vals: list = [now]
        if "amount_qar" in payload:
            updates.insert(0, "amount_qar = ?")
            vals.insert(0, parse_int(payload.get("amount_qar"), 0))
        if "message" in payload:
            updates.insert(0, "message = ?")
            vals.insert(0, str(payload.get("message", "")).strip() or None)
        if "expires_at" in payload:
            updates.insert(0, "expires_at = ?")
            vals.insert(0, str(payload.get("expires_at", "")).strip() or None)

        vals.append(bid_uid)
        conn.execute(f"UPDATE offer_bids SET {', '.join(updates)} WHERE bid_uid = ?", vals)
        bid_row = conn.execute("SELECT * FROM offer_bids WHERE bid_uid = ?", (bid_uid,)).fetchone()
        customer_id = bid["customer_id"]
        request_uid_val = bid["request_uid"]
        amount_qar = bid_row["amount_qar"]
        conn.commit()

    def _notify():
        dispatch_platform_notification(
            customer_id,
            title="Bid revised on your offer request",
            body=f"New amount: QAR {amount_qar:,}",
            url=f"/instant-offers/{request_uid_val}"
        )
    threading.Thread(target=_notify, daemon=True).start()

    return {"bid": dict(bid_row)}


def withdraw_offer_bid(handler, user, bid_uid) -> dict:
    """Dealer withdraws their pending bid."""
    dealer_id = user["id"]
    now = utc_now_iso()

    with db_connect() as conn:
        bid = conn.execute(
            "SELECT b.*, r.customer_id, r.request_uid, r.id as req_id FROM offer_bids b JOIN offer_requests r ON r.id = b.request_id WHERE b.bid_uid = ?",
            (bid_uid,)
        ).fetchone()
        if not bid:
            raise ValueError("bid not found")
        if bid["dealer_id"] != dealer_id:
            raise PermissionError("Not your bid")
        if bid["status"] != "pending":
            raise ValueError(f"Cannot withdraw bid with status '{bid['status']}'")

        conn.execute(
            "UPDATE offer_bids SET status = 'withdrawn', updated_at = ? WHERE bid_uid = ?",
            (now, bid_uid)
        )

        pending_count = conn.execute(
            "SELECT COUNT(*) as c FROM offer_bids WHERE request_id = ? AND status = 'pending'",
            (bid["req_id"],)
        ).fetchone()["c"]
        if pending_count == 0:
            conn.execute(
                "UPDATE offer_requests SET status = 'open', updated_at = ? WHERE id = ?",
                (now, bid["req_id"])
            )

        customer_id = bid["customer_id"]
        request_uid_val = bid["request_uid"]
        conn.commit()

    def _notify():
        dispatch_platform_notification(
            customer_id,
            title="A dealer withdrew their bid",
            body="One bid has been withdrawn from your offer request",
            url=f"/instant-offers/{request_uid_val}"
        )
    threading.Thread(target=_notify, daemon=True).start()

    return {"ok": True}


def accept_offer_bid(handler, user, request_uid, payload) -> dict:
    """Customer accepts a bid — atomic transaction."""
    customer_id = user["id"]
    bid_uid = str(payload.get("bid_uid", "")).strip()
    if not bid_uid:
        raise ValueError("bid_uid is required")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != customer_id:
            raise PermissionError("Not your offer request")

        winning_bid = conn.execute(
            "SELECT * FROM offer_bids WHERE bid_uid = ? AND request_id = ?",
            (bid_uid, req["id"])
        ).fetchone()
        if not winning_bid:
            raise ValueError("bid not found on this request")
        # BE-005: block accepting expired bids
        if winning_bid["expires_at"] and winning_bid["expires_at"] < now:
            raise ValueError("This bid has expired and cannot be accepted")

        # Atomic: accept winner, reject others, update request
        conn.execute(
            "UPDATE offer_bids SET status = 'accepted', updated_at = ? WHERE bid_uid = ?",
            (now, bid_uid)
        )
        conn.execute(
            "UPDATE offer_bids SET status = 'rejected', updated_at = ? WHERE request_id = ? AND status = 'pending' AND bid_uid != ?",
            (now, req["id"], bid_uid)
        )
        # BE-002: transition to 'pending' (awaiting finalisation) rather than straight to 'accepted'
        conn.execute(
            "UPDATE offer_requests SET status = 'pending', accepted_bid_id = ?, updated_at = ? WHERE id = ?",
            (winning_bid["id"], now, req["id"])
        )

        req_row = conn.execute("SELECT * FROM offer_requests WHERE id = ?", (req["id"],)).fetchone()
        rejected_dealers = conn.execute(
            "SELECT dealer_id FROM offer_bids WHERE request_id = ? AND status = 'rejected'",
            (req["id"],)
        ).fetchall()
        winning_dealer_id = winning_bid["dealer_id"]
        conn.commit()

    def _notify():
        dispatch_platform_notification(
            winning_dealer_id,
            title="Your bid was accepted!",
            body=f"QAR {winning_bid['amount_qar']:,} — congratulations",
            url=f"/instant-offers/{request_uid}"
        )
        for r in rejected_dealers:
            if r["dealer_id"] != winning_dealer_id:
                dispatch_platform_notification(
                    r["dealer_id"],
                    title="Your bid was not selected",
                    body="The customer has accepted another offer",
                    url=f"/instant-offers/{request_uid}"
                )
    threading.Thread(target=_notify, daemon=True).start()

    return {"ok": True, "request": dict(req_row), "bid": dict(winning_bid)}


def reject_offer_bid(handler, user, request_uid, payload) -> dict:
    """Customer rejects a specific bid."""
    customer_id = user["id"]
    bid_uid = str(payload.get("bid_uid", "")).strip()
    if not bid_uid:
        raise ValueError("bid_uid is required")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != customer_id:
            raise PermissionError("Not your offer request")

        bid = conn.execute(
            "SELECT * FROM offer_bids WHERE bid_uid = ? AND request_id = ?",
            (bid_uid, req["id"])
        ).fetchone()
        if not bid:
            raise ValueError("bid not found")

        conn.execute(
            "UPDATE offer_bids SET status = 'rejected', updated_at = ? WHERE bid_uid = ?",
            (now, bid_uid)
        )

        pending_count = conn.execute(
            "SELECT COUNT(*) as c FROM offer_bids WHERE request_id = ? AND status = 'pending'",
            (req["id"],)
        ).fetchone()["c"]
        if pending_count == 0:
            conn.execute(
                "UPDATE offer_requests SET status = 'open', updated_at = ? WHERE id = ?",
                (now, req["id"])
            )

        dealer_id = bid["dealer_id"]
        conn.commit()

    def _notify():
        dispatch_platform_notification(
            dealer_id,
            title="Your bid was rejected",
            body="The customer has declined your offer",
            url=f"/instant-offers/{request_uid}"
        )
    threading.Thread(target=_notify, daemon=True).start()

    return {"ok": True}


def cancel_offer_request(handler, user, request_uid) -> dict:
    """Customer cancels their offer request."""
    customer_id = user["id"]
    terminal = {"accepted", "pending", "rejected", "expired", "cancelled"}
    now = utc_now_iso()

    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != customer_id:
            raise PermissionError("Not your offer request")
        if req["status"] in terminal:
            raise ValueError(f"Cannot cancel request with status '{req['status']}'")

        conn.execute(
            "UPDATE offer_requests SET status = 'cancelled', updated_at = ? WHERE id = ?",
            (now, req["id"])
        )
        conn.execute(
            "UPDATE offer_bids SET status = 'rejected', updated_at = ? WHERE request_id = ? AND status = 'pending'",
            (now, req["id"])
        )

        affected_dealers = conn.execute(
            "SELECT DISTINCT dealer_id FROM offer_bids WHERE request_id = ? AND status = 'rejected'",
            (req["id"],)
        ).fetchall()
        conn.commit()

    def _notify():
        for r in affected_dealers:
            dispatch_platform_notification(
                r["dealer_id"],
                title="Offer request cancelled",
                body="The customer has cancelled their offer request",
                url=f"/instant-offers/{request_uid}"
            )
    threading.Thread(target=_notify, daemon=True).start()

    return {"ok": True}


def decline_offer_request(handler, user, request_uid) -> dict:
    """Dealer declines (passes on) an offer request without bidding."""
    dealer_id = user["id"]
    now = utc_now_iso()

    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")

        bid_uid = "od_" + secrets.token_hex(12)
        conn.execute(
            """
            INSERT OR IGNORE INTO offer_bids
            (bid_uid, request_id, dealer_id, amount_qar, status, created_at, updated_at)
            VALUES (?, ?, ?, 0, 'rejected', ?, ?)
            """,
            (bid_uid, req["id"], dealer_id, now, now)
        )
        conn.commit()

    return {"ok": True}


def send_offer_message(handler, user, request_uid, payload) -> tuple[dict, int]:
    """Send a message on an offer request thread."""
    caller_id = user["id"]
    recipient_id = str(payload.get("recipient_id", "")).strip()
    body = str(payload.get("body", "")).strip()
    if not recipient_id:
        raise ValueError("recipient_id is required")
    if not body:
        raise ValueError("body is required")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")

        assert_can_message(conn, req["id"], caller_id, req["customer_id"])

        conn.execute(
            """
            INSERT INTO offer_messages (request_id, sender_id, recipient_id, body, is_read, created_at)
            VALUES (?, ?, ?, ?, 0, ?)
            """,
            (req["id"], caller_id, recipient_id, body, now)
        )
        msg_row = conn.execute(
            "SELECT * FROM offer_messages WHERE rowid = last_insert_rowid()"
        ).fetchone()
        conn.commit()

    msg_dict = dict(msg_row)

    def _notify():
        dispatch_platform_notification(
            recipient_id,
            title="New message on offer request",
            body=body[:100],
            url=f"/instant-offers/{request_uid}"
        )
    threading.Thread(target=_notify, daemon=True).start()

    return ({"message": msg_dict}, HTTPStatus.CREATED)


def list_offer_messages(handler, user, request_uid, params) -> dict:
    """List messages for an offer request thread."""
    caller_id = user["id"]
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    offset = max(0, parse_int(params.get("offset", ["0"])[0], 0))

    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")

        assert_can_message(conn, req["id"], caller_id, req["customer_id"])

        total = conn.execute(
            "SELECT COUNT(*) as c FROM offer_messages WHERE request_id = ?",
            (req["id"],)
        ).fetchone()["c"]
        rows = conn.execute(
            "SELECT * FROM offer_messages WHERE request_id = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (req["id"], limit, offset)
        ).fetchall()

        # Mark caller's unread messages as read
        conn.execute(
            "UPDATE offer_messages SET is_read = 1 WHERE request_id = ? AND recipient_id = ? AND is_read = 0",
            (req["id"], caller_id)
        )
        conn.commit()

    return {"rows": [dict(r) for r in rows], "total": total}


def list_my_bids(handler, user, params) -> dict:
    """Dealer view of their own bids with embedded request summary."""
    dealer_id = user["id"]
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    offset = max(0, parse_int(params.get("offset", ["0"])[0], 0))
    status_filter = params.get("status", [""])[0].strip()

    where_parts = ["b.dealer_id = ?"]
    vals: list = [dealer_id]
    if status_filter:
        where_parts.append("b.status = ?")
        vals.append(status_filter)
    where_sql = " AND ".join(where_parts)

    with db_connect() as conn:
        # BE-005: expire stale bids
        expire_stale_bids(conn)
        total = conn.execute(
            f"SELECT COUNT(*) as c FROM offer_bids b WHERE {where_sql}", vals
        ).fetchone()["c"]
        rows = conn.execute(
            f"""
            SELECT b.*, r.request_uid, r.make, r.class_name, r.model, r.year, r.km,
                   r.city, r.condition, r.status as request_status, r.customer_id
            FROM offer_bids b
            JOIN offer_requests r ON r.id = b.request_id
            WHERE {where_sql}
            ORDER BY b.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (*vals, limit, offset),
        ).fetchall()

    return {"rows": [format_bid(r) for r in rows], "total": total}


def get_dealer_preferences(handler, user) -> dict:
    """Return dealer notification preferences."""
    dealer_id = user["id"]
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM dealer_offer_preferences WHERE dealer_id = ?",
            (dealer_id,)
        ).fetchone()
    return {"preferences": dict(row) if row else None}


def upsert_dealer_preferences(handler, user, payload) -> dict:
    """Create or update dealer offer notification preferences."""
    dealer_id = user["id"]
    now = utc_now_iso()

    makes = payload.get("makes") or []
    cities = payload.get("cities") or []
    min_year = parse_int(payload.get("min_year"), 0) or None
    max_year = parse_int(payload.get("max_year"), 0) or None
    max_km = parse_int(payload.get("max_km"), 0) or None
    notify_push = 1 if str(payload.get("notify_push", "1")) in {"1", "true", "True"} else 0
    notify_whatsapp = 1 if str(payload.get("notify_whatsapp", "1")) in {"1", "true", "True"} else 0
    active = 1 if str(payload.get("active", "1")) in {"1", "true", "True"} else 0

    with db_connect() as conn:
        existing = conn.execute(
            "SELECT created_at FROM dealer_offer_preferences WHERE dealer_id = ?",
            (dealer_id,)
        ).fetchone()
        created_at = existing["created_at"] if existing else now

        conn.execute(
            """
            INSERT OR REPLACE INTO dealer_offer_preferences
            (dealer_id, makes_json, cities_json, min_year, max_year, max_km,
             notify_push, notify_whatsapp, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dealer_id,
                json.dumps(makes, ensure_ascii=False),
                json.dumps(cities, ensure_ascii=False),
                min_year,
                max_year,
                max_km,
                notify_push,
                notify_whatsapp,
                active,
                created_at,
                now,
            )
        )
        pref_row = conn.execute(
            "SELECT * FROM dealer_offer_preferences WHERE dealer_id = ?",
            (dealer_id,)
        ).fetchone()
        conn.commit()

    return {"ok": True, "preferences": dict(pref_row)}


# ──────────────────────────────────────────────────────────────────────────────
# BE-001: Phone Approval Workflow
# ──────────────────────────────────────────────────────────────────────────────

def create_phone_request(handler, user, request_uid) -> tuple[dict, int]:
    """Dealer requests access to seller's phone number."""
    dealer_id = user["id"]
    now = utc_now_iso()

    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")

        try:
            conn.execute(
                """
                INSERT INTO phone_access_requests (request_uid, dealer_id, status, requested_at)
                VALUES (?, ?, 'pending', ?)
                """,
                (request_uid, dealer_id, now),
            )
            # BE-006: audit log
            conn.execute(
                "INSERT INTO phone_approval_audit (request_uid, dealer_id, action, timestamp, ip) VALUES (?,?,?,?,?)",
                (request_uid, dealer_id, "requested", now, handler.client_address[0]),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise ValueError("You have already requested phone access for this offer")

    return ({"ok": True, "status": "pending"}, HTTPStatus.CREATED)


def approve_phone_request(handler, user, request_uid, payload) -> dict:
    """Customer approves a dealer's phone access request."""
    customer_id = user["id"]
    dealer_id = str(payload.get("dealer_id", "")).strip()
    if not dealer_id:
        raise ValueError("dealer_id is required")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != customer_id:
            raise PermissionError("Not your offer request")

        par = conn.execute(
            "SELECT * FROM phone_access_requests WHERE request_uid = ? AND dealer_id = ?",
            (request_uid, dealer_id)
        ).fetchone()
        if not par:
            raise ValueError("Phone access request not found")

        conn.execute(
            "UPDATE phone_access_requests SET status='approved', responded_at=? WHERE request_uid=? AND dealer_id=?",
            (now, request_uid, dealer_id)
        )
        # BE-006: audit log
        conn.execute(
            "INSERT INTO phone_approval_audit (request_uid, dealer_id, action, timestamp, ip) VALUES (?,?,?,?,?)",
            (request_uid, dealer_id, "approved", now, handler.client_address[0]),
        )
        conn.commit()

    return {"ok": True, "status": "approved"}


def reject_phone_request(handler, user, request_uid, payload) -> dict:
    """Customer rejects a dealer's phone access request."""
    customer_id = user["id"]
    dealer_id = str(payload.get("dealer_id", "")).strip()
    if not dealer_id:
        raise ValueError("dealer_id is required")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != customer_id:
            raise PermissionError("Not your offer request")

        par = conn.execute(
            "SELECT * FROM phone_access_requests WHERE request_uid = ? AND dealer_id = ?",
            (request_uid, dealer_id)
        ).fetchone()
        if not par:
            raise ValueError("Phone access request not found")

        conn.execute(
            "UPDATE phone_access_requests SET status='rejected', responded_at=? WHERE request_uid=? AND dealer_id=?",
            (now, request_uid, dealer_id)
        )
        # BE-006: audit log
        conn.execute(
            "INSERT INTO phone_approval_audit (request_uid, dealer_id, action, timestamp, ip) VALUES (?,?,?,?,?)",
            (request_uid, dealer_id, "rejected", now, handler.client_address[0]),
        )
        conn.commit()

    return {"ok": True, "status": "rejected"}


def list_phone_requests(handler, user, request_uid) -> dict:
    """Customer views all phone access requests for their offer."""
    customer_id = user["id"]

    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != customer_id:
            raise PermissionError("Not your offer request")

        rows = conn.execute(
            "SELECT dealer_id, status, requested_at, responded_at FROM phone_access_requests WHERE request_uid = ? ORDER BY requested_at ASC",
            (request_uid,)
        ).fetchall()

    return {"phone_requests": [dict(r) for r in rows]}


# ──────────────────────────────────────────────────────────────────────────────
# BE-006: Phone Approval Audit Log
# ──────────────────────────────────────────────────────────────────────────────

def get_phone_approval_log(handler, user, request_uid) -> dict:
    """Admin-only: return timestamped audit log for phone approval events."""
    if user.get("role") != "admin":
        raise PermissionError("Admin access required")

    with db_connect() as conn:
        rows = conn.execute(
            "SELECT dealer_id, action, timestamp, ip FROM phone_approval_audit WHERE request_uid = ? ORDER BY timestamp ASC",
            (request_uid,),
        ).fetchall()

    return {"log": [dict(r) for r in rows]}


# ──────────────────────────────────────────────────────────────────────────────
# BE-007: Document Visibility PATCH
# ──────────────────────────────────────────────────────────────────────────────

def patch_document_visibility(handler, user, request_uid, payload) -> dict:
    """Offer owner updates document visibility setting."""
    visibility = str(payload.get("document_visibility", "")).strip()
    allowed = {"all_dealers", "approved_only", "none"}
    if visibility not in allowed:
        raise ValueError(f"document_visibility must be one of {sorted(allowed)}")

    with db_connect() as conn:
        req = conn.execute(
            "SELECT customer_id FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")
        if req["customer_id"] != user["id"]:
            raise PermissionError("Not your offer request")

        conn.execute(
            "UPDATE offer_requests SET document_visibility=?, updated_at=? WHERE request_uid=?",
            (visibility, utc_now_iso(), request_uid),
        )
        conn.commit()

    return {"ok": True, "document_visibility": visibility}


# ──────────────────────────────────────────────────────────────────────────────
# BE-008: Dealer Online Status
# ──────────────────────────────────────────────────────────────────────────────

def get_user_online_status(handler, user_id) -> dict:
    """Return is_online flag and last_seen for any user."""
    with db_connect() as conn:
        row = conn.execute(
            "SELECT last_seen FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    if not row:
        raise ValueError("user not found")
    last_seen = row["last_seen"]
    is_online = bool(last_seen and last_seen > plus_seconds_iso(-300))
    return {"is_online": is_online, "last_seen": last_seen}


# ──────────────────────────────────────────────────────────────────────────────
# BE-003: Dealer Subscription Status
# ──────────────────────────────────────────────────────────────────────────────

def get_dealer_subscription(handler, user) -> dict:
    """Return active subscription info for the current dealer."""
    dealer_id = user["id"]

    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT plan, amount_qar, is_active, started_at, expires_at, next_billing
            FROM dealer_subscriptions
            WHERE dealer_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (dealer_id,)
        ).fetchone()

    if not row:
        return {
            "is_active": False,
            "plan": None,
            "amount_qar": None,
            "started_at": None,
            "expires_at": None,
            "next_billing": None,
        }

    return {
        "is_active": bool(row["is_active"]),
        "plan": row["plan"],
        "amount_qar": row["amount_qar"],
        "started_at": row["started_at"],
        "expires_at": row["expires_at"],
        "next_billing": row["next_billing"],
    }


# ──────────────────────────────────────────────────────────────────────────────
# BE-004: Saved Dealer Filters
# ──────────────────────────────────────────────────────────────────────────────

def _compute_filter_match_count(conn, dealer_id: int, makes, cities, min_year, max_year, max_km, condition) -> int:
    """Count open offer_requests matching the saved filter criteria."""
    parts = ["status = 'open'"]
    vals: list = []
    if makes:
        placeholders = ",".join("?" * len(makes))
        parts.append(f"make IN ({placeholders})")
        vals.extend(makes)
    if cities:
        placeholders = ",".join("?" * len(cities))
        parts.append(f"city IN ({placeholders})")
        vals.extend(cities)
    if min_year:
        parts.append("year >= ?")
        vals.append(min_year)
    if max_year:
        parts.append("year <= ?")
        vals.append(max_year)
    if max_km:
        parts.append("km <= ?")
        vals.append(max_km)
    if condition:
        parts.append("condition = ?")
        vals.append(condition)
    where_sql = "WHERE " + " AND ".join(parts)
    row = conn.execute(f"SELECT COUNT(*) as c FROM offer_requests {where_sql}", vals).fetchone()
    return row["c"]


def list_saved_filters(handler, user) -> dict:
    """Return dealer's saved filters with match counts."""
    dealer_id = user["id"]

    with db_connect() as conn:
        rows = conn.execute(
            "SELECT * FROM dealer_saved_filters WHERE dealer_id = ? ORDER BY created_at ASC",
            (dealer_id,)
        ).fetchall()

        result = []
        for row in rows:
            f = dict(row)
            makes = json.loads(f.pop("makes_json") or "[]")
            cities = json.loads(f.pop("cities_json") or "[]")
            f["makes"] = makes
            f["cities"] = cities
            f["match_count"] = _compute_filter_match_count(
                conn, dealer_id, makes, cities,
                f.get("min_year"), f.get("max_year"), f.get("max_km"), f.get("condition")
            )
            result.append(f)

    return {"filters": result}


def create_saved_filter(handler, user, payload) -> tuple[dict, int]:
    """Create a new saved filter for a dealer."""
    dealer_id = user["id"]
    name = str(payload.get("name", "")).strip()
    if not name:
        raise ValueError("name is required")

    makes = payload.get("makes") or []
    cities = payload.get("cities") or []
    min_year = parse_int(payload.get("min_year"), 0) or None
    max_year = parse_int(payload.get("max_year"), 0) or None
    max_km = parse_int(payload.get("max_km"), 0) or None
    condition = str(payload.get("condition", "")).strip() or None
    now = utc_now_iso()

    with db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO dealer_saved_filters
            (dealer_id, name, makes_json, cities_json, min_year, max_year, max_km, condition, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dealer_id, name,
                json.dumps(makes) if makes else None,
                json.dumps(cities) if cities else None,
                min_year, max_year, max_km, condition, now,
            )
        )
        new_id = cur.lastrowid
        row = conn.execute("SELECT * FROM dealer_saved_filters WHERE id = ?", (new_id,)).fetchone()
        match_count = _compute_filter_match_count(conn, dealer_id, makes, cities, min_year, max_year, max_km, condition)
        conn.commit()

    f = dict(row)
    f["makes"] = json.loads(f.pop("makes_json") or "[]")
    f["cities"] = json.loads(f.pop("cities_json") or "[]")
    f["match_count"] = match_count
    return ({"filter": f}, HTTPStatus.CREATED)


def delete_saved_filter(handler, user, filter_id: int) -> dict:
    """Delete a saved filter (owner only)."""
    dealer_id = user["id"]

    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM dealer_saved_filters WHERE id = ?", (filter_id,)
        ).fetchone()
        if not row:
            raise ValueError("filter not found")
        if str(row["dealer_id"]) != str(dealer_id):
            raise PermissionError("Not your filter")

        conn.execute("DELETE FROM dealer_saved_filters WHERE id = ?", (filter_id,))
        conn.commit()

    return {"ok": True}


# ──────────────────────────────────────────────────────────────────────────────
# BE-005: Bid Expiration helpers
# ──────────────────────────────────────────────────────────────────────────────

def format_bid(row) -> dict:
    """Convert a bid row to dict and add is_expired computed field."""
    b = dict(row)
    if b.get("expires_at"):
        b["is_expired"] = b["expires_at"] < utc_now_iso()
    else:
        b["is_expired"] = False
    return b


def expire_stale_bids(conn) -> None:
    """Mark bids as expired if their expires_at has passed. Call at start of bid-listing endpoints."""
    conn.execute(
        "UPDATE offer_bids SET status='expired', updated_at=? WHERE expires_at IS NOT NULL AND expires_at < ? AND status='pending'",
        (utc_now_iso(), utc_now_iso())
    )


# ──────────────────────────────────────────────────────────────────────────────
# Admin functions for instant offers
# ──────────────────────────────────────────────────────────────────────────────

def admin_list_offer_requests(handler, params) -> dict:
    """Admin view of all offer requests with filters."""
    require_admin(handler)
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    offset = max(0, parse_int(params.get("offset", ["0"])[0], 0))
    status_filter = params.get("status", [""])[0].strip()
    customer_id_filter = params.get("customer_id", [""])[0].strip()
    make_filter = params.get("make", [""])[0].strip()
    search_filter = params.get("search", [""])[0].strip().lower()

    where_parts: list = []
    vals: list = []

    if status_filter:
        where_parts.append("status = ?")
        vals.append(status_filter)
    if customer_id_filter:
        where_parts.append("customer_id = ?")
        vals.append(customer_id_filter)
    if make_filter:
        where_parts.append("make = ?")
        vals.append(make_filter)
    if search_filter:
        where_parts.append("(lower(make) like ? or lower(class_name) like ? or lower(city) like ?)")
        token = f"%{search_filter}%"
        vals.extend([token, token, token])

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    with db_connect() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) as c FROM offer_requests {where_sql}", vals
        ).fetchone()["c"]
        rows = conn.execute(
            f"SELECT * FROM offer_requests {where_sql} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (*vals, limit, offset),
        ).fetchall()

    return {"rows": [dict(r) for r in rows], "total": total}


def admin_moderate_offer_request(handler, request_uid, payload) -> dict:
    """Admin force-set offer request status to cancelled or expired."""
    require_admin(handler)
    new_status = str(payload.get("status", "")).strip()
    if new_status not in ("cancelled", "expired"):
        raise ValueError("status must be 'cancelled' or 'expired'")

    now = utc_now_iso()
    with db_connect() as conn:
        req = conn.execute(
            "SELECT * FROM offer_requests WHERE request_uid = ?", (request_uid,)
        ).fetchone()
        if not req:
            raise ValueError("offer request not found")

        conn.execute(
            "UPDATE offer_requests SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, now, req["id"])
        )
        if new_status == "cancelled":
            conn.execute(
                "UPDATE offer_bids SET status = 'rejected', updated_at = ? WHERE request_id = ? AND status = 'pending'",
                (now, req["id"])
            )
        conn.commit()

    return {"ok": True}


def admin_list_bids(handler, params) -> dict:
    """Admin view of all bids with filters."""
    require_admin(handler)
    limit = max(1, min(parse_int(params.get("limit", ["50"])[0], 50), 200))
    offset = max(0, parse_int(params.get("offset", ["0"])[0], 0))
    status_filter = params.get("status", [""])[0].strip()
    dealer_id_filter = params.get("dealer_id", [""])[0].strip()
    request_uid_filter = params.get("request_uid", [""])[0].strip()

    where_parts: list = []
    vals: list = []

    if status_filter:
        where_parts.append("b.status = ?")
        vals.append(status_filter)
    if dealer_id_filter:
        where_parts.append("b.dealer_id = ?")
        vals.append(dealer_id_filter)
    if request_uid_filter:
        where_parts.append("r.request_uid = ?")
        vals.append(request_uid_filter)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    with db_connect() as conn:
        # BE-005: expire stale bids before admin view
        expire_stale_bids(conn)
        total = conn.execute(
            f"SELECT COUNT(*) as c FROM offer_bids b JOIN offer_requests r ON r.id = b.request_id {where_sql}", vals
        ).fetchone()["c"]
        rows = conn.execute(
            f"""
            SELECT b.*, r.request_uid, r.make, r.class_name, r.year, r.city
            FROM offer_bids b
            JOIN offer_requests r ON r.id = b.request_id
            {where_sql}
            ORDER BY b.created_at DESC
            LIMIT ? OFFSET ?
            """,
            (*vals, limit, offset),
        ).fetchall()

    return {"rows": [format_bid(r) for r in rows], "total": total}


# ──────────────────────────────────────────────────────────────────────────────
# ML Valuation
# ──────────────────────────────────────────────────────────────────────────────

def ml_estimate(params: dict) -> dict:
    """Call the ML model to estimate car price."""
    try:
        import sys as _sys
        _sys.path.insert(0, "/opt/mzad-db-ui/ml")
        from car_valuator import estimate_price  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError(f"ML model not available: {exc}") from exc

    features = {
        "make": params.get("make", [""])[0].strip() or None,
        "class_name": params.get("class_name", [""])[0].strip() or None,
        "manufacture_year": params.get("manufacture_year", [None])[0],
        "km": params.get("km", [None])[0],
        "fuel_type": params.get("fuel_type", [""])[0].strip() or None,
        "gear_type": params.get("gear_type", [""])[0].strip() or None,
        "car_type": params.get("car_type", [""])[0].strip() or None,
        "cylinder_count": params.get("cylinder_count", [None])[0],
        "city": params.get("city", [""])[0].strip() or None,
        "warranty_status": params.get("warranty_status", [""])[0].strip() or None,
        "seller_type": params.get("seller_type", [""])[0].strip() or None,
    }
    # Remove None values so model uses its own defaults
    features = {k: v for k, v in features.items() if v is not None}
    return estimate_price(features)


def push_test(payload: dict) -> dict:
    user_key = str(payload.get("user_key", "")).strip()
    if not user_key:
        raise ValueError("user_key is required")

    with db_connect() as conn:
        subs = conn.execute(
            "select id, endpoint, subscription_json from web_push_subscriptions where user_key = ? and active = 1",
            (user_key,),
        ).fetchall()

        if not subs:
            return {"ok": False, "sent": 0, "reason": "no_active_subscription"}

        sent = 0
        failed = 0
        for row in subs:
            subscription = json.loads(row["subscription_json"])
            ok, msg = send_web_push(
                subscription,
                {
                    "title": "Mzad Push Test",
                    "body": "Web push is configured correctly for your user.",
                    "url": "https://mzadqatar.com",
                },
            )
            if ok:
                sent += 1
                conn.execute(
                    "update web_push_subscriptions set last_success_at = ?, last_error = null, updated_at = ? where id = ?",
                    (utc_now_iso(), utc_now_iso(), row["id"]),
                )
            else:
                failed += 1
                conn.execute(
                    "update web_push_subscriptions set last_error = ?, updated_at = ? where id = ?",
                    (msg[:800], utc_now_iso(), row["id"]),
                )
        conn.commit()
    return {"ok": sent > 0, "sent": sent, "failed": failed}


def trigger_incremental_collection() -> int:
    script = Path("/home/briyad/collect_mzad_api_incremental.py")
    if not script.exists():
        raise FileNotFoundError(f"Collector script not found: {script}")
    proc = subprocess.run(
        [sys.executable, str(script), "--max-pages", "40", "--numberperpage", "50", "--stale-pages", "8"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"collector failed with code {proc.returncode}")
    new_rows = 0
    for line in (proc.stdout or "").splitlines():
        if line.startswith("new_rows="):
            new_rows = parse_int(line.split("=", 1)[1], 0)
    return new_rows


def placeholder_image_svg() -> bytes:
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 320 180'>"
        "<defs><linearGradient id='g' x1='0' x2='1' y1='0' y2='1'>"
        "<stop offset='0%' stop-color='#edf2f7'/>"
        "<stop offset='100%' stop-color='#d9e2ec'/>"
        "</linearGradient></defs>"
        "<rect width='320' height='180' fill='url(#g)'/>"
        "<rect x='22' y='40' width='276' height='100' rx='12' fill='#cbd5e1'/>"
        "<text x='160' y='96' text-anchor='middle' font-size='18' fill='#334155' font-family='Arial, sans-serif'>"
        "Image unavailable"
        "</text>"
        "</svg>"
    )
    return svg.encode("utf-8")


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_OPTIONS(self) -> None:
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, X-Guest-Token, Content-Type")
        self.send_header("Access-Control-Max-Age", "86400")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self.handle_api_get(parsed)
            return
        if parsed.path.startswith("/uploads/"):
            name = parsed.path.split("/uploads/", 1)[1].strip()
            if not name:
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
                return
            self.send_file(UPLOADS_DIR / name, "application/octet-stream")
            return
        if parsed.path == "/swagger.yaml":
            self.send_file(APP_DIR / "swagger.yaml", "application/yaml; charset=utf-8")
            return
        if parsed.path == "/docs":
            self.path = "/docs.html"
            super().do_GET()
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
            return
        try:
            payload = load_json_body(self)
            if parsed.path == "/api/auth/guest/login":
                self.send_json(auth_guest_login())
                return
            if parsed.path == "/api/auth/register":
                data, status = auth_register(payload)
                self.send_json(data, status=status)
                return
            if parsed.path == "/api/auth/login":
                self.send_json(auth_login(payload))
                return
            if parsed.path == "/api/auth/refresh":
                self.send_json(auth_refresh(payload))
                return
            if parsed.path == "/api/auth/change-password":
                self.send_json(auth_change_password(self, payload))
                return
            if parsed.path == "/api/auth/request-reset":
                self.send_json(auth_request_reset(payload))
                return
            if parsed.path == "/api/auth/reset-password":
                self.send_json(auth_reset_password(payload))
                return
            if parsed.path == "/api/auth/admin-bootstrap":
                data, status = auth_admin_bootstrap(payload)
                self.send_json(data, status=status)
                return
            if parsed.path == "/api/auth/logout":
                self.send_json(auth_logout(self))
                return
            if parsed.path == "/api/listings":
                data, status = create_listing(self, payload)
                self.send_json(data, status=status)
                return
            if parsed.path.startswith("/api/listings/") and parsed.path.endswith("/view"):
                # No auth check needed: this endpoint is public telemetry.
                pid = parsed.path[len("/api/listings/") : -len("/view")].strip().strip("/")
                if not pid:
                    raise ValueError("product_id is required")
                self.send_json(track_listing_view(self, pid, payload))
                return
            if parsed.path == "/api/uploads":
                data, status = create_upload(self, payload)
                self.send_json(data, status=status)
                return
            if parsed.path == "/api/favorites":
                data, status = create_favorite(self, payload)
                self.send_json(data, status=status)
                return
            if parsed.path == "/api/alerts":
                self.send_json(create_alert(payload))
                return
            if parsed.path == "/api/alerts/active":
                self.send_json(set_alert_active(payload))
                return
            if parsed.path == "/api/channels":
                self.send_json(upsert_channels(payload))
                return
            if parsed.path == "/api/channels/test-whatsapp":
                self.send_json(send_test_whatsapp(payload))
                return
            if parsed.path == "/api/push/subscribe":
                self.send_json(upsert_push_subscription(payload))
                return
            if parsed.path == "/api/push/unsubscribe":
                self.send_json(deactivate_push_subscription(payload))
                return
            if parsed.path == "/api/push/test":
                self.send_json(push_test(payload))
                return
            if parsed.path == "/api/notifications/mark-read":
                self.send_json(mark_notifications_read(payload))
                return
            if parsed.path == "/api/collector/run":
                count = trigger_incremental_collection()
                price_history_rows = 0
                with db_connect() as conn:
                    price_history_rows = record_daily_price_history_snapshot(conn, TABLE_NAME, source="collector")
                    conn.commit()
                deal_stats = recompute_deals_in_db()
                run_alert_matcher_background()
                self.send_json(
                    {
                        "ok": True,
                        "new_rows": count,
                        "deals": deal_stats,
                        "price_history_rows": price_history_rows,
                    }
                )
                return
            if parsed.path == "/api/deals/recompute":
                require_admin(self)
                self.send_json(recompute_deals_in_db())
                return
            if parsed.path.startswith("/api/admin/listings/") and parsed.path.endswith("/approval"):
                pid = parsed.path[len("/api/admin/listings/") : -len("/approval")].strip().strip("/")
                self.send_json(set_listing_approval(self, pid, payload))
                return
            # ── Instant Offers POST routes ─────────────────────────────────
            if parsed.path == "/api/instant-offers/requests":
                user = require_auth(self, allow_guest=False)
                data, status = create_offer_request(self, user, payload)
                self.send_json(data, status=status)
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/bids"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/bids")].strip("/")
                user = require_dealer(self)
                data, status = create_offer_bid(self, user, uid, payload)
                self.send_json(data, status=status)
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/accept-bid"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/accept-bid")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(accept_offer_bid(self, user, uid, payload))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/reject-bid"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/reject-bid")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(reject_offer_bid(self, user, uid, payload))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/cancel"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/cancel")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(cancel_offer_request(self, user, uid))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/decline"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/decline")].strip("/")
                user = require_dealer(self)
                self.send_json(decline_offer_request(self, user, uid))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/messages"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/messages")].strip("/")
                user = require_auth(self, allow_guest=False)
                data, status = send_offer_message(self, user, uid, payload)
                self.send_json(data, status=status)
                return
            if parsed.path.startswith("/api/instant-offers/bids/") and parsed.path.endswith("/withdraw"):
                uid = parsed.path[len("/api/instant-offers/bids/"):-len("/withdraw")].strip("/")
                user = require_dealer(self)
                self.send_json(withdraw_offer_bid(self, user, uid))
                return
            if parsed.path.startswith("/api/admin/instant-offers/requests/") and parsed.path.endswith("/moderate"):
                uid = parsed.path[len("/api/admin/instant-offers/requests/"):-len("/moderate")].strip("/")
                self.send_json(admin_moderate_offer_request(self, uid, payload))
                return
            # BE-004: create saved filter
            if parsed.path == "/api/instant-offers/filters":
                user = require_dealer(self)
                data, status = create_saved_filter(self, user, payload)
                self.send_json(data, status=status)
                return
            # BE-001: phone access requests
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/phone-request/approve"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/phone-request/approve")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(approve_phone_request(self, user, uid, payload))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/phone-request/reject"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/phone-request/reject")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(reject_phone_request(self, user, uid, payload))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/phone-request"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/phone-request")].strip("/")
                user = require_dealer(self)
                data, status = create_phone_request(self, user, uid)
                self.send_json(data, status=status)
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
        except PermissionError as exc:
            msg = str(exc) or "Unauthorized"
            code = HTTPStatus.FORBIDDEN if "Admin" in msg or "not allowed" in msg else HTTPStatus.UNAUTHORIZED
            self.send_json({"error": msg}, status=code)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
            return
        try:
            payload = load_json_body(self)
            if parsed.path == "/api/me/profile":
                self.send_json(upsert_profile(self, payload))
                return
            if parsed.path.startswith("/api/users/"):
                user_id = parsed.path.split("/api/users/", 1)[1].strip()
                self.send_json(put_user(self, user_id, payload))
                return
            if parsed.path.startswith("/api/listings/"):
                pid = parsed.path.split("/api/listings/", 1)[1].strip()
                self.send_json(update_listing(self, pid, payload, replace=True))
                return
            if parsed.path == "/api/instant-offers/preferences":
                user = require_dealer(self)
                self.send_json(upsert_dealer_preferences(self, user, payload))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
        except PermissionError as exc:
            msg = str(exc) or "Unauthorized"
            code = HTTPStatus.FORBIDDEN if "Admin" in msg or "not allowed" in msg else HTTPStatus.UNAUTHORIZED
            self.send_json({"error": msg}, status=code)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_PATCH(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
            return
        try:
            payload = load_json_body(self)
            if parsed.path == "/api/me/profile":
                self.send_json(upsert_profile(self, payload))
                return
            if parsed.path.startswith("/api/users/"):
                user_id = parsed.path.split("/api/users/", 1)[1].strip()
                self.send_json(patch_user(self, user_id, payload))
                return
            if parsed.path.startswith("/api/listings/"):
                pid = parsed.path.split("/api/listings/", 1)[1].strip()
                self.send_json(update_listing(self, pid, payload, replace=False))
                return
            if parsed.path.startswith("/api/instant-offers/bids/"):
                uid = parsed.path[len("/api/instant-offers/bids/"):].strip("/")
                user = require_dealer(self)
                self.send_json(patch_offer_bid(self, user, uid, payload))
                return
            # BE-007: document visibility control
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/document-visibility"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/document-visibility")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(patch_document_visibility(self, user, uid, payload))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
        except PermissionError as exc:
            msg = str(exc) or "Unauthorized"
            code = HTTPStatus.FORBIDDEN if "Admin" in msg or "not allowed" in msg else HTTPStatus.UNAUTHORIZED
            self.send_json({"error": msg}, status=code)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if not parsed.path.startswith("/api/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
            return
        try:
            if parsed.path.startswith("/api/users/"):
                user_id = parsed.path.split("/api/users/", 1)[1].strip()
                delete_user(self, user_id)
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return
            if parsed.path.startswith("/api/listings/"):
                pid = parsed.path.split("/api/listings/", 1)[1].strip()
                delete_listing(self, pid)
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return
            if parsed.path.startswith("/api/favorites/"):
                fid_text = parsed.path.split("/api/favorites/", 1)[1].strip()
                fid = parse_int(fid_text, 0)
                if fid <= 0:
                    raise ValueError("invalid favorite id")
                params = parse_qs(parsed.query)
                delete_favorite(self, fid, params)
                self.send_response(HTTPStatus.NO_CONTENT)
                self.end_headers()
                return
            # BE-004: delete saved filter
            if parsed.path.startswith("/api/instant-offers/filters/"):
                filter_id_text = parsed.path[len("/api/instant-offers/filters/"):].strip("/")
                filter_id = parse_int(filter_id_text, 0)
                if filter_id <= 0:
                    raise ValueError("invalid filter id")
                user = require_dealer(self)
                self.send_json(delete_saved_filter(self, user, filter_id))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
        except PermissionError as exc:
            msg = str(exc) or "Unauthorized"
            code = HTTPStatus.FORBIDDEN if "Admin" in msg or "not allowed" in msg else HTTPStatus.UNAUTHORIZED
            self.send_json({"error": msg}, status=code)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def handle_api_get(self, parsed):
        try:
            if parsed.path == "/api/me":
                self.send_json(me(self))
                return
            if parsed.path == "/api/me/profile":
                self.send_json(get_profile(self))
                return
            if parsed.path == "/api/users":
                self.send_json(list_users(self))
                return
            # BE-008: online status (before the generic /api/users/{id} catch-all)
            if parsed.path.startswith("/api/users/") and parsed.path.endswith("/online-status"):
                uid = parsed.path[len("/api/users/"):-len("/online-status")].strip("/")
                require_auth(self, allow_guest=True)
                self.send_json(get_user_online_status(self, uid))
                return
            if parsed.path.startswith("/api/users/"):
                user_id = parsed.path.split("/api/users/", 1)[1].strip()
                self.send_json(get_user_by_id(self, user_id))
                return
            if parsed.path == "/api/health":
                self.send_json({"status": "ok"})
                return
            if parsed.path == "/api/makes":
                with db_connect() as conn:
                    rows = conn.execute(
                        f"SELECT DISTINCT make FROM {TABLE_NAME} WHERE make IS NOT NULL AND make <> '' ORDER BY make ASC"
                    ).fetchall()
                self.send_json({"makes": [r["make"] for r in rows]})
                return
            if parsed.path == "/api/summary":
                self.send_json(fetch_summary())
                return
            if parsed.path == "/api/spotted":
                self.send_json(fetch_spotted())
                return
            if parsed.path == "/api/ranges":
                self.send_json(fetch_ranges())
                return
            if parsed.path == "/api/img-proxy":
                params = parse_qs(parsed.query)
                target_url = params.get("url", [None])[0]
                allowed_hosts = {
                    "files.qatarliving.com",
                    "images.qatarliving.com",
                    "content.mzadqatar.com",
                }
                host = urlparse(str(target_url or "")).hostname
                if not target_url or not host or not any(
                    host == h or host.endswith(f".{h}") for h in allowed_hosts
                ):
                    self.send_error(HTTPStatus.FORBIDDEN, "Origin not allowed")
                    return
                try:
                    req = urllib.request.Request(
                        str(target_url),
                        headers={"User-Agent": "Mozilla/5.0 (compatible; MzadCarsBot/1.0)"},
                    )
                    with urllib.request.urlopen(req, timeout=10) as resp:
                        data = resp.read()
                        content_type = resp.headers.get("Content-Type", "image/jpeg")
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Cache-Control", "public, max-age=86400")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.end_headers()
                    self.wfile.write(data)
                    return
                except Exception as exc:  # noqa: BLE001
                    # Fallback: return a lightweight placeholder image instead of failing hard.
                    data = placeholder_image_svg()
                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", "image/svg+xml; charset=utf-8")
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Cache-Control", "public, max-age=3600")
                    self.send_header("Access-Control-Allow-Origin", "*")
                    self.send_header("X-Proxy-Fallback", "1")
                    self.send_header("X-Proxy-Error", str(exc)[:120])
                    self.end_headers()
                    self.wfile.write(data)
                    return
            if parsed.path == "/api/listings":
                params = parse_qs(parsed.query)
                requested_approval = params.get("is_approved", ["1"])[0].strip().lower()
                if requested_approval not in {"0", "1", "any"}:
                    requested_approval = "1"
                include_views = params.get("include_views", ["0"])[0].strip() == "1"
                include_unapproved = (
                    params.get("include_unapproved", ["0"])[0].strip() == "1"
                    or requested_approval in {"0", "any"}
                )
                if include_unapproved:
                    require_admin(self)
                self.send_json(
                    fetch_listings(
                        params,
                        include_unapproved=include_unapproved,
                        approval_filter=requested_approval,
                        include_views=include_views,
                    )
                )
                return
            if parsed.path == "/api/favorites":
                params = parse_qs(parsed.query)
                self.send_json(get_favorites(self, params))
                return
            if parsed.path.startswith("/api/listings/"):
                if parsed.path.endswith("/peers"):
                    pid = parsed.path[len("/api/listings/") : -len("/peers")].strip().strip("/")
                    if not pid:
                        raise ValueError("product_id is required")
                    self.send_json(fetch_listing_peers(pid))
                    return
                if parsed.path.endswith("/price-history"):
                    # internal — not in swagger contract
                    pid = parsed.path[len("/api/listings/") : -len("/price-history")].strip().strip("/")
                    if not pid:
                        raise ValueError("product_id is required")
                    self.send_json(fetch_price_history(pid))
                    return
                pid = parsed.path.split("/api/listings/", 1)[1].strip()
                params = parse_qs(parsed.query)
                include_unapproved = params.get("include_unapproved", ["0"])[0].strip() == "1"
                if include_unapproved:
                    require_admin(self)
                self.send_json(get_listing(pid, include_unapproved=include_unapproved))
                return
            if parsed.path == "/api/value-estimate":
                params = parse_qs(parsed.query)
                self.send_json(estimate_value(params))
                return
            if parsed.path == "/api/ml/estimate":
                params = parse_qs(parsed.query)
                self.send_json(ml_estimate(params))
                return
            # ── Instant Offers GET routes ──────────────────────────────────
            if parsed.path == "/api/instant-offers/comps":
                params = parse_qs(parsed.query)
                make = params.get("make", [""])[0].strip()
                class_name = params.get("class_name", [""])[0].strip()
                model = params.get("model", [""])[0].strip()
                year = parse_int(params.get("year", ["0"])[0], 0)
                km = parse_int(params.get("km", ["0"])[0], 0)
                self.send_json(fetch_offer_comps(make, class_name, model, year, km))
                return
            if parsed.path == "/api/instant-offers/requests/mine":
                user = require_auth(self, allow_guest=False)
                params = parse_qs(parsed.query)
                self.send_json(list_my_offer_requests(self, user, params))
                return
            if parsed.path == "/api/instant-offers/requests":
                user = require_auth(self, allow_guest=False)
                params = parse_qs(parsed.query)
                if user["role"] in ("dealer", "admin"):
                    self.send_json(list_offer_requests_for_dealer(self, user, params))
                else:
                    self.send_json(list_my_offer_requests(self, user, params))
                return
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/messages"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/messages")].strip("/")
                user = require_auth(self, allow_guest=False)
                params = parse_qs(parsed.query)
                self.send_json(list_offer_messages(self, user, uid, params))
                return
            # BE-001: GET phone-requests list (must be before the catch-all)
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/phone-requests"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/phone-requests")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(list_phone_requests(self, user, uid))
                return
            # BE-006: admin phone approval audit log
            if parsed.path.startswith("/api/instant-offers/requests/") and parsed.path.endswith("/phone-approval-log"):
                uid = parsed.path[len("/api/instant-offers/requests/"):-len("/phone-approval-log")].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(get_phone_approval_log(self, user, uid))
                return
            if parsed.path.startswith("/api/instant-offers/requests/"):
                uid = parsed.path[len("/api/instant-offers/requests/"):].strip("/")
                user = require_auth(self, allow_guest=False)
                self.send_json(get_offer_request_detail(self, user, uid))
                return
            if parsed.path == "/api/instant-offers/bids/mine":
                user = require_dealer(self)
                params = parse_qs(parsed.query)
                self.send_json(list_my_bids(self, user, params))
                return
            # BE-003: dealer subscription
            if parsed.path == "/api/instant-offers/subscription":
                user = require_dealer(self)
                self.send_json(get_dealer_subscription(self, user))
                return
            # BE-004: saved filters
            if parsed.path == "/api/instant-offers/filters":
                user = require_dealer(self)
                self.send_json(list_saved_filters(self, user))
                return
            if parsed.path == "/api/instant-offers/preferences":
                user = require_dealer(self)
                self.send_json(get_dealer_preferences(self, user))
                return
            if parsed.path == "/api/admin/instant-offers/requests":
                params = parse_qs(parsed.query)
                self.send_json(admin_list_offer_requests(self, params))
                return
            if parsed.path == "/api/admin/instant-offers/bids":
                params = parse_qs(parsed.query)
                self.send_json(admin_list_bids(self, params))
                return
            if parsed.path == "/api/channels":
                params = parse_qs(parsed.query)
                self.send_json(fetch_channels(params.get("user_key", [""])[0].strip()))
                return
            if parsed.path == "/api/push/public-key":
                self.send_json(push_public_key_info())
                return
            if parsed.path == "/api/push/status":
                params = parse_qs(parsed.query)
                self.send_json(fetch_push_status(params.get("user_key", [""])[0].strip()))
                return
            if parsed.path == "/api/alerts":
                params = parse_qs(parsed.query)
                self.send_json(fetch_alerts(params.get("user_key", [""])[0].strip()))
                return
            if parsed.path == "/api/notifications":
                params = parse_qs(parsed.query)
                self.send_json(fetch_notifications(params))
                return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown API endpoint")
        except PermissionError as exc:
            msg = str(exc) or "Unauthorized"
            code = HTTPStatus.FORBIDDEN if "Admin" in msg or "not allowed" in msg else HTTPStatus.UNAUTHORIZED
            self.send_json({"error": msg}, status=code)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def send_json(self, payload: dict, status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: Path, content_type: str) -> None:
        if not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser(description="Search UI for local Mzad DB")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8090)
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")

    with db_connect() as conn:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    for attempt in range(5):
        try:
            ensure_notification_schema()
            break
        except sqlite3.OperationalError as e:
            if 'locked' in str(e) and attempt < 4:
                time.sleep(2 ** attempt)
            else:
                raise

    ensure_instant_offer_schema()
    try:
        recompute_deals_in_db()
    except Exception as exc:  # noqa: BLE001
        print(f"Deal recompute skipped: {exc}")
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Serving Mzad DB UI on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
