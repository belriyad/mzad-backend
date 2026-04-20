"""Alert matching worker for new listings -> user_notifications dispatch."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Callable


def _matches_alert(listing: dict, alert: sqlite3.Row) -> bool:
    if alert["make"] and str(listing.get("make") or "").lower() != str(alert["make"]).lower():
        return False
    if alert["class_name"] and str(listing.get("class_name") or "").lower() != str(alert["class_name"]).lower():
        return False
    if alert["min_year"] and int(listing.get("manufacture_year") or 0) < int(alert["min_year"]):
        return False
    if alert["max_year"] and int(listing.get("manufacture_year") or 9999) > int(alert["max_year"]):
        return False
    if alert["min_price_qar"] and float(listing.get("price_qar") or 0) < float(alert["min_price_qar"]):
        return False
    if alert["max_price_qar"] and float(listing.get("price_qar") or 0) > float(alert["max_price_qar"]):
        return False
    if alert["min_km"] and float(listing.get("km") or 0) < float(alert["min_km"]):
        return False
    if alert["max_km"] and float(listing.get("km") or 999999) > float(alert["max_km"]):
        return False
    if int(alert["deals_only"] or 0) == 1 and float(listing.get("discount_pct") or 0) >= 0:
        return False
    if alert["min_discount_pct"] is not None and float(listing.get("discount_pct") or 0) > float(alert["min_discount_pct"]):
        return False
    if alert["search_text"]:
        hay = f"{listing.get('title') or ''} {listing.get('make') or ''} {listing.get('class_name') or ''}".lower()
        if str(alert["search_text"]).lower() not in hay:
            return False
    return True


def run_alert_matcher(
    conn: sqlite3.Connection,
    table_name: str,
    notify_fn: Callable[[str, dict], None],
) -> dict:
    """Match active alerts against recent approved listings and dispatch notifications.

    Touches tables: user_alerts, user_notifications, car_listings_api_10000 (or provided table).
    """
    alerts = conn.execute("SELECT * FROM user_alerts WHERE active = 1").fetchall()
    threshold_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - (2 * 60 * 60 * 1000)
    listings = conn.execute(
        f"""
        SELECT *
        FROM {table_name}
        WHERE COALESCE(is_approved, 0) = 1
          AND CAST(COALESCE(date_of_advertise, '0') AS INTEGER) >= ?
        """,
        (threshold_ms,),
    ).fetchall()

    sent = 0
    for alert in alerts:
        matched = [dict(l) for l in listings if _matches_alert(dict(l), alert)]
        if not matched:
            continue
        for listing in matched[:3]:
            title = f"New match: {(listing.get('make') or '').strip()} {(listing.get('class_name') or '').strip()}".strip()
            message = (
                f"{listing.get('title') or ''} · QAR {int(float(listing.get('price_qar') or 0)):,} · "
                f"{listing.get('city') or 'Qatar'}"
            )
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO user_notifications
                (user_key, alert_id, product_id, title, price_qar, city, url, main_image_url, listing_date, message, is_read, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    alert["user_key"],
                    alert["id"],
                    listing.get("product_id"),
                    title,
                    int(float(listing.get("price_qar") or 0)) if listing.get("price_qar") is not None else None,
                    listing.get("city"),
                    listing.get("url"),
                    listing.get("main_image_url"),
                    str(listing.get("date_of_advertise") or ""),
                    message,
                    datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                ),
            )
            if int(cur.rowcount or 0) > 0:
                sent += 1
                notify_fn(str(alert["user_key"]), listing)

    return {"ok": True, "notifications": sent, "alerts_checked": len(alerts)}
