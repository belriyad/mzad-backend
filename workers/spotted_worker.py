"""Spotted feed cache computation worker for spotted_cache endpoint."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone


def compute_spotted_cache(conn: sqlite3.Connection, table_name: str) -> int:
    """Compute and persist top spotted listings into spotted_cache.

    Touches tables: car_listings_api_10000 (or provided table), spotted_cache.
    Returns number of cached rows.
    """
    rows = conn.execute(
        f"""
        SELECT product_id, discount_pct, km, manufacture_year, peer_count, main_image_url
        FROM {table_name}
        WHERE COALESCE(is_approved, 0) = 1
        """
    ).fetchall()

    current_year = datetime.now(timezone.utc).year
    scored: list[tuple[str, int, str]] = []
    for r in rows:
        product_id = str(r["product_id"] or "").strip()
        if not product_id:
            continue
        discount_pct = float(r["discount_pct"]) if r["discount_pct"] is not None else 0.0
        km = int(r["km"]) if r["km"] is not None else 999999
        year = int(r["manufacture_year"]) if r["manufacture_year"] is not None else 0
        peer_count = int(r["peer_count"]) if r["peer_count"] is not None else 0
        main_image_url = str(r["main_image_url"] or "").strip()

        score = 0
        reasons: list[str] = []

        if discount_pct < -15:
            score += 3
            reasons.append("Hot deal")
        elif discount_pct < -5:
            score += 2
            reasons.append("Good deal")

        if km < 20000:
            score += 2
            reasons.append("Low KM")

        if year >= current_year - 1:
            score += 2
            reasons.append("Nearly new")

        if peer_count >= 5:
            score += 1
            reasons.append("Well-priced vs peers")

        if not main_image_url:
            score -= 2

        scored.append((product_id, score, json.dumps(reasons, ensure_ascii=False)))

    top = sorted(scored, key=lambda x: x[1], reverse=True)[:50]

    conn.execute("DELETE FROM spotted_cache")
    for product_id, score, reasons_json in top:
        conn.execute(
            """
            INSERT INTO spotted_cache (product_id, score, reasons, cached_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(product_id) DO UPDATE SET
              score = excluded.score,
              reasons = excluded.reasons,
              cached_at = excluded.cached_at
            """,
            (product_id, score, reasons_json, datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
        )
    return len(top)
