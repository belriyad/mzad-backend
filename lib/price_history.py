"""Price history helpers for listing_price_history table operations."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def record_price_history(
    conn: sqlite3.Connection,
    product_id: str,
    price_qar: float,
    source: str = "collector",
) -> None:
    """Record one daily price snapshot in listing_price_history for a listing.

    Touches table: listing_price_history.
    """
    if not product_id:
        return
    now = _utc_now_iso()
    conn.execute(
        """
        INSERT INTO listing_price_history (product_id, price_qar, source, recorded_at)
        SELECT ?, ?, ?, ?
        WHERE NOT EXISTS (
            SELECT 1
            FROM listing_price_history ph
            WHERE ph.product_id = ?
              AND date(ph.recorded_at) = date(?)
        )
        """,
        (product_id, float(price_qar), source, now, product_id, now),
    )


def record_daily_price_history_snapshot(
    conn: sqlite3.Connection,
    table_name: str,
    source: str = "collector",
) -> int:
    """Record daily price snapshots for listings table rows with non-null prices.

    Touches tables: car_listings_api_10000 (or provided table), listing_price_history.
    Returns number of rows inserted.
    """
    now = _utc_now_iso()
    # table_name is caller-controlled constant, not user input.
    sql = f"""
        INSERT INTO listing_price_history (product_id, price_qar, source, recorded_at)
        SELECT l.product_id, CAST(l.price_qar AS REAL), ?, ?
        FROM {table_name} l
        WHERE l.product_id IS NOT NULL
          AND l.price_qar IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM listing_price_history ph
              WHERE ph.product_id = l.product_id
                AND date(ph.recorded_at) = date(?)
          )
    """
    cur = conn.execute(sql, (source, now, now))
    return int(cur.rowcount or 0)
