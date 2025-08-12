# api.py — ToolTally Backend API (Flask + SQLite)
# Reads from canonical tables: products, offers, vendors
# Env: DB_PATH (default: data/tooltally.db)

from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Tuple

from flask import Flask, g, jsonify, request

DB_PATH = os.environ.get("DB_PATH", os.path.join("data", "tooltally.db"))

app = Flask(__name__)

# ---------- DB helpers ----------
def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> Dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def get_db() -> sqlite3.Connection:
    db = getattr(g, "_db", None)
    if db is None:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = _dict_factory
        db.execute("PRAGMA journal_mode=WAL;")
        db.execute("PRAGMA synchronous=NORMAL;")
        g._db = db
    return db

@app.teardown_appcontext
def close_db(exception=None):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()

# ---------- Utilities ----------
def _parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _build_search_filters(term: str | None) -> Tuple[str, List[str]]:
    """
    Tokenized LIKE search on products.name; all tokens must match (AND).
    """
    if not term:
        return "", []
    tokens = [t.strip() for t in term.split() if t.strip()]
    if not tokens:
        return "", []
    clauses, params = [], []
    for t in tokens:
        clauses.append("LOWER(p.name) LIKE ?")
        params.append(f"%{t.lower()}%")
    return " AND ".join(clauses), params

# ---------- Routes ----------
@app.route("/health")
def health():
    try:
        con = get_db()
        con.execute("SELECT 1")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/products", methods=["GET"])
def list_products():
    """
    Returns unique products with min_price and vendors_count.
    Supports: search (or q), category, page, limit.
    Response: { items, total, page, limit }
    """
    con = get_db()
    search = request.args.get("search") or request.args.get("q") or ""
    category = (request.args.get("category") or "").strip()
    page = _parse_int(request.args.get("page"), 1)
    limit = _parse_int(request.args.get("limit"), 24)
    if page < 1: page = 1
    if limit < 1: limit = 24

    where_parts: List[str] = []
    params: List[Any] = []

    sw, sp = _build_search_filters(search)
    if sw:
        where_parts.append(f"({sw})")
        params.extend(sp)

    if category:
        where_parts.append("(p.category = ?)")
        params.append(category)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    total_sql = f"""
        SELECT COUNT(*) AS total
        FROM (
          SELECT p.id
          FROM products p
          JOIN offers o ON o.product_id = p.id
          {where_sql}
          GROUP BY p.id
        ) x
    """
    total = int(con.execute(total_sql, params).fetchone()["total"])
    offset = (page - 1) * limit

    items_sql = f"""
        SELECT
          p.id,
          p.name,
          p.category,
          MIN(o.price_pounds) AS min_price,
          COUNT(DISTINCT o.vendor_id) AS vendors_count
        FROM products p
        JOIN offers o ON o.product_id = p.id
        {where_sql}
        GROUP BY p.id
        ORDER BY min_price ASC, p.name ASC
        LIMIT ? OFFSET ?
    """
    rows = con.execute(items_sql, params + [limit, offset]).fetchall()
    items = [
        {
            "id": r["id"],
            "name": r["name"],
            "category": r.get("category") or None,
            "min_price": float(r["min_price"]) if r["min_price"] is not None else None,
            "vendors_count": int(r["vendors_count"]) if r["vendors_count"] is not None else 0,
        }
        for r in rows
    ]
    return jsonify({"items": items, "total": total, "page": page, "limit": limit})

@app.route("/products/<int:product_id>", methods=["GET"])
def get_product(product_id: int):
    """
    Returns one product with all vendor offers sorted by price.
    """
    con = get_db()
    p = con.execute(
        "SELECT id, name, category FROM products WHERE id = ?",
        (product_id,)
    ).fetchone()
    if not p:
        return jsonify({"error": "Not found"}), 404

    offers = con.execute(
        """
        SELECT v.name AS vendor, o.price_pounds AS price, o.url AS buy_url
        FROM offers o
        JOIN vendors v ON v.id = o.vendor_id
        WHERE o.product_id = ?
        ORDER BY o.price_pounds ASC, v.name ASC
        """,
        (product_id,)
    ).fetchall()

    return jsonify({
        "id": p["id"],
        "name": p["name"],
        "category": p.get("category") or None,
        "vendors": [
            {"vendor": r["vendor"],
             "price": float(r["price"]) if r["price"] is not None else None,
             "buy_url": r["buy_url"]}
            for r in offers
        ],
    })

@app.route("/categories", methods=["GET"])
def list_categories():
    con = get_db()
    rows = con.execute(
        """
        SELECT DISTINCT category
        FROM products
        WHERE category IS NOT NULL AND TRIM(category) <> ''
        ORDER BY category COLLATE NOCASE ASC
        """
    ).fetchall()
    return jsonify([r["category"] for r in rows])

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
