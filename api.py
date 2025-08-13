# tooltally-scrapers/api.py
import os
import sqlite3
from flask import Flask, request, jsonify, g
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "data", "tooltally.db")
DB_PATH = os.path.abspath(DB_PATH)

app = Flask(__name__)

# -------------------- DB helpers --------------------

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys=ON;")
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()

def tokens_from_query(q: str):
    return [t.strip() for t in (q or "").split() if t.strip()]

def build_filter_sql(tokens, category):
    """Match semantics: each token must match name OR category (AND across tokens)."""
    wheres = []
    params = []

    # Constrain to products that HAVE offers by joining to offers-agg (done in queries)
    # Tokens:
    for t in tokens:
        wheres.append("(LOWER(p.name) LIKE ? OR LOWER(p.category) LIKE ?)")
        like = f"%{t.lower()}%"
        params.extend([like, like])

    # Optional category filter
    if category:
        wheres.append("LOWER(p.category) = ?")
        params.append(category.lower())

    if wheres:
        return "WHERE " + " AND ".join(wheres), params
    return "", []

# -------------------- Routes --------------------

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/products")
def products():
    """
    Returns ONLY products that actually have offers.
    Paginated shape:
    {
      items: [{ id, name, category, min_price, vendors_count }],
      total, page, limit
    }
    """
    db = get_db()
    q = request.args.get("search", "", type=str)
    category = request.args.get("category", "", type=str)
    page = max(request.args.get("page", 1, type=int), 1)
    limit = min(max(request.args.get("limit", 24, type=int), 1), 100)
    offset = (page - 1) * limit

    tokens = tokens_from_query(q)

    # offers aggregate ensures we only see products WITH offers
    # stats: min price and distinct vendor count per product
    where_sql, where_params = build_filter_sql(tokens, category)

    # total count over filtered set (products with offers)
    total_sql = f"""
        WITH stats AS (
            SELECT product_id,
                   MIN(price_pounds) AS min_price,
                   COUNT(DISTINCT vendor_id) AS vendors_count
            FROM offers
            GROUP BY product_id
        )
        SELECT COUNT(*)
        FROM products p
        JOIN stats s ON s.product_id = p.id
        {where_sql}
    """
    total = db.execute(total_sql, where_params).fetchone()[0]

    # page of rows
    rows_sql = f"""
        WITH stats AS (
            SELECT product_id,
                   MIN(price_pounds) AS min_price,
                   COUNT(DISTINCT vendor_id) AS vendors_count
            FROM offers
            GROUP BY product_id
        )
        SELECT p.id, p.name, p.category, s.min_price, s.vendors_count
        FROM products p
        JOIN stats s ON s.product_id = p.id
        {where_sql}
        ORDER BY p.name COLLATE NOCASE ASC
        LIMIT ? OFFSET ?
    """
    rows = db.execute(rows_sql, (*where_params, limit, offset)).fetchall()

    items = [
        {
            "id": r["id"],
            "name": r["name"],
            "category": r["category"],
            "min_price": r["min_price"],
            "vendors_count": r["vendors_count"],
        }
        for r in rows
    ]

    return jsonify({"items": items, "total": total, "page": page, "limit": limit})

@app.get("/products/<int:pid>")
def product_detail(pid: int):
    """
    { id, name, category, vendors: [{ vendor, price, buy_url }] }
    Vendors sorted by ascending price.
    """
    db = get_db()
    pr = db.execute(
        "SELECT id, name, category FROM products WHERE id = ?", (pid,)
    ).fetchone()
    if not pr:
        return jsonify({"error": "not found"}), 404

    offers = db.execute(
        """
        SELECT v.name AS vendor, o.price_pounds AS price, o.url AS buy_url
        FROM offers o
        JOIN vendors v ON v.id = o.vendor_id
        WHERE o.product_id = ?
        ORDER BY o.price_pounds ASC, datetime(o.scraped_at) DESC, o.id DESC
        """,
        (pid,),
    ).fetchall()

    vendors = [
        {"vendor": r["vendor"], "price": r["price"], "buy_url": r["buy_url"]}
        for r in offers
    ]

    return jsonify({"id": pr["id"], "name": pr["name"], "category": pr["category"], "vendors": vendors})

@app.get("/categories")
def categories():
    """
    Distinct categories for products that actually have offers.
    """
    db = get_db()
    rows = db.execute(
        """
        SELECT DISTINCT p.category
        FROM products p
        WHERE p.id IN (SELECT DISTINCT product_id FROM offers)
          AND COALESCE(TRIM(p.category),'') != ''
        ORDER BY p.category COLLATE NOCASE ASC
        """
    ).fetchall()
    return jsonify([r["category"] for r in rows])

if __name__ == "__main__":
    print(f"DB_PATH = {DB_PATH}")
    app.run(host="127.0.0.1", port=5000, debug=True)
