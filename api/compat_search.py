#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compat API for ToolTally — with /categories (deduped) and category filtering.
- /categories: list distinct categories with counts (normalized to avoid dupes)
- /products: search + pagination (+ optional ?category=), lowest_price ignores 0/NULL and rows without URL
- /product/<pid>: detail + offers (best row per vendor), TEXT-safe ids

Run:
  pip install flask flask-cors
  py api\\compat_search.py
"""

from __future__ import annotations
import re
import sqlite3
from typing import Any, Dict, List, Optional, Tuple
from flask import Flask, jsonify, request, abort

try:
    from flask_cors import CORS
except ImportError:
    CORS = None

DB_PATH = "data/tooltally.db"

app = Flask(__name__)
if CORS:
    CORS(app, resources={
        r"/categories": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]},
        r"/products*":  {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]},
        r"/product/*":  {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]},
        r"/search":     {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]},
    })

# ---------------- DB ----------------
def open_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

# ---------------- helpers ----------------
ALNUM_UPPER = re.compile(r'[^A-Z0-9]+')
MODEL_CODE  = re.compile(r'\b([A-Z]{2,5}\d{2,4}[A-Z0-9-]*)\b', re.I)

def norm_alnum_upper(s: str) -> str:
    return ALNUM_UPPER.sub('', (s or '').upper())

def normalize_query(q: str) -> str:
    t = q or ""
    t = re.sub(r'20\s*v\s*max', '18v', t, flags=re.I)
    t = re.sub(r'10\.8\s*v', '12v', t, flags=re.I)
    t = re.sub(r'\b20(?=[A-Za-z]{2,}\d)', '', t)
    return t.strip()

def extract_model_from_query(q: str) -> Optional[str]:
    m = MODEL_CODE.search(q or '')
    if not m:
        return None
    code = m.group(1).upper().replace(' ', '')
    base = re.sub(r'(Z|N|NT|J|TJ|RTJ|RJ|RFJ|RMJ|PS|P1|P2)$', '', code)
    return base

def build_search_parts(q_raw: str) -> Tuple[str, List[Any], Optional[str], Optional[int], Tuple[str, List[Any]]]:
    q = normalize_query(q_raw)
    params: List[Any] = []
    clauses: List[str] = []

    model = extract_model_from_query(q)
    if model:
        like_model = f"%{model}%"
        clauses.append("(p.model LIKE ? OR p.name LIKE ?)")
        params.extend([like_model, like_model])

    q_norm = norm_alnum_upper(q)
    norm_clause = "(REPLACE(REPLACE(UPPER(p.name),' ','') ,'-','') LIKE ? OR REPLACE(REPLACE(UPPER(COALESCE(p.model,'')),' ','') ,'-','') LIKE ?)"
    params.extend([f"%{q_norm}%", f"%{q_norm}%"])
    clauses.append(norm_clause)

    tokens = [t for t in re.split(r'\s+', q) if t]
    token_score_terms = []
    for t in tokens:
        t_like_escaped = t.replace("'", "''")
        token_score_terms.append(
            f"(p.name LIKE '%{t_like_escaped}%' OR p.brand LIKE '%{t_like_escaped}%' OR p.model LIKE '%{t_like_escaped}%')"
        )
    score_expr = None
    score_threshold = None
    if token_score_terms:
        score_expr = " + ".join(f"CASE WHEN {s} THEN 1 ELSE 0 END" for s in token_score_terms)
        score_threshold = max(1, (len(tokens) + 1) // 2)

    primary_where = "WHERE " + " OR ".join(clauses) if clauses else ""

    fb_params: List[Any] = []
    or_pieces: List[str] = []
    for t in tokens:
        like = f"%{t}%"
        or_pieces.append("(p.name LIKE ? OR p.brand LIKE ? OR p.model LIKE ?)")
        fb_params.extend([like, like, like])
    fallback_where = "WHERE " + (" OR ".join(or_pieces) if or_pieces else "1=1")

    return primary_where, params, score_expr, score_threshold, (fallback_where, fb_params)

# ---------------- offers helper ----------------
def get_offers(cur: sqlite3.Cursor, product_id_text: str) -> List[Dict[str, Any]]:
    cur.execute("""
        WITH ranked AS (
          SELECT
            o.vendor_id,
            v.name AS vendor_name,
            o.price_pounds AS price,
            o.url AS vendor_product_url,
            ROW_NUMBER() OVER (
              PARTITION BY o.vendor_id
              ORDER BY
                CASE WHEN (o.url IS NULL OR o.url='') THEN 1 ELSE 0 END ASC,
                CASE WHEN (o.price_pounds IS NULL OR o.price_pounds=0) THEN 1 ELSE 0 END ASC,
                o.price_pounds ASC,
                o.id ASC
            ) AS rn
          FROM offers o
          JOIN vendors v ON v.id = o.vendor_id
          WHERE CAST(o.product_id AS TEXT) = ?
        )
        SELECT vendor_name, price, vendor_product_url
        FROM ranked
        WHERE rn = 1
        ORDER BY
          CASE WHEN (price IS NULL OR price=0) THEN 1 ELSE 0 END ASC,
          price ASC
    """, (str(product_id_text),))
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        price = float(r["price"]) if r["price"] is not None else None
        out.append({
            "vendor_name": r["vendor_name"],
            "price": price,
            "original_price": None,
            "availability": "unknown",
            "vendor_product_url": r["vendor_product_url"],
            "delivery_info": None,
        })
    return out

# ---------------- CATEGORIES (deduped) ----------------
@app.get("/categories")
def categories():
    con = open_db()
    try:
        cur = con.cursor()
        rows = cur.execute("""
            WITH norm AS (
              SELECT
                CASE
                  WHEN TRIM(COALESCE(category,'')) = '' THEN 'Uncategorized'
                  ELSE TRIM(category)
                END AS name_trim
              FROM products
            ),
            pretty AS (
              SELECT
                UPPER(SUBSTR(name_trim,1,1)) || LOWER(SUBSTR(name_trim,2)) AS name
              FROM norm
            )
            SELECT name, COUNT(*) AS count
            FROM pretty
            GROUP BY name
            ORDER BY name ASC
        """).fetchall()

        def slugify(s: str) -> str:
            s = s.strip().lower()
            s = re.sub(r'[^a-z0-9]+', '-', s)
            s = re.sub(r'-{2,}', '-', s).strip('-')
            return s or 'uncategorized'

        by_slug: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            name = r["name"]
            count = int(r["count"] or 0)
            slug = slugify(name)
            if slug in by_slug:
                by_slug[slug]["count"] += count
            else:
                by_slug[slug] = {"name": name, "slug": slug, "count": count}

        items = sorted(by_slug.values(), key=lambda x: x["name"])
        return jsonify({"items": items})
    finally:
        con.close()

# ---------------- list endpoint (with optional category) ----------------
@app.get("/products")
def products():
    q_raw = (request.args.get("search") or "").strip()
    category_raw = (request.args.get("category") or "").strip()
    page  = max(int(request.args.get("page") or 1), 1)
    limit = max(min(int(request.args.get("limit") or 24), 100), 1)
    offset = (page - 1) * limit

    con = open_db()
    try:
        cur = con.cursor()

        where_sql = ""
        where_params: List[Any] = []
        use_score_expr = None
        use_threshold = None

        if category_raw:
            where_sql += ("WHERE " if not where_sql else " AND ") + """
              CASE
                WHEN TRIM(COALESCE(p.category,''))='' THEN 'Uncategorized'
                ELSE UPPER(SUBSTR(TRIM(p.category),1,1)) || LOWER(SUBSTR(TRIM(p.category),2))
              END = ?
            """
            where_params.append(category_raw.strip())

        if q_raw:
            primary_where, primary_params, score_expr, score_threshold, fallback = build_search_parts(q_raw)

            if where_sql:
                primary_where = primary_where.replace("WHERE ", "", 1)
                primary_where = "WHERE " + where_sql.replace("WHERE ", "") + " AND (" + primary_where + ")"
                primary_params = where_params + primary_params

            cur.execute(f"SELECT COUNT(*) FROM products p {primary_where}", primary_params)
            total = int(cur.fetchone()[0])

            use_where, use_params = primary_where, primary_params
            use_score_expr, use_threshold = score_expr, score_threshold

            if total == 0 and fallback:
                fb_where, fb_params = fallback
                if where_sql:
                    fb_where = fb_where.replace("WHERE ", "", 1)
                    fb_where = "WHERE " + where_sql.replace("WHERE ", "") + " AND (" + fb_where + ")"
                    fb_params = where_params + fb_params
                cur.execute(f"SELECT COUNT(*) FROM products p {fb_where}", fb_params)
                total = int(cur.fetchone()[0])
                use_where, use_params = fb_where, fb_params
                use_score_expr, use_threshold = None, None
        else:
            if where_sql:
                cur.execute(f"SELECT COUNT(*) FROM products p {where_sql}", where_params)
                total = int(cur.fetchone()[0])
                use_where, use_params = where_sql, where_params
            else:
                total = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
                use_where, use_params = "", []
            use_score_expr, use_threshold = None, None

        having_clause = f"HAVING ({use_score_expr}) >= {use_threshold}" if (use_score_expr and use_threshold is not None) else ""
        list_sql = f"""
            WITH o1 AS (
              SELECT
                CAST(o.product_id AS TEXT) AS pid,
                o.vendor_id,
                o.price_pounds,
                o.url,
                ROW_NUMBER() OVER (
                  PARTITION BY CAST(o.product_id AS TEXT), o.vendor_id
                  ORDER BY
                    CASE WHEN (o.url IS NULL OR o.url='') THEN 1 ELSE 0 END ASC,
                    CASE WHEN (o.price_pounds IS NULL OR o.price_pounds=0) THEN 1 ELSE 0 END ASC,
                    o.price_pounds ASC,
                    o.id ASC
                ) AS rn
              FROM offers o
            )
            SELECT
              CAST(p.id AS TEXT) AS id,
              p.name AS title,
              p.brand,
              p.image_url AS image_url,  -- << return real image
              COALESCE(MIN(NULLIF(CASE WHEN (o1.url IS NULL OR o1.url='') THEN NULL ELSE o1.price_pounds END,0)), NULL) AS lowest_price,
              COUNT(DISTINCT CASE WHEN (o1.url IS NOT NULL AND o1.url<>'') THEN o1.vendor_id END) AS vendor_count
            FROM products p
            LEFT JOIN o1 ON (o1.pid = CAST(p.id AS TEXT) AND o1.rn = 1)
            {use_where}
            GROUP BY p.id
            {having_clause}
            ORDER BY vendor_count DESC, lowest_price ASC, p.id ASC
            LIMIT ? OFFSET ?
        """
        cur.execute(list_sql, use_params + [limit, offset])

        items: List[Dict[str, Any]] = []
        for row in cur.fetchall():
            items.append({
                "id": row["id"],
                "title": row["title"],
                "brand": (row["brand"] or "").title() if row["brand"] else None,
                "image_url": row["image_url"],  # << use it
                "lowest_price": float(row["lowest_price"]) if row["lowest_price"] is not None else None,
                "vendor_count": int(row["vendor_count"] or 0),
            })

        return jsonify({"items": items, "total": int(total), "page": page, "limit": limit})
    finally:
        con.close()

# ---------------- product detail endpoint ----------------
@app.get("/product/<pid>")
def product_detail(pid: str):
    con = open_db()
    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM products WHERE CAST(id AS TEXT) = ?", (str(pid),))
        p = cur.fetchone()
        if not p:
            abort(404)
        offers = get_offers(cur, str(pid))
        return jsonify({
            "product_info": {
                "id": str(p["id"]),
                "title": p["name"] or "",
                "brand": (p["brand"] or "").title() if p["brand"] else "",
                "description": "",
                "image_url": p["image_url"],  # << return actual image for detail too
            },
            "offers": offers,
        })
    finally:
        con.close()

# ---------------- single-product convenience ----------------
@app.get("/search")
def search():
    q_raw = (request.args.get("query") or "").strip()
    if not q_raw:
        return jsonify({"product_info": {}, "offers": []})

    q = normalize_query(q_raw)
    con = open_db()
    try:
        cur = con.cursor()
        model = extract_model_from_query(q)
        if model:
            like = f"%{model}%"
            cur.execute("""
                SELECT * FROM products p
                WHERE p.model LIKE ? OR p.name LIKE ?
                ORDER BY p.id ASC
                LIMIT 1
            """, (like, like))
            p = cur.fetchone()
        else:
            primary_where, primary_params, score_expr, score_threshold, fallback = build_search_parts(q)
            select_sql = f"SELECT * FROM products p {primary_where}"
            if score_expr and score_threshold is not None:
                select_sql += f" GROUP BY p.id HAVING ({score_expr}) >= {score_threshold}"
            select_sql += " ORDER BY p.id ASC LIMIT 1"
            cur.execute(select_sql, primary_params)
            p = cur.fetchone()
            if not p and fallback:
                fb_where, fb_params = fallback
                cur.execute(f"SELECT * FROM products p {fb_where} ORDER BY p.id ASC LIMIT 1", fb_params)
                p = cur.fetchone()

        if not p:
            return jsonify({"product_info": {}, "offers": []})

        pid_text = str(p["id"])
        offers = get_offers(cur, pid_text)
        return jsonify({
            "product_info": {
                "id": pid_text,
                "title": p["name"] or "",
                "brand": (p["brand"] or "").title() if p["brand"] else "",
                "description": "",
                "image_url": p["image_url"],  # << include here as well
            },
            "offers": offers,
        })
    finally:
        con.close()

if __name__ == "__main__":
    print(f"DB_PATH = {DB_PATH}")
    app.run(host="127.0.0.1", port=5000, debug=True)
