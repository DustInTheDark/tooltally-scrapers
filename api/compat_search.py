#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import sqlite3
from typing import Any, Dict, List, Optional
from flask import Flask, jsonify, request, abort

# NEW: allow browser requests from localhost:3000
try:
    from flask_cors import CORS
except ImportError:
    CORS = None  # we'll handle if missing

DB_PATH = "data/tooltally.db"

app = Flask(__name__)
if CORS:
    CORS(app, resources={r"/search": {"origins": ["http://localhost:3000", "http://127.0.0.1:3000"]}})

def open_db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def find_best_product(cur: sqlite3.Cursor, q: str) -> Optional[sqlite3.Row]:
    q_like = f"%{q}%"
    cur.execute("""
        SELECT *
        FROM products
        WHERE (model IS NOT NULL AND model LIKE ?)
           OR name LIKE ?
           OR brand LIKE ?
        ORDER BY
          CASE
            WHEN model LIKE ? THEN 0
            WHEN name  LIKE ? THEN 1
            WHEN brand LIKE ? THEN 2
            ELSE 3
          END,
          id ASC
        LIMIT 1
    """, (q_like, q_like, q_like, q_like, q_like, q_like))
    return cur.fetchone()

def get_offers(cur: sqlite3.Cursor, product_id: int) -> List[Dict[str, Any]]:
    cur.execute("""
        SELECT o.price_pounds AS price,
               o.url          AS vendor_product_url,
               v.name         AS vendor_name
        FROM offers o
        JOIN vendors v ON v.id = o.vendor_id
        WHERE o.product_id = ?
        ORDER BY price ASC, o.id ASC
    """, (product_id,))
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

@app.get("/search")
def search():
    q = (request.args.get("query") or "").strip()
    if not q:
        abort(400, description="Missing query param ?query=")

    con = open_db()
    try:
        cur = con.cursor()
        p = find_best_product(cur, q)
        if not p:
            return jsonify({"product_info": {}, "offers": []})

        offers = get_offers(cur, p["id"])
        return jsonify({
            "product_info": {
                "title": p["name"] or "",
                "brand": (p["brand"] or "").title() if p["brand"] else "",
                "description": "",
                "image_url": None,
            },
            "offers": offers,
        })
    finally:
        con.close()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
