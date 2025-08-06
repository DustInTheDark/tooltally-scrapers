"""Lightweight API for serving scraped product data.

This Flask application exposes read-only endpoints backed by the
``tooltally`` SQLite database.  The previous implementation relied on
``engine.execute`` which was removed in SQLAlchemy 2.0.  The routes now
use explicit connections via ``engine.connect()`` to remain compatible
with modern versions of SQLAlchemy.
"""

from __future__ import annotations

import decimal
import os
from typing import Any, Dict, List

from flask import Flask, jsonify, request
from sqlalchemy import create_engine, text

app = Flask(__name__)

# Obtain the database URI from the environment with a sensible default.
DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI", "sqlite:///data/tooltally.db")

# ``future=True`` enables SQLAlchemy 2.0 style usage and ensures ``Engine``
# provides ``connect`` for context managed execution.
engine = create_engine(DATABASE_URI, future=True)


@app.route("/")
def home() -> str:
    """Basic health-check endpoint."""
    return "Backend is running!"


@app.route("/products")
def list_products():  # pragma: no cover - simple query wrapper
    """Return a list of products with optional filtering."""

    search = request.args.get("search")
    category = request.args.get("category")

    filters: List[str] = []
    params: Dict[str, Any] = {}

    if search:
        filters.append("LOWER(p.name) LIKE :search")
        params["search"] = f"%{search.lower()}%"

    if category:
        # Match categories case-insensitively so UI filters don't need exact casing
        filters.append("LOWER(p.category) = :category")
        params["category"] = category.lower()

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    base_query = f"""
        SELECT p.id, p.name, p.product_code, p.price, p.url, p.category,
               v.name AS vendor_name
        FROM products p
        JOIN vendors v ON p.vendor_id = v.id
        {where_clause}
        ORDER BY p.name
    """

    with engine.connect() as conn:
        result = conn.execute(text(base_query), params).mappings().all()

    rows: List[Dict[str, Any]] = [dict(r) for r in result]

    # ``decimal.Decimal`` instances are not JSON serialisable by default.
    for row in rows:
        if isinstance(row.get("price"), decimal.Decimal):
            row["price"] = float(row["price"])

    return jsonify(rows)


@app.route("/products/<int:product_id>")
def get_product(product_id: int):  # pragma: no cover - simple query wrapper
    """Return details for a single product."""

    query = text(
        """
        SELECT p.id, p.name, p.product_code, p.price, p.url, p.category,
               v.name AS vendor_name
        FROM products p
        JOIN vendors v ON p.vendor_id = v.id
        WHERE p.id = :pid
        """
    )

    with engine.connect() as conn:
        result = conn.execute(query, {"pid": product_id}).mappings().first()

    if not result:
        return jsonify({"error": "Product not found"}), 404

    row = dict(result)

    if isinstance(row.get("price"), decimal.Decimal):
        row["price"] = float(row["price"])

    return jsonify(row)


@app.route("/categories")
def list_categories():  # pragma: no cover - simple query wrapper
    """Return a sorted list of distinct product categories."""

    query = text(
        """
        SELECT DISTINCT category
        FROM products
        WHERE category IS NOT NULL AND category != ''
        ORDER BY category
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(query).scalars().all()

    return jsonify(rows)


if __name__ == "__main__":  # pragma: no cover
    app.run(host="0.0.0.0", port=5000)