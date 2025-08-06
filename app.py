from flask import Flask, request, jsonify
from sqlalchemy import create_engine, text
import os

app = Flask(__name__)

# Determine database location; default to local SQLite file
DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI", "sqlite:///data/tooltally.db")
engine = create_engine(DATABASE_URI)


@app.route("/")
def home():
    """Simple health check route."""
    return "Backend is running!"


@app.get("/products")
def list_products():
    """Return a list of products with optional search and category filters."""
    search = request.args.get("search")
    category = request.args.get("category")

    base_query = """
        SELECT p.id, p.name, p.category, MIN(vp.price) AS min_price
        FROM products p
        LEFT JOIN vendor_products vp ON p.id = vp.product_id
    """
    conditions = []
    params = {}

    if search:
        conditions.append("(p.name LIKE :search OR p.brand LIKE :search)")
        params["search"] = f"%{search}%"
    if category:
        conditions.append("p.category = :category")
        params["category"] = category
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)

    base_query += " GROUP BY p.id ORDER BY p.name"
    rows = engine.execute(text(base_query), params).mappings().all()
    return jsonify(rows)


@app.get("/products/<int:product_id>")
def product_detail(product_id: int):
    """Return product details and vendor offers for a given product."""
    product = engine.execute(
        text("SELECT id, name, category FROM products WHERE id = :id"),
        {"id": product_id},
    ).mappings().first()
    if not product:
        return jsonify({"error": "Product not found"}), 404

    vendor_rows = engine.execute(
        text(
            """
            SELECT v.name AS vendor, vp.price, vp.buy_url
            FROM vendor_products vp
            JOIN vendors v ON v.id = vp.vendor_id
            WHERE vp.product_id = :id
            ORDER BY vp.price
            """
        ),
        {"id": product_id},
    ).mappings().all()

    product_dict = dict(product)
    product_dict["vendors"] = vendor_rows
    return jsonify(product_dict)


@app.get("/categories")
def list_categories():
    """Return all distinct product categories."""
    rows = engine.execute(
        text("SELECT DISTINCT category FROM products WHERE category IS NOT NULL ORDER BY category")
    ).fetchall()
    categories = [row[0] for row in rows]
    return jsonify(categories)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)