from __future__ import annotations

import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    def load_dotenv() -> None:
        """Fallback no-op if python-dotenv is not installed."""
        pass
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Numeric,
    String,
    func,
    create_engine,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import uuid

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

Base = declarative_base()

class Vendor(Base):
    __tablename__ = "vendors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    domain = Column(String, nullable=False)
    logo_url = Column(String)

    products = relationship("Product", back_populates="vendor")

class Product(Base):
    __tablename__ = "products"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String, nullable=False)
    slug = Column(String, unique=True, nullable=False)
    vendor_id = Column(UUID(as_uuid=True), ForeignKey("vendors.id"), nullable=False)
    price = Column(Numeric(10, 2), nullable=False)
    url = Column(String, nullable=False)
    category = Column(String)
    scrape_source = Column(String, nullable=False)
    last_updated = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    vendor = relationship("Vendor", back_populates="products")


def init_db() -> None:
    """Create tables in the database if they do not already exist."""
    Base.metadata.create_all(bind=engine)

