# ============================================================
# config_db.py – PostgreSQL connection config
# ============================================================
"""
Đọc thông tin kết nối PostgreSQL từ .env (hoặc environment variables).

Usage:
    from db.config_db import get_engine, get_conn_str, DSN

Tạo .env từ template:
    cp .env.example .env
    # Điền POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB,...
"""

from __future__ import annotations

import os
from pathlib import Path

# Load .env nếu có
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_file)

# ────────────────────────────────────────────────────────────
# Connection parameters
# ────────────────────────────────────────────────────────────
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")

if POSTGRES_HOST.startswith("postgres://") or POSTGRES_HOST.startswith("postgresql://"):
    DSN: str = POSTGRES_HOST
    SQLALCHEMY_URL: str = POSTGRES_HOST.replace("postgres://", "postgresql://", 1)
    
    import urllib.parse
    parsed = urllib.parse.urlparse(POSTGRES_HOST)
    DB_HOST = parsed.hostname
    DB_PORT = parsed.port
    DB_NAME = parsed.path.lstrip('/')
    DB_USER = parsed.username
    DB_PASSWORD = parsed.password
else:
    DB_HOST: str = POSTGRES_HOST
    DB_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    DB_NAME: str = os.getenv("POSTGRES_DB", "vertex_football")
    DB_USER: str = os.getenv("POSTGRES_USER", "postgres")
    DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "postgres")

    # psycopg2 DSN string
    DSN: str = (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD}"
    )

    # SQLAlchemy connection URL
    SQLALCHEMY_URL: str = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def get_engine():
    """Trả về SQLAlchemy Engine (lazy creation)."""
    from sqlalchemy import create_engine
    return create_engine(SQLALCHEMY_URL, pool_pre_ping=True)


def get_connection():
    """Trả về psycopg2 raw connection."""
    import psycopg2
    return psycopg2.connect(DSN)


def test_connection() -> bool:
    """Kiểm tra kết nối PostgreSQL. Trả về True nếu OK."""
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            ver = cur.fetchone()[0]
        conn.close()
        print(f"✓ PostgreSQL OK: {ver[:60]}...")
        return True
    except Exception as exc:
        print(f"✗ Kết nối thất bại: {exc}")
        print(f"  DSN: host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER}")
        return False
