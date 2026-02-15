import os
from pathlib import Path

from dotenv import load_dotenv
import psycopg

_CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"
load_dotenv(_CONFIG_DIR / ".env")

DB_NAME = os.getenv("PG_DB", "postgres")
ADMIN_DB = os.getenv("PG_ADMIN_DB", "postgres")
URL = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/postgres")
USER = os.getenv("PG_USER", "postgres")
PASSWORD = os.getenv("PG_PASSWORD", "postgres")


def _dsn(db):
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    return f"host={host} port={port} dbname={db} user={USER} password={PASSWORD}"


def ensure_database():
    with psycopg.connect(_dsn(ADMIN_DB), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DB_NAME,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(f'CREATE DATABASE "{DB_NAME}"')


def ensure_tables():
    with psycopg.connect(_dsn(DB_NAME)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.transformed_transactions (
                    id TEXT NOT NULL,
                    product TEXT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    currency TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    status TEXT NOT NULL,
                    date DATE,
                    month INTEGER
                );
                """
            )
        conn.commit()


def main():
    ensure_database()
    ensure_tables()


if __name__ == "__main__":
    main()
