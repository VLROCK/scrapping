import hashlib
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse
import zstandard as zstd
import json
import io
import re

DB_PATH = "data/dataset_lixo.db"
SCHEMA_PATH = "schema.sql"

os.makedirs("data", exist_ok=True)
conn = sqlite3.connect(DB_PATH)
with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
    conn.executescript(schema_file.read())
conn.commit()

cursor = conn.execute("SELECT content_hash FROM texts")
rows = cursor.fetchall()
seen_hashes = {row[0] for row in rows if row[0]}
print(f"[DB] {len(seen_hashes)} textos já existentes carregados.")

