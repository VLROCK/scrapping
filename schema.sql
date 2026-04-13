CREATE TABLE IF NOT EXISTS texts (
    text_id        TEXT PRIMARY KEY,
    content        TEXT NOT NULL,
    label          INTEGER NOT NULL DEFAULT 0,
    broad_area     TEXT,
    specific_theme TEXT,
    char_count     INTEGER,
    word_count     INTEGER,
    size_category  TEXT,
    creation_date  TEXT,
    source_url     TEXT,
    source_name    TEXT,
    content_hash   TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS texts_ia (
    text_id        TEXT PRIMARY KEY,
    content        TEXT NOT NULL,
    label          INTEGER NOT NULL DEFAULT 1,
    broad_area     TEXT,
    specific_theme TEXT,
    char_count     INTEGER,
    word_count     INTEGER,
    size_category  TEXT,
    source_model   TEXT,
    style          TEXT,
    context_year   TEXT,
    temperature    REAL,
    content_hash   TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS visited_urls (
    url_hash TEXT PRIMARY KEY,
    visited_at TEXT NOT NULL
);