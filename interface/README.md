# Web UI plan for PostgreSQLCrawler databases

This document outlines a small, standalone web interface (Flask-based) to browse crawl databases (SQLite or PostgreSQL) without modifying the main crawler.

## Goals
- Read-only browsing of existing tables/views.
- Simple HTML UI inspired by LibreCrawl.
- Works for both SQLite and PostgreSQL crawl DBs.
- Safe by default (SELECT-only, row limits, local bind).

## Architecture
- Entry script: `webui.py` (Flask).
- Data access: SQLAlchemy engine from `DB_URL`; optional `DB_SCHEMA` for Postgres.
- Static/templating: one HTML template with light JS/CSS (could live in `webui_templates` and `webui_static` or be inlined for simplicity).
- No coupling to crawler runtime; just point at a DB file/connection string.

## Configuration
- `DB_URL` (env or `--db-url`): e.g. `sqlite:////path/to/crawl.db` or `postgresql://user:pass@host:5432/dbname`.
- `DB_SCHEMA` (env or `--db-schema`): defaults to `public` for Postgres; ignored for SQLite.
- `HOST` / `PORT`: default `127.0.0.1:8008`.
- Optional `AUTH_TOKEN` for a shared-secret header.

## Safety rules
- Only allow statements starting with `SELECT`.
- Reject multiple statements (no semicolons).
- Auto-append `LIMIT 200` if none present.
- Short timeout (e.g., Postgres `statement_timeout = 5000` via connection options).
- Bind to localhost by default.

## Endpoints
- `GET /` — serve the UI page.
- `GET /api/meta` — list tables/views and backend type.
- `GET /api/view-sql?name=...` — return stored view definition.
- `POST /api/query` — run guarded SELECT, return `{columns, rows, rowcount, truncated}`.

## Metadata queries
- PostgreSQL (tables/views):
  `SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = :schema ORDER BY table_type, table_name;`
- SQLite (tables/views):
  `SELECT name, type FROM sqlite_master WHERE type IN ('view','table') ORDER BY type, name;`
- View definition:
  - Postgres: `SELECT pg_get_viewdef(:name::regclass, true);`
  - SQLite: `SELECT sql FROM sqlite_master WHERE type='view' AND name=:name;`

## UI sketch
- Left pane: list of tables/views; click to preload `SELECT * FROM <name> LIMIT 100;`
- Middle: view definition display.
- Right: query textarea + Run button; results rendered as HTML table (client-side pagination optional).
- Minimal styling; no heavy frontend framework needed.

## Dependencies
- Add to `requirements.txt`: `Flask`, `SQLAlchemy`, `psycopg2-binary` (Postgres). SQLite works without extras.

## Run command

**For PostgreSQL:**
```bash
python interface/webui.py --db-url postgresql://user:password@localhost:5432/crawler_db --db-schema public --host 127.0.0.1 --port 8008
```

**For SQLite:**
```bash
python interface/webui.py --db-url sqlite:////path/to/crawl.db --host 127.0.0.1 --port 8008
```

**Using environment variables:**
```bash
DB_URL=postgresql://user:password@localhost:5432/crawler_db \
DB_SCHEMA=public \
python interface/webui.py
```

**With optional authentication token:**
```bash
DB_URL=postgresql://user:password@localhost:5432/crawler_db \
AUTH_TOKEN=your-secret-token \
python interface/webui.py
```

Then open `http://127.0.0.1:8008` in your browser.

## Installation

Install dependencies:
```bash
pip install flask sqlalchemy psycopg2-binary
```

Or install all project dependencies:
```bash
pip install -r requirements.txt
```

## Status

✅ Implementation complete. The web UI is ready to use!

