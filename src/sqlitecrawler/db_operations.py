"""
Database operations using the abstraction layer.

This module provides database operations that work with both SQLite and PostgreSQL
through the database abstraction layer.
"""

from __future__ import annotations
import json
import os
import time
import asyncio
import random
from typing import Optional, Iterable, Tuple, List, Dict, Any
import datetime
from .database import DatabaseConnection, DatabaseConfig, create_connection, set_global_config
from .postgresql_schema import get_postgres_pages_schema_statements, get_postgres_crawl_schema_statements, get_postgres_schema_statements
from .database_views import get_sqlite_views, get_postgres_views
from .config import get_database_config


# SQLite schema definitions (for backward compatibility)
SQLITE_PAGES_SCHEMA = """
CREATE TABLE IF NOT EXISTS pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  headers_json TEXT,
  html_compressed BLOB,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  UNIQUE(url_id)
);
CREATE INDEX IF NOT EXISTS idx_pages_url_id ON pages(url_id);
"""

SQLITE_CRAWL_SCHEMA = """
CREATE TABLE IF NOT EXISTS urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url TEXT UNIQUE NOT NULL,
  kind TEXT CHECK (kind IN ('html','sitemap','sitemap_index','image','asset','other')),
  classification TEXT CHECK (classification IN ('internal','subdomain','network','external','social')),
  discovered_from_id INTEGER,
  first_seen INTEGER,
  last_seen INTEGER,
  headers_compressed BLOB,
  FOREIGN KEY (discovered_from_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS content (
  url_id INTEGER PRIMARY KEY,
  title TEXT,
  meta_description_id INTEGER,
  h1_tags TEXT,
  h2_tags TEXT,
  word_count INTEGER,
  html_lang_id INTEGER,
  internal_links_count INTEGER DEFAULT 0,
  external_links_count INTEGER DEFAULT 0,
  internal_links_unique_count INTEGER DEFAULT 0,
  external_links_unique_count INTEGER DEFAULT 0,
  crawl_depth INTEGER DEFAULT 0,
  inlinks_count INTEGER DEFAULT 0,
  inlinks_unique_count INTEGER DEFAULT 0,
  content_hash_sha256 TEXT,
  content_hash_simhash TEXT,
  content_length INTEGER,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (meta_description_id) REFERENCES meta_descriptions (id),
  FOREIGN KEY (html_lang_id) REFERENCES html_languages (id)
);

CREATE TABLE IF NOT EXISTS anchor_texts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  text TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS fragments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  fragment TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS html_languages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  language_code TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS meta_descriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  description TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS xpaths (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  xpath TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS internal_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_url_id INTEGER NOT NULL,
  target_url_id INTEGER,
  anchor_text_id INTEGER,
  xpath_id INTEGER,
  href_url_id INTEGER NOT NULL,
  fragment_id INTEGER,
  url_parameters TEXT,
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (source_url_id) REFERENCES urls (id),
  FOREIGN KEY (target_url_id) REFERENCES urls (id),
  FOREIGN KEY (anchor_text_id) REFERENCES anchor_texts (id),
  FOREIGN KEY (xpath_id) REFERENCES xpaths (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id),
  FOREIGN KEY (fragment_id) REFERENCES fragments (id)
);

CREATE TABLE IF NOT EXISTS robots_directive_strings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  directive TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS robots_directives (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  source TEXT CHECK (source IN ('robots_txt', 'html_meta', 'http_header')) NOT NULL,
  directive_id INTEGER NOT NULL,
  value TEXT,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (directive_id) REFERENCES robots_directive_strings (id)
);

CREATE TABLE IF NOT EXISTS canonical_urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  canonical_url_id INTEGER NOT NULL,
  source TEXT CHECK (source IN ('html_head', 'http_header')) NOT NULL,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (canonical_url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS hreflang_languages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  language_code TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS hreflang_sitemap (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  hreflang_id INTEGER NOT NULL,
  href_url_id INTEGER NOT NULL,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS hreflang_http_header (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  hreflang_id INTEGER NOT NULL,
  href_url_id INTEGER NOT NULL,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS hreflang_html_head (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  hreflang_id INTEGER NOT NULL,
  href_url_id INTEGER NOT NULL,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (hreflang_id) REFERENCES hreflang_languages (id),
  FOREIGN KEY (href_url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS page_metadata (
  url_id INTEGER PRIMARY KEY,
  initial_status_code INTEGER,
  final_status_code INTEGER,
  final_url_id INTEGER,
  redirect_destination_url_id INTEGER,
  fetched_at INTEGER,
  etag TEXT,
  last_modified TEXT,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (final_url_id) REFERENCES urls (id),
  FOREIGN KEY (redirect_destination_url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS indexability (
  url_id INTEGER PRIMARY KEY,
  robots_txt_allows BOOLEAN,
  html_meta_allows BOOLEAN,
  http_header_allows BOOLEAN,
  overall_indexable BOOLEAN,
  robots_txt_directives TEXT,
  html_meta_directives TEXT,
  http_header_directives TEXT,
  FOREIGN KEY (url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS redirects (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_url_id INTEGER NOT NULL,
  target_url_id INTEGER NOT NULL,
  redirect_chain TEXT NOT NULL,
  chain_length INTEGER NOT NULL,
  final_status INTEGER NOT NULL,
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (source_url_id) REFERENCES urls (id),
  FOREIGN KEY (target_url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS frontier (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  depth INTEGER NOT NULL,
  parent_id INTEGER,
  status TEXT NOT NULL CHECK (status IN ('queued','pending','done')),
  enqueued_at INTEGER,
  updated_at INTEGER,
  priority_score REAL DEFAULT 0.0,
  sitemap_priority REAL DEFAULT 0.5,
  inlinks_count INTEGER DEFAULT 0,
  content_type_score REAL DEFAULT 1.0,
  reset_count INTEGER DEFAULT 0,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (parent_id) REFERENCES urls (id),
  UNIQUE(url_id)
);

CREATE TABLE IF NOT EXISTS sitemaps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sitemap_url TEXT UNIQUE NOT NULL,
  discovered_at INTEGER DEFAULT (strftime('%s', 'now')),
  last_crawled_at INTEGER,
  total_urls_found INTEGER DEFAULT 0,
  is_sitemap_index BOOLEAN DEFAULT FALSE,
  parent_sitemap_id INTEGER,
  FOREIGN KEY (parent_sitemap_id) REFERENCES sitemaps (id)
);

CREATE TABLE IF NOT EXISTS url_sitemaps (
  url_id INTEGER NOT NULL,
  sitemap_id INTEGER NOT NULL,
  position INTEGER,
  discovered_at INTEGER DEFAULT (strftime('%s', 'now')),
  PRIMARY KEY (url_id, sitemap_id),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (sitemap_id) REFERENCES sitemaps (id)
);

CREATE TABLE IF NOT EXISTS failed_urls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  status_code INTEGER NOT NULL,
  failure_reason TEXT,
  retry_count INTEGER DEFAULT 0,
  next_retry_at INTEGER,
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  updated_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id)
);

CREATE TABLE IF NOT EXISTS schema_types (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_instances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  content_hash TEXT UNIQUE NOT NULL,
  schema_type_id INTEGER NOT NULL,
  format TEXT CHECK (format IN ('json-ld', 'microdata', 'rdfa')) NOT NULL,
  raw_data TEXT NOT NULL,
  parsed_data TEXT,
  is_valid BOOLEAN NOT NULL DEFAULT TRUE,
  validation_errors TEXT,
  severity TEXT CHECK (severity IN ('info', 'warning', 'error', 'critical')) DEFAULT 'info',
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (schema_type_id) REFERENCES schema_types (id)
);

CREATE TABLE IF NOT EXISTS page_schema_references (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  schema_instance_id INTEGER NOT NULL,
  position INTEGER,  -- Position on page (for multiple instances of same type)
  property_name TEXT,  -- For nested properties (e.g., 'image', 'breadcrumb', 'video')
  is_main_entity BOOLEAN DEFAULT FALSE,  -- Whether this is the main entity for the page
  parent_entity_id INTEGER,  -- Reference to parent entity if this is a nested property
  discovered_at INTEGER NOT NULL,
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (schema_instance_id) REFERENCES schema_instances (id),
  FOREIGN KEY (parent_entity_id) REFERENCES page_schema_references (id)
);

CREATE TABLE IF NOT EXISTS schema_data (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  url_id INTEGER NOT NULL,
  schema_type_id INTEGER NOT NULL,
  format TEXT CHECK (format IN ('json-ld', 'microdata', 'rdfa')) NOT NULL,
  raw_data TEXT NOT NULL,
  parsed_data TEXT,
  position INTEGER,
  is_valid BOOLEAN NOT NULL DEFAULT TRUE,
  validation_errors TEXT,
  severity TEXT CHECK (severity IN ('info', 'warning', 'error', 'critical')) DEFAULT 'info',
  created_at INTEGER DEFAULT (strftime('%s', 'now')),
  FOREIGN KEY (url_id) REFERENCES urls (id),
  FOREIGN KEY (schema_type_id) REFERENCES schema_types (id)
);
"""


async def init_pages_db(config: DatabaseConfig, pages_db_path: str = None):
    """Initialize the pages database with the appropriate schema."""
    if config.backend == "postgresql":
        # For PostgreSQL, we use the same database but different schema
        # The schema is created as part of the crawl database initialization
        pass
    else:
        # For SQLite, create the pages database
        if pages_db_path:
            # Create a separate config for the pages database
            pages_config = DatabaseConfig(
                backend="sqlite",
                sqlite_path=pages_db_path
            )
            set_global_config(pages_config)
            
            async with create_connection() as conn:
                # Apply SQLite optimizations
                if hasattr(conn, '_optimize_connection'):
                    await conn._optimize_connection()
                
                # Execute pages schema
                for stmt in SQLITE_PAGES_SCHEMA.split(";\n"):
                    if stmt.strip():
                        await conn.execute(stmt)
                await conn.commit()
            
            # Restore the original config
            set_global_config(config)


async def init_crawl_db(config: DatabaseConfig, crawl_db_path: str = None):
    """Initialize the crawl database with the appropriate schema."""
    if config.backend == "postgresql":
        # For PostgreSQL, create the database if it doesn't exist
        import asyncpg
        
        # First, connect to the default 'postgres' database to create our target database
        try:
            admin_conn = await asyncpg.connect(
                host=config.postgres_host,
                port=config.postgres_port,
                database="postgres",  # Connect to default database
                user=config.postgres_user,
                password=config.postgres_password
            )
            
            # Create the database if it doesn't exist
            await admin_conn.execute(f"CREATE DATABASE {config.postgres_database}")
            await admin_conn.close()
        except asyncpg.exceptions.DuplicateDatabaseError:
            # Database already exists, that's fine
            pass
        except Exception as e:
            print(f"Warning: Could not create database {config.postgres_database}: {e}")
        
        # Now connect to our target database and create the schema
        async with create_connection() as conn:
            # Set the search path to public schema
            await conn.execute("SET search_path TO public")
            
            # Execute PostgreSQL schema statements (both pages and crawl schemas)
            # Note: Schema uses CREATE TABLE IF NOT EXISTS, so existing tables and data are preserved
            statements = get_postgres_schema_statements()
            for statement in statements:
                await conn.execute(statement)

            # Ensure redirects has unique index for ON CONFLICT and indexability has directive columns
            try:
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_redirects_source_unique ON redirects(source_url_id)")
            except Exception as e:
                print(f"Warning: Could not ensure unique index on redirects(source_url_id): {e}")
            # Add directive columns to indexability if missing
            try:
                await conn.execute("ALTER TABLE indexability ADD COLUMN IF NOT EXISTS robots_txt_directives TEXT")
                await conn.execute("ALTER TABLE indexability ADD COLUMN IF NOT EXISTS html_meta_directives TEXT")
                await conn.execute("ALTER TABLE indexability ADD COLUMN IF NOT EXISTS http_header_directives TEXT")
            except Exception as e:
                print(f"Warning: Could not migrate indexability directives columns: {e}")
            
            # Ensure redirects has a unique index on source_url_id for ON CONFLICT to work
            try:
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_redirects_source_unique ON redirects(source_url_id)")
            except Exception as e:
                print(f"Warning: Could not ensure unique index on redirects(source_url_id): {e}")
            
            # Migrate existing frontier table to add 'pending' status if needed
            try:
                # Find all CHECK constraints on the frontier table that might be for status
                constraints = await conn.fetchall("""
                    SELECT tc.constraint_name 
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.constraint_column_usage ccu 
                        ON tc.constraint_name = ccu.constraint_name
                    WHERE tc.table_name = 'frontier' 
                    AND tc.constraint_type = 'CHECK'
                    AND ccu.column_name = 'status'
                """)
                
                # Drop all existing status CHECK constraints
                dropped_any = False
                for constraint_row in constraints:
                    constraint_name = constraint_row[0]
                    try:
                        await conn.execute(f"ALTER TABLE frontier DROP CONSTRAINT IF EXISTS {constraint_name}")
                        dropped_any = True
                    except Exception:
                        pass
                
                # Add the new constraint with 'pending' status
                await conn.execute("""
                    ALTER TABLE frontier 
                    ADD CONSTRAINT frontier_status_check 
                    CHECK (status IN ('queued','pending','done'))
                """)
                if dropped_any:
                    print("  -> Updated frontier table to support 'pending' status")
            except Exception as e:
                # If migration fails, try a simpler approach - just add the constraint
                # (it will fail if it already exists, which is fine)
                try:
                    await conn.execute("""
                        ALTER TABLE frontier 
                        ADD CONSTRAINT frontier_status_check 
                        CHECK (status IN ('queued','pending','done'))
                    """)
                except Exception:
                    # Constraint might already exist with correct values, that's fine
                    pass
            
            # Add reset_count column if it doesn't exist
            try:
                await conn.execute("ALTER TABLE frontier ADD COLUMN IF NOT EXISTS reset_count INTEGER DEFAULT 0")
            except Exception as e:
                print(f"Warning: Could not add reset_count column: {e}")
            
            # Create database views
            view_statements = get_postgres_views()
            for view_stmt in view_statements:
                if view_stmt:
                    try:
                        await conn.execute(view_stmt)
                    except Exception as e:
                        # Log view creation errors but don't fail the initialization
                        print(f"Warning: Failed to create view: {e}")
                        pass
    else:
        # For SQLite, create the crawl database
        # Create a config for the crawl database
        if crawl_db_path:
            crawl_config = DatabaseConfig(
                backend="sqlite",
                sqlite_path=crawl_db_path
            )
            set_global_config(crawl_config)
        
        async with create_connection() as conn:
            # Apply SQLite optimizations
            if hasattr(conn, '_optimize_connection'):
                await conn._optimize_connection()
            
            # Execute crawl schema
            import re
            schema_clean = re.sub(r'--.*$', '', SQLITE_CRAWL_SCHEMA, flags=re.MULTILINE)
            statements = [stmt.strip() for stmt in schema_clean.split(';') if stmt.strip()]
            
            for stmt in statements:
                if stmt:
                    await conn.execute(stmt)
            
            # Add reset_count column if it doesn't exist (migration)
            try:
                await conn.execute("ALTER TABLE frontier ADD COLUMN reset_count INTEGER DEFAULT 0")
            except Exception:
                # Column already exists, that's fine
                pass
            
            # Create database views
            view_statements = get_sqlite_views()
            for view_stmt in view_statements:
                if view_stmt:
                    await conn.execute(view_stmt)
            
            await conn.commit()
        
        # Restore the original config
        if crawl_db_path:
            set_global_config(config)


async def write_page(url_id: int, headers: dict, html: str, config: DatabaseConfig, pages_db_path: str = None):
    """Write a page to the database."""
    from .database import compress_headers, compress_html
    
    headers_compressed = compress_headers(headers)
    html_compressed = compress_html(html)
    
    if config.backend == "postgresql":
        query = """
        INSERT INTO pages (url_id, headers_json, html_compressed)
        VALUES ($1, $2, $3)
        """
        async with create_connection() as conn:
            await conn.execute(query, url_id, headers_compressed, html_compressed)
    else:
        query = """
        INSERT INTO pages (url_id, headers_json, html_compressed)
        VALUES (?, ?, ?)
        """
        async with create_connection() as conn:
            await conn.execute(query, url_id, headers_compressed, html_compressed)
            await conn.commit()


async def upsert_url(url: str, kind: str, base_domain: str, discovered_from_id: int = None, 
                    is_from_sitemap: bool = False, is_from_hreflang: bool = False, 
                    config: DatabaseConfig = None) -> int:
    """Upsert a URL and return its ID."""
    if config is None:
        config = get_database_config()
    
    # Get or create URL ID
    url_id = await get_or_create_url_id(url, base_domain, config)
    
    if config.backend == "postgresql":
        query = """
        INSERT INTO urls (url, kind, classification, discovered_from_id, is_from_sitemap, is_from_hreflang)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (url) DO UPDATE SET
            kind = EXCLUDED.kind,
            classification = EXCLUDED.classification,
            discovered_from_id = EXCLUDED.discovered_from_id,
            is_from_sitemap = EXCLUDED.is_from_sitemap,
            is_from_hreflang = EXCLUDED.is_from_hreflang
        RETURNING id
        """
        async with create_connection() as conn:
            result = await conn.fetchone(query, url, kind, classify_url(url, base_domain, is_from_sitemap, is_from_hreflang), 
                                       discovered_from_id, is_from_sitemap, is_from_hreflang)
            return result[0] if result else url_id
    else:
        query = """
        INSERT OR REPLACE INTO urls (id, url, kind, classification, discovered_from_id, is_from_sitemap, is_from_hreflang)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        async with create_connection() as conn:
            await conn.execute(query, url_id, url, kind, classify_url(url, base_domain, is_from_sitemap, is_from_hreflang), 
                             discovered_from_id, is_from_sitemap, is_from_hreflang)
            await conn.commit()
            return url_id


async def batch_get_or_create_url_ids(urls: List[str], base_domain: str, config: DatabaseConfig = None, conn: DatabaseConnection = None) -> Dict[str, int]:
    """Batch get or create URL IDs efficiently.
    
    Returns a dictionary mapping URL -> ID.
    Uses a single connection if provided, otherwise creates one.
    """
    if config is None:
        config = get_database_config()
    
    if not urls:
        return {}
    
    if config.backend == "postgresql":
        # Use provided connection or create one
        should_close = conn is None
        pooled_conn_wrapper = None
        if conn is None:
            pooled_conn_wrapper = create_connection()
            conn = await pooled_conn_wrapper.__aenter__()
        
        try:
            # First, try to get existing IDs
            existing_query = """
                SELECT url, id FROM urls WHERE url = ANY($1::text[])
            """
            existing_result = await conn.fetchall(existing_query, urls)
            url_to_id = {row[0]: row[1] for row in existing_result}
            
            # Find URLs that don't exist yet
            missing_urls = [url for url in urls if url not in url_to_id]
            
            if missing_urls:
                if len(missing_urls) > 100:
                    print(f"  Resolving {len(missing_urls)} new URLs...")
                # Batch insert missing URLs - use executemany but optimize with chunking
                # asyncpg's executemany uses prepared statements which is reasonably efficient
                from .db import classify_url
                
                # Prepare batch data
                insert_data = []
                for url in missing_urls:
                    classification = classify_url(url, base_domain)
                    insert_data.append((url, "other", classification))
                
                # Use executemany with chunking for better performance
                # Chunk size of 500 is a good balance between query size and round trips
                chunk_size = 500
                insert_query = """
                    INSERT INTO urls (url, kind, classification)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (url) DO UPDATE SET url = EXCLUDED.url
                """
                
                # Process in chunks to avoid memory issues and improve throughput
                for i in range(0, len(insert_data), chunk_size):
                    chunk = insert_data[i:i + chunk_size]
                    await conn.executemany(insert_query, chunk)
                
                # Now fetch all the IDs we just inserted/updated
                fetch_query = """
                    SELECT url, id FROM urls WHERE url = ANY($1::text[])
                """
                fetch_result = await conn.fetchall(fetch_query, missing_urls)
                for row in fetch_result:
                    url_to_id[row[0]] = row[1]
            
            return url_to_id
        finally:
            if should_close and pooled_conn_wrapper:
                await pooled_conn_wrapper.__aexit__(None, None, None)
    else:
        # SQLite implementation
        should_close = conn is None
        conn_wrapper = None
        if conn is None:
            conn_wrapper = create_connection()
            conn = await conn_wrapper.__aenter__()
        
        try:
            url_to_id = {}
            from .db import classify_url
            
            for url in urls:
                # Try to get existing ID
                result = await conn.fetchone("SELECT id FROM urls WHERE url = ?", url)
                if result:
                    url_to_id[url] = result[0]
                else:
                    # Insert new URL
                    await conn.execute(
                        "INSERT OR IGNORE INTO urls (url, kind, classification) VALUES (?, ?, ?)",
                        url, "other", classify_url(url, base_domain)
                    )
                    await conn.commit()
                    
                    # Get the ID
                    result = await conn.fetchone("SELECT id FROM urls WHERE url = ?", url)
                    if result:
                        url_to_id[url] = result[0]
            
            return url_to_id
        finally:
            if should_close and conn_wrapper:
                await conn_wrapper.__aexit__(None, None, None)


async def get_or_create_url_id(url: str, base_domain: str, config: DatabaseConfig = None) -> int:
    """Get or create a URL ID (single URL version, uses batch internally for efficiency)."""
    url_to_id = await batch_get_or_create_url_ids([url], base_domain, config)
    return url_to_id.get(url, 0)


def classify_url(url: str, base_domain: str, is_from_sitemap: bool = False, is_from_hreflang: bool = False) -> str:
    """Classify a URL as internal, external, etc."""
    from urllib.parse import urlparse
    
    parsed = urlparse(url)
    url_domain = parsed.netloc.lower()
    
    if is_from_hreflang:
        return "network"
    elif url_domain == base_domain:
        return "internal"
    elif url_domain.endswith(f".{base_domain}"):
        return "subdomain"
    elif any(social in url_domain for social in ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com', 'youtube.com']):
        return "social"
    else:
        return "external"


async def frontier_seed(url: str, base_domain: str, reset: bool = False, 
                       sitemap_priority: float = None, depth: int = 0, 
                       config: DatabaseConfig = None) -> bool:
    """Seed the frontier with a URL. Does NOT touch URLs that are already 'done'."""
    if config is None:
        config = get_database_config()
    
    # Get or create URL ID
    url_id = await get_or_create_url_id(url, base_domain, config)
    
    if reset:
        # Clear existing frontier entries for this URL
        if config.backend == "postgresql":
            query = "DELETE FROM frontier WHERE url_id = $1"
        else:
            query = "DELETE FROM frontier WHERE url_id = ?"
        
        async with create_connection() as conn:
            await conn.execute(query, url_id)
            if config.backend == "sqlite":
                await conn.commit()
    
    # If URL already exists in frontier (any status), skip it - only add NEW URLs
    # However, if URL is in urls table but not in frontier, add it to frontier
    if config.backend == "postgresql":
        async with create_connection() as conn:
            # Check if URL exists in frontier at all
            existing = await conn.fetchone(
                "SELECT url_id, status FROM frontier WHERE url_id = $1",
                url_id
            )
            # If URL already exists in frontier (any status), don't touch it - skip entirely
            if existing:
                return True  # URL already in frontier, skip
            
            # URL doesn't exist in frontier - add it as 'queued'
            # This handles the case where URL is in urls table but not in frontier (unknown status)
            priority_score = sitemap_priority or 0.0
            query = """
            INSERT INTO frontier (url_id, depth, status, priority_score, sitemap_priority)
            VALUES ($1, $2, 'queued', $3, $4)
            ON CONFLICT (url_id) DO NOTHING
            """
            await conn.execute(query, url_id, depth, priority_score, sitemap_priority)
    else:
        # SQLite implementation
        async with create_connection() as conn:
            # Check if URL exists in frontier at all
            existing = await conn.fetchone(
                "SELECT url_id FROM frontier WHERE url_id = ?",
                url_id
            )
            # If URL already exists in frontier, don't touch it - skip entirely
            if existing:
                return True
            
            # Only insert if URL doesn't exist in frontier
            query = """
            INSERT INTO frontier (url_id, depth, status, priority_score, sitemap_priority)
            VALUES (?, ?, 'queued', ?, ?)
            """
            priority_score = sitemap_priority or 0.0
            await conn.execute(query, url_id, depth, priority_score, sitemap_priority)
            await conn.commit()
    
    return True


async def filter_done_urls(urls: List[str], base_domain: str, config: DatabaseConfig = None) -> List[str]:
    """Filter out URLs that are already marked as 'done' in the frontier.
    
    Returns a list of URLs that are NOT done (i.e., can be safely added to frontier).
    """
    if config is None:
        config = get_database_config()
    
    if not urls:
        return []
    
    if config.backend == "postgresql":
        async with create_connection() as conn:
            # Get URL IDs for all URLs using batch_get_or_create_url_ids
            url_to_id_map = await batch_get_or_create_url_ids(urls, base_domain, config, conn)
            
            if not url_to_id_map:
                return urls  # If no IDs found, return all URLs (they'll be checked individually)
            
            # Check which ones are already 'done'
            url_ids = list(url_to_id_map.values())
            done_query = """
            SELECT url_id FROM frontier 
            WHERE url_id = ANY($1::bigint[]) AND status = 'done'
            """
            done_results = await conn.fetchall(done_query, url_ids)
            done_url_ids = {row[0] for row in done_results}
            
            # Filter out URLs that are done
            filtered_urls = [
                url for url in urls
                if url_to_id_map.get(url) not in done_url_ids
            ]
            return filtered_urls
    else:
        # SQLite - simpler approach, check individually
        filtered_urls = []
        async with create_connection() as conn:
            for url in urls:
                url_id = await get_or_create_url_id(url, base_domain, config)
                existing = await conn.fetchone(
                    "SELECT status FROM frontier WHERE url_id = ?",
                    url_id
                )
                if not existing or existing[0] != 'done':
                    filtered_urls.append(url)
        return filtered_urls


async def frontier_next_batch(batch_size: int, config: DatabaseConfig = None) -> List[Tuple[str, int, Optional[str]]]:
    """Get the next batch of URLs from the frontier and mark them as 'pending'.
    
    This prevents the same URLs from being fetched by multiple processes simultaneously.
    URLs are marked as 'pending' when fetched, then should be marked as 'done' after processing.
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        # Fetch URLs and immediately mark them as 'pending' in a single transaction
        # This prevents race conditions where the same URL could be fetched multiple times
        # We can't use FOR UPDATE with window functions, so we do it in two steps:
        # 1. Select and lock the rows
        # 2. Update them to 'pending' and return the results
        current_time = int(time.time())
        async with create_connection() as conn:
            # Start a transaction to ensure atomicity
            await conn.execute("BEGIN")
            try:
                # Step 1: Lock frontier rows first (FOR UPDATE only on frontier table, not on joined tables)
                # Then join to get URL strings
                select_query = """
                SELECT f.url_id, u.url, f.depth, p.url as parent_url
                FROM frontier f
                JOIN urls u ON f.url_id = u.id
                LEFT JOIN urls p ON f.parent_id = p.id
                WHERE f.status = 'queued'
                ORDER BY f.priority_score DESC, f.enqueued_at ASC
                LIMIT $1
                FOR UPDATE OF f SKIP LOCKED
                """
                selected = await conn.fetchall(select_query, batch_size * 2)  # Get extra to account for duplicates
                
                if not selected:
                    await conn.execute("ROLLBACK")
                    return []
                
                # Step 2: Deduplicate by URL string in Python (keep first occurrence)
                seen_urls = set()
                unique_selected = []
                duplicate_count = 0
                for row in selected:
                    url = row[1]  # url is the second column
                    if url not in seen_urls:
                        seen_urls.add(url)
                        unique_selected.append(row)
                        if len(unique_selected) >= batch_size:
                            break
                    else:
                        duplicate_count += 1
                
                if duplicate_count > 0:
                    print(f"  -> Warning: Found {duplicate_count} duplicate URLs in database query (deduplicated)")
                
                # Step 3: Update only the unique URLs to 'pending'
                url_ids = [row[0] for row in unique_selected]
                update_query = """
                UPDATE frontier
                SET status = 'pending', updated_at = $1
                WHERE url_id = ANY($2::bigint[])
                """
                await conn.execute(update_query, current_time, url_ids)
                await conn.execute("COMMIT")
                
                # Return the unique URLs
                return [(row[1], row[2], row[3]) for row in unique_selected]
            except Exception as e:
                await conn.execute("ROLLBACK")
                raise
    else:
        # SQLite: Fetch and update in separate queries (SQLite doesn't support UPDATE...FROM)
        query = """
        SELECT DISTINCT f.url_id, u.url, f.depth, p.url
        FROM frontier f
        JOIN urls u ON f.url_id = u.id
        LEFT JOIN urls p ON f.parent_id = p.id
        WHERE f.status = 'queued'
        ORDER BY f.priority_score DESC, f.enqueued_at ASC
        LIMIT ?
        """
        async with create_connection() as conn:
            results = await conn.fetchall(query, batch_size)
            if results:
                # Mark fetched URLs as 'pending'
                url_ids = [row[0] for row in results]
                current_time = int(time.time())
                update_query = """
                UPDATE frontier 
                SET status = 'pending', updated_at = ?
                WHERE url_id = ?
                """
                await conn.executemany(update_query, [(current_time, url_id) for url_id in url_ids])
                await conn.commit()
            # Return (url, depth, parent_url) - drop url_id
            return [(row[1], row[2], row[3]) for row in results]


async def frontier_reset_pending_to_queued(urls: List[str], base_domain: str, config: DatabaseConfig = None):
    """Reset URLs from 'pending' back to 'queued' status for retry.
    
    This is used when a URL fails and should be retried later.
    """
    if not urls:
        return
    
    if config is None:
        config = get_database_config()
    
    # Get URL IDs
    url_ids = []
    for url in urls:
        url_id = await get_or_create_url_id(url, base_domain, config)
        url_ids.append(url_id)
    
    # Sort URL IDs to ensure consistent lock ordering
    url_ids = sorted(url_ids)
    
    if config.backend == "postgresql":
        current_time = int(time.time())
        query = """
        UPDATE frontier 
        SET status = 'queued', updated_at = $1
        WHERE url_id = ANY($2::bigint[]) AND status = 'pending'
        """
        async with create_connection() as conn:
            await conn.execute(query, current_time, url_ids)
    else:
        query = """
        UPDATE frontier 
        SET status = 'queued', updated_at = strftime('%s', 'now')
        WHERE url_id = ? AND status = 'pending'
        """
        async with create_connection() as conn:
            for url_id in url_ids:
                await conn.execute(query, url_id)
            await conn.commit()


async def frontier_mark_done(urls: List[str], base_domain: str, config: DatabaseConfig = None):
    """Mark URLs as done in the frontier. 
    
    URLs should be in 'pending' status (from frontier_next_batch), and will be marked as 'done'.
    If a URL doesn't exist in frontier, it will be inserted with 'done' status.
    """
    if not urls:
        return
    
    if config is None:
        config = get_database_config()
    
    # Get URL IDs
    url_ids = []
    for url in urls:
        url_id = await get_or_create_url_id(url, base_domain, config)
        url_ids.append(url_id)
    
    # Sort URL IDs to ensure consistent lock ordering and prevent deadlocks
    url_ids = sorted(url_ids)
    
    if config.backend == "postgresql":
        current_time = int(time.time())
        # Update from 'pending' to 'done', or insert if missing
        query = """
        INSERT INTO frontier (url_id, depth, status, enqueued_at, updated_at, priority_score, reset_count)
        SELECT url_id, 0, 'done', $1, $1, 0.0, 0
        FROM UNNEST($2::bigint[]) AS url_id
        ON CONFLICT (url_id) DO UPDATE SET
            status = 'done',
            updated_at = $1,
            reset_count = 0
        WHERE frontier.status IN ('pending', 'queued')
        """
        # Retry logic for deadlock errors
        max_retries = 3
        retry_delay = 0.1  # 100ms
        
        for attempt in range(max_retries):
            try:
                async with create_connection() as conn:
                    await conn.execute(query, current_time, url_ids)
                break  # Success, exit retry loop
            except Exception as e:
                error_msg = str(e).lower()
                if "deadlock" in error_msg and attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = retry_delay * (2 ** attempt) + random.uniform(0, 0.05)
                    await asyncio.sleep(delay)
                    continue
                else:
                    # Re-raise if not a deadlock or out of retries
                    raise
    else:
        # SQLite: Update from 'pending' or 'queued' to 'done', reset counter
        query = """
        UPDATE frontier 
        SET status = 'done', updated_at = strftime('%s', 'now'), reset_count = 0
        WHERE url_id = ? AND status IN ('pending', 'queued')
        """
        async with create_connection() as conn:
            for url_id in url_ids:
                await conn.execute(query, url_id)
            await conn.commit()


async def frontier_reset_all_pending_to_queued(config: DatabaseConfig = None, max_reset_attempts: int = 5) -> int:
    """Reset all URLs stuck in 'pending' status back to 'queued'.
    
    URLs that have exceeded max_reset_attempts will be marked as 'done' with a timeout error
    instead of being reset again.
    
    Args:
        config: Database configuration
        max_reset_attempts: Maximum number of times a URL can be reset before being marked as failed
    
    Returns:
        Tuple of (reset_count, failed_count) - number of URLs reset and number marked as failed
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        current_time = int(time.time())
        async with create_connection() as conn:
            # First, mark URLs that have exceeded max attempts as done (timeout)
            failed_query = """
            UPDATE frontier 
            SET status = 'done', updated_at = $1
            WHERE status = 'pending' AND reset_count >= $2
            RETURNING url_id
            """
            failed_result = await conn.fetchall(failed_query, current_time, max_reset_attempts)
            failed_count = len(failed_result)
            
            if failed_count > 0:
                # Log the failed URLs
                url_rows = await conn.fetchall("SELECT url FROM urls WHERE id = ANY($1::bigint[])", 
                                               [row[0] for row in failed_result])
                failed_urls = [row[0] for row in url_rows]
                if len(failed_urls) <= 5:
                    for url in failed_urls:
                        print(f"  -> Marking {url} as failed (timeout: exceeded {max_reset_attempts} reset attempts)")
                else:
                    for url in failed_urls[:3]:
                        print(f"  -> Marking {url} as failed (timeout: exceeded {max_reset_attempts} reset attempts)")
                    print(f"  -> ... and {len(failed_urls) - 3} more URLs marked as failed")
            
            # Now reset URLs that haven't exceeded max attempts
            reset_query = """
            UPDATE frontier 
            SET status = 'queued', updated_at = $1, reset_count = reset_count + 1
            WHERE status = 'pending' AND reset_count < $2
            RETURNING url_id
            """
            reset_result = await conn.fetchall(reset_query, current_time, max_reset_attempts)
            reset_count = len(reset_result)
            
            if failed_count > 0:
                print(f"  -> Marked {failed_count} URLs as failed (exceeded {max_reset_attempts} reset attempts)")
            
            return reset_count
    else:
        async with create_connection() as conn:
            # First, mark URLs that have exceeded max attempts as done (timeout)
            failed_query = """
            UPDATE frontier 
            SET status = 'done', updated_at = strftime('%s', 'now')
            WHERE status = 'pending' AND reset_count >= ?
            """
            await conn.execute(failed_query, max_reset_attempts)
            failed_count_result = await conn.fetchone("SELECT changes()")
            failed_count = failed_count_result[0] if failed_count_result and failed_count_result[0] is not None else 0
            
            if failed_count > 0:
                # Log the failed URLs
                failed_urls_query = """
                SELECT u.url 
                FROM frontier f
                JOIN urls u ON f.url_id = u.id
                WHERE f.status = 'done' AND f.reset_count >= ?
                """
                failed_urls = await conn.fetchall(failed_urls_query, max_reset_attempts)
                failed_url_list = [row[0] for row in failed_urls]
                if len(failed_url_list) <= 5:
                    for url in failed_url_list:
                        print(f"  -> Marking {url} as failed (timeout: exceeded {max_reset_attempts} reset attempts)")
                else:
                    for url in failed_url_list[:3]:
                        print(f"  -> Marking {url} as failed (timeout: exceeded {max_reset_attempts} reset attempts)")
                    print(f"  -> ... and {len(failed_url_list) - 3} more URLs marked as failed")
            
            # Now reset URLs that haven't exceeded max attempts
            reset_query = """
            UPDATE frontier 
            SET status = 'queued', updated_at = strftime('%s', 'now'), reset_count = reset_count + 1
            WHERE status = 'pending' AND reset_count < ?
            """
            await conn.execute(reset_query, max_reset_attempts)
            await conn.commit()
            
            reset_count_result = await conn.fetchone("SELECT changes()")
            reset_count = reset_count_result[0] if reset_count_result and reset_count_result[0] is not None else 0
            
            if failed_count > 0:
                print(f"  -> Marked {failed_count} URLs as failed (exceeded {max_reset_attempts} reset attempts)")
            
            return reset_count


async def frontier_stats(config: DatabaseConfig = None) -> Tuple[int, int]:
    """Get frontier statistics."""
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE status = 'queued') as queued,
            COUNT(*) FILTER (WHERE status = 'done') as done
        FROM frontier
        """
    else:
        query = """
        SELECT 
            SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) as queued,
            SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) as done
        FROM frontier
        """
    
    async with create_connection() as conn:
        result = await conn.fetchone(query)
        return (result[0] or 0, result[1] or 0) if result else (0, 0)


# Additional database operations can be added here following the same pattern
# Each operation should check the backend type and use appropriate SQL syntax


async def batch_write_pages(pages_data: List[Tuple], pages_db_path: str = None, crawl_db_path: str = None, config: DatabaseConfig = None):
    """Write pages data to the database."""
    if config is None:
        config = get_database_config()
    
    if not pages_data:
        return
    
    if config.backend == "postgresql":
        # Local import to avoid UnboundLocalError in exception paths
        import json
        # For PostgreSQL, write to both pages and page_metadata tables
        # pages_data format: (original_norm, final_norm, status, headers_norm, text, base_domain, redirect_chain_json)
        pages_query = """
        INSERT INTO pages (url_id, headers_json, html_compressed)
        VALUES ($1, $2, $3)
        ON CONFLICT (url_id) DO UPDATE SET
            headers_json = EXCLUDED.headers_json,
            html_compressed = EXCLUDED.html_compressed
        """
        
        metadata_query = """
        INSERT INTO page_metadata (url_id, initial_status_code, final_status_code, final_url_id, redirect_destination_url_id, fetched_at, etag, last_modified)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (url_id) DO UPDATE SET
            initial_status_code = EXCLUDED.initial_status_code,
            final_status_code = EXCLUDED.final_status_code,
            final_url_id = EXCLUDED.final_url_id,
            redirect_destination_url_id = EXCLUDED.redirect_destination_url_id,
            fetched_at = EXCLUDED.fetched_at,
            etag = EXCLUDED.etag,
            last_modified = EXCLUDED.last_modified
        """
        
        async with create_connection() as conn:
            # Extract base_domain from first tuple (all should have the same base_domain)
            if pages_data:
                _, _, _, _, _, base_domain, _ = pages_data[0]
            else:
                base_domain = ""
            
            # Collect all URLs that need ID resolution (both original and final URLs)
            urls_to_resolve = []
            for original_norm, final_norm, _, _, _, _, _ in pages_data:
                urls_to_resolve.append(original_norm)
                if final_norm != original_norm:
                    urls_to_resolve.append(final_norm)
            
            # Batch resolve all URL IDs at once
            url_to_id_map = await batch_get_or_create_url_ids(urls_to_resolve, base_domain, config, conn)
            
            # Prepare batch data for pages and metadata
            pages_batch_data = []
            metadata_batch_data = []
            current_time = int(time.time())
            
            for page_tuple in pages_data:
                original_norm, final_norm, status, headers_norm, text, base_domain, redirect_chain_json = page_tuple
                
                # Get URL IDs from the map
                url_id = url_to_id_map.get(original_norm)
                
                # If URL ID is missing, try to get/create it individually (shouldn't happen, but be defensive)
                if not url_id:
                    print(f"Warning: URL ID not found for {original_norm}, attempting to create...")
                    url_id = await get_or_create_url_id(original_norm, base_domain, config)
                    if url_id:
                        url_to_id_map[original_norm] = url_id
                
                final_url_id = url_to_id_map.get(final_norm) if final_norm != original_norm else url_id
                if not final_url_id and final_norm != original_norm:
                    final_url_id = await get_or_create_url_id(final_norm, base_domain, config)
                    if final_url_id:
                        url_to_id_map[final_norm] = final_url_id
                
                # Only add if we got a valid URL ID
                # If we still don't have a URL ID after trying to create it, log an error but continue
                # (This should be extremely rare - indicates a serious database issue)
                if not url_id:
                    print(f"ERROR: Could not create URL ID for {original_norm} - skipping page metadata insertion")
                    continue
                
                # Extract initial status code and redirect destination from redirect chain
                # status should always be an integer (0 for errors, or HTTP status code)
                initial_status_code = status if status is not None else 0
                redirect_destination_url_id = None
                
                if redirect_chain_json:
                        try:
                            redirect_chain = json.loads(redirect_chain_json)
                            if redirect_chain:
                                # First step in chain is the initial request
                                initial_status_code = redirect_chain[0].get('status', status if status else 0)
                                
                                # If there's a redirect (301, 302, etc.), find the destination
                                if initial_status_code in [301, 302, 303, 307, 308]:
                                    # Look for Location header in the first redirect response
                                    first_response_headers = redirect_chain[0].get('headers', {})
                                    location = first_response_headers.get('location')
                                    if location:
                                        # Normalize the redirect destination URL
                                        from urllib.parse import urljoin
                                        redirect_destination = urljoin(original_norm, location)
                                        redirect_destination_url_id = url_to_id_map.get(redirect_destination)
                                        if redirect_destination_url_id is None:
                                            # Need to resolve this URL
                                            redirect_dest_id_map = await batch_get_or_create_url_ids([redirect_destination], base_domain, config, conn)
                                            redirect_destination_url_id = redirect_dest_id_map.get(redirect_destination)
                        except (json.JSONDecodeError, KeyError, IndexError):
                            # If parsing fails, use defaults
                            pass
                    
                # Ensure initial_status_code is never None
                if initial_status_code is None:
                    initial_status_code = 0
                
                # Extract ETag and Last-Modified from headers
                etag = headers_norm.get('etag', '').strip('"') if headers_norm and headers_norm.get('etag') else None
                last_modified = headers_norm.get('last-modified', '').strip() if headers_norm and headers_norm.get('last-modified') else None
                
                # Compress HTML content
                from .database import compress_html
                html_compressed = compress_html(text) if text else None
                
                # Pages data (HTML and headers only)
                pages_batch_data.append((
                    url_id,
                    json.dumps(headers_norm) if headers_norm else None,
                    html_compressed
                ))
                
                # Metadata data (status, timestamps, etc.)
                metadata_batch_data.append((
                    url_id,
                    initial_status_code,
                    status,
                    final_url_id if final_url_id != url_id else None,
                    redirect_destination_url_id,
                    current_time,
                    etag,
                    last_modified
                ))
            
            # Batch insert pages using executemany with chunking
            if pages_batch_data:
                chunk_size = 500
                for i in range(0, len(pages_batch_data), chunk_size):
                    chunk = pages_batch_data[i:i + chunk_size]
                    await conn.executemany(pages_query, chunk)
            
            # Batch insert metadata using executemany with chunking
            if metadata_batch_data:
                print(f"  -> DEBUG: Inserting {len(metadata_batch_data)} page_metadata rows...")
                chunk_size = 500
                for i in range(0, len(metadata_batch_data), chunk_size):
                    chunk = metadata_batch_data[i:i + chunk_size]
                    try:
                        await conn.executemany(metadata_query, chunk)
                        print(f"  -> DEBUG: Successfully inserted page_metadata chunk {i//chunk_size + 1} ({len(chunk)} rows)")
                    except Exception as e:
                        print(f"  -> ERROR: Failed to insert page_metadata chunk: {e}")
                        print(f"  -> DEBUG: First row in failed chunk: {chunk[0] if chunk else 'N/A'}")
                        raise
            else:
                print(f"  -> WARNING: metadata_batch_data is empty! pages_data had {len(pages_data)} items")
            
            # Calculate and upsert indexability for pages that don't have it yet
            # This ensures indexability is calculated even when content extraction didn't happen
            # But we only do this if indexability doesn't already exist (to avoid overwriting with empty arrays)
            indexability_rows = []
            for page_tuple in pages_data:
                original_norm, final_norm, status, headers_norm, text, base_domain, redirect_chain_json = page_tuple
                url_id = url_to_id_map.get(original_norm, 0)
                if not url_id:
                    continue
                
                # Check if indexability already exists - if so, skip (content extraction will handle it)
                existing_indexability = await conn.fetchone(
                    "SELECT url_id FROM indexability WHERE url_id = $1",
                    url_id
                )
                if existing_indexability:
                    continue  # Skip - content extraction will update it with proper data
                
                try:
                    from .robots import is_url_crawlable
                    import json
                    
                    # Extract directives from headers (already normalized to lowercase)
                    http_header_directives = []
                    if headers_norm:
                        x_robots_tag = headers_norm.get('x-robots-tag', '')
                        if x_robots_tag:
                            http_header_directives = [d.strip().lower() for d in x_robots_tag.split(',')]
                    
                    # For HTML pages, we can't extract meta robots here (need HTML parsing)
                    # Use empty list for parity with SQLite; content extraction can overwrite later
                    html_meta_directives = []
                    html_meta_allows = True  # Default assumption until content extraction runs
                    
                    http_header_allows = not any('noindex' in d for d in http_header_directives) if http_header_directives else True
                    
                    robots_txt_allows = is_url_crawlable(final_norm, "SQLiteCrawler/0.2")
                    robots_txt_directives = ['disallow'] if not robots_txt_allows else []
                    robots_txt_directives_str = json.dumps(robots_txt_directives, ensure_ascii=False)
                    
                    # Store robots.txt directives in robots_directives table
                    from .robots import get_matching_robots_txt_rules
                    matching_rules = get_matching_robots_txt_rules(final_norm, "SQLiteCrawler/0.2")
                    for rule_type, rule_path in matching_rules:
                        try:
                            # Get or create directive ID for the rule type
                            directive_norm = rule_type.strip().lower()
                            existing_row = await conn.fetchone(
                                "SELECT id FROM robots_directive_strings WHERE directive = $1",
                                directive_norm
                            )
                            if existing_row:
                                dir_id = existing_row[0]
                            else:
                                insert_row = await conn.fetchone(
                                    "INSERT INTO robots_directive_strings (directive) VALUES ($1) RETURNING id",
                                    directive_norm
                                )
                                dir_id = insert_row[0] if insert_row else None
                            
                            if dir_id:
                                await conn.execute(
                                    """
                                    INSERT INTO robots_directives (url_id, source, directive_id, value)
                                    SELECT $1, 'robots_txt', $2, $3
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM robots_directives 
                                        WHERE url_id = $1 AND source = 'robots_txt' AND directive_id = $2 AND value = $3
                                    )
                                    """,
                                    url_id, dir_id, rule_path
                                )
                        except Exception as e:
                            print(f"Warning: could not insert robots_txt directive '{rule_type}' (path: {rule_path}) for url_id {url_id} in batch_write_pages: {e}")
                    
                    # Check canonical self
                    canonical_row = await conn.fetchone(
                        "SELECT canonical_url_id FROM canonical_urls WHERE url_id = $1",
                        url_id
                    )
                    is_self_canonical = canonical_row and canonical_row[0] == url_id
                    
                    # Get initial status code from page_metadata (which we just inserted)
                    page_meta_row = await conn.fetchone(
                        "SELECT initial_status_code FROM page_metadata WHERE url_id = $1",
                        url_id
                    )
                    initial_status_code = page_meta_row[0] if page_meta_row and page_meta_row[0] is not None else status
                    
                    # Ensure initial_status_code is never None
                    if initial_status_code is None:
                        initial_status_code = status if status else 0
                    
                    overall_indexable = (
                        initial_status_code == 200 and
                        is_self_canonical and
                        robots_txt_allows and
                        html_meta_allows and
                        http_header_allows
                    )
                    
                    http_header_directives_str = json.dumps(http_header_directives or [], ensure_ascii=False)
                    
                    indexability_rows.append((
                        url_id,
                        robots_txt_allows,
                        html_meta_allows,
                        http_header_allows,
                        robots_txt_directives_str,
                        None,  # html_meta_directives - leave NULL for content extraction to fill
                        http_header_directives_str,
                        overall_indexable
                    ))
                    # Debug: log first few to verify values
                    if len(indexability_rows) <= 3:
                        print(f"  -> DEBUG indexability: url_id={url_id}, overall_indexable={overall_indexable} (type: {type(overall_indexable)}), initial_status={initial_status_code}, is_self_canonical={is_self_canonical}, robots_txt_allows={robots_txt_allows}, html_meta_allows={html_meta_allows}, http_header_allows={http_header_allows}")
                except Exception as e:
                    # Best-effort; skip indexability on errors
                    print(f"Warning: could not calculate indexability for url_id {url_id}: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Upsert indexability rows (only for URLs that don't have it yet)
            if indexability_rows:
                print(f"  -> DEBUG: Inserting {len(indexability_rows)} indexability rows from batch_write_pages...")
                indexability_query = """
                INSERT INTO indexability (
                    url_id, robots_txt_allows, html_meta_allows, http_header_allows,
                    robots_txt_directives, html_meta_directives, http_header_directives, overall_indexable
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (url_id) DO NOTHING
                """
                try:
                    await conn.executemany(indexability_query, indexability_rows)
                    print(f"  -> DEBUG: Successfully inserted {len(indexability_rows)} indexability rows")
                except Exception as e:
                    print(f"Warning: could not upsert indexability rows from batch_write_pages: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"  -> DEBUG: No indexability rows to insert from batch_write_pages (all may already exist)")
    else:
        # For SQLite, use the original implementation
        from .db import batch_write_pages as sqlite_batch_write_pages
        # Use the database paths from config or the provided paths
        pages_path = pages_db_path if pages_db_path else (config.sqlite_path if config and hasattr(config, 'sqlite_path') else None)
        crawl_path = crawl_db_path if crawl_db_path else (config.sqlite_path if config and hasattr(config, 'sqlite_path') else None)
        await sqlite_batch_write_pages(pages_data, pages_path, crawl_path)


async def batch_upsert_urls(urls_data: List[Tuple], config: DatabaseConfig = None):
    """Upsert URLs data to the database."""
    if config is None:
        config = get_database_config()
    
    if not urls_data:
        return
    
    if config.backend == "postgresql":
        query = """
        INSERT INTO urls (url, kind, classification, discovered_from_id, first_seen, last_seen, headers_compressed)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (url) DO UPDATE SET
            kind = EXCLUDED.kind,
            classification = EXCLUDED.classification,
            discovered_from_id = EXCLUDED.discovered_from_id,
            last_seen = EXCLUDED.last_seen,
            headers_compressed = EXCLUDED.headers_compressed
        """
        
        async with create_connection() as conn:
            # Collect all URLs that need ID resolution (main URLs + discovered_from URLs)
            all_urls_to_resolve = []
            discovered_from_urls = []
            
            for url_tuple in urls_data:
                # url_tuple format: (url, kind, base_domain, discovered_from_id) or (url, kind, base_domain, discovered_from_id, is_from_sitemap)
                if len(url_tuple) == 4:
                    url, kind, base_domain, discovered_from_id = url_tuple
                    is_from_sitemap = False
                else:
                    url, kind, base_domain, discovered_from_id, is_from_sitemap = url_tuple
                
                all_urls_to_resolve.append(url)
                if discovered_from_id:
                    discovered_from_urls.append(discovered_from_id)
            
            # Batch resolve all URL IDs at once
            all_urls_to_resolve.extend(discovered_from_urls)
            if len(all_urls_to_resolve) > 100:
                print(f"  Resolving {len(all_urls_to_resolve)} URL IDs...")
            url_to_id_map = await batch_get_or_create_url_ids(all_urls_to_resolve, base_domain, config, conn)
            
            # Prepare batch data
            batch_data = []
            current_time = int(time.time())
            
            for url_tuple in urls_data:
                # url_tuple format: (url, kind, base_domain, discovered_from_id) or (url, kind, base_domain, discovered_from_id, is_from_sitemap)
                if len(url_tuple) == 4:
                    url, kind, base_domain, discovered_from_id = url_tuple
                    is_from_sitemap = False
                else:
                    url, kind, base_domain, discovered_from_id, is_from_sitemap = url_tuple
                
                # Get discovered_from_id if provided
                discovered_from_url_id = url_to_id_map.get(discovered_from_id) if discovered_from_id else None
                
                # Classify URL
                from .db import classify_url
                classification = classify_url(url, base_domain, is_from_sitemap=is_from_sitemap)
                
                batch_data.append((
                    url,
                    kind,
                    classification,
                    discovered_from_url_id,
                    current_time,  # first_seen
                    current_time,  # last_seen
                    None  # headers_compressed
                ))
            
            # Batch insert using executemany with chunking for better performance
            if batch_data:
                chunk_size = 500
                for i in range(0, len(batch_data), chunk_size):
                    chunk = batch_data[i:i + chunk_size]
                    await conn.executemany(query, chunk)
    else:
        # For SQLite, use the original implementation with the correct database path
        from .db import batch_upsert_urls as sqlite_batch_upsert_urls
        db_path = config.sqlite_path if config and hasattr(config, 'sqlite_path') else None
        if db_path:
            await sqlite_batch_upsert_urls(urls_data, db_path)
        else:
            await sqlite_batch_upsert_urls(urls_data)


async def batch_enqueue_frontier(frontier_data: List[Tuple], config: DatabaseConfig = None) -> int:
    """Enqueue URLs to the frontier. Does NOT touch URLs that are already 'done'.
    
    Returns:
        int: Number of URLs actually enqueued (after filtering out existing ones)
    """
    if config is None:
        config = get_database_config()
    
    if not frontier_data:
        return 0
    
    if config.backend == "postgresql":
        # Use a single connection for both checking and inserting to avoid transaction isolation issues
        async with create_connection() as conn:
            # Collect all URLs that need ID resolution
            urls_to_resolve = []
            for frontier_tuple in frontier_data:
                child_norm, depth, original_norm, base_domain = frontier_tuple
                urls_to_resolve.append(child_norm)
                if original_norm:
                    urls_to_resolve.append(original_norm)
            
            # Batch resolve all URL IDs at once
            url_to_id_map = await batch_get_or_create_url_ids(urls_to_resolve, base_domain, config, conn)
            
            # Check which URLs already exist in frontier (any status) and filter them out
            # This prevents re-enqueuing URLs that are already 'done' or 'queued'
            child_url_ids = [url_to_id_map.get(child_norm) for child_norm, _, _, _ in frontier_data if url_to_id_map.get(child_norm)]
            if child_url_ids:
                existing_query = """
                SELECT url_id, status FROM frontier 
                WHERE url_id = ANY($1::bigint[])
                """
                existing_results = await conn.fetchall(existing_query, child_url_ids)
                existing_url_ids = {row[0] for row in existing_results}  # Any status
                done_url_ids = {row[0] for row in existing_results if row[1] == 'done'}  # Only 'done' status
                
                # Filter out URLs that already exist in frontier (any status)
                frontier_data = [
                    ft for ft in frontier_data
                    if url_to_id_map.get(ft[0]) not in existing_url_ids
                ]
                
                # Ensure we never try to enqueue URLs that are already 'done'
                # This is a defensive check - the filtering above should already handle this
                if done_url_ids:
                    # Double-check: filter out any URLs that are 'done' (should already be filtered, but be extra safe)
                    frontier_data = [
                        ft for ft in frontier_data
                        if url_to_id_map.get(ft[0]) not in done_url_ids
                    ]
            
            if not frontier_data:
                return 0  # All URLs already exist in frontier
            
            # Now insert using the same connection
            query = """
            INSERT INTO frontier (url_id, depth, parent_id, status, enqueued_at, priority_score, sitemap_priority, inlinks_count, content_type_score)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (url_id) DO NOTHING
            """
            
            # Prepare batch data
            batch_data = []
            current_time = int(time.time())
            
            for frontier_tuple in frontier_data:
                # frontier_tuple format: (child_norm, depth + 1, original_norm, base_domain)
                child_norm, depth, original_norm, base_domain = frontier_tuple
                
                # Get URL IDs from the map
                url_id = url_to_id_map.get(child_norm, 0)
                parent_id = url_to_id_map.get(original_norm) if original_norm else None
                
                if url_id:  # Only add if we got a valid URL ID
                    batch_data.append((
                        url_id,
                        depth,
                        parent_id,
                        'queued',  # status
                        current_time,  # enqueued_at
                        0.0,  # priority_score
                        0.5,  # sitemap_priority
                        0,  # inlinks_count
                        1.0  # content_type_score
                    ))
            
            # Sort batch data by url_id to ensure consistent lock ordering and prevent deadlocks
            batch_data.sort(key=lambda x: x[0])  # Sort by url_id (first element)
            
            # Batch insert using executemany with chunking for better performance
            if batch_data:
                chunk_size = 500
                max_retries = 3
                retry_delay = 0.1  # 100ms
                total_inserted = 0
                
                for i in range(0, len(batch_data), chunk_size):
                    chunk = batch_data[i:i + chunk_size]
                    
                    # Retry logic for deadlock errors
                    for attempt in range(max_retries):
                        try:
                            await conn.executemany(query, chunk)
                            total_inserted += len(chunk)
                            break  # Success, exit retry loop
                        except Exception as e:
                            error_msg = str(e).lower()
                            if "deadlock" in error_msg and attempt < max_retries - 1:
                                # Exponential backoff with jitter
                                delay = retry_delay * (2 ** attempt) + random.uniform(0, 0.05)
                                await asyncio.sleep(delay)
                                continue
                            else:
                                # Re-raise if not a deadlock or out of retries
                                raise
                
                return total_inserted
            else:
                return 0
    else:
        # For SQLite, use the original implementation with the correct database path
        from .db import batch_enqueue_frontier as sqlite_batch_enqueue_frontier
        db_path = config.sqlite_path if config and hasattr(config, 'sqlite_path') else None
        if db_path:
            await sqlite_batch_enqueue_frontier(frontier_data, db_path)
        else:
            await sqlite_batch_enqueue_frontier(frontier_data)
        # SQLite version doesn't return count, estimate from input
        return len(frontier_data)


async def batch_write_content_with_url_resolution(content_data: List[Dict], crawl_db_path: str = None, config: DatabaseConfig = None):
    """Write content data to the database with URL resolution."""
    if config is None:
        config = get_database_config()
    
    if not content_data:
        return
    
    if config.backend == "postgresql":
        # For PostgreSQL, write to the content table
        query = """
        INSERT INTO content (
            url_id, title, meta_description_id, h1_tags, h2_tags, word_count,
            html_lang_id, internal_links_count, external_links_count,
            internal_links_unique_count, external_links_unique_count,
            crawl_depth, inlinks_count, inlinks_unique_count,
            content_hash_sha256, content_hash_simhash, content_length
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (url_id) DO UPDATE SET
            title = EXCLUDED.title,
            meta_description_id = EXCLUDED.meta_description_id,
            h1_tags = EXCLUDED.h1_tags,
            h2_tags = EXCLUDED.h2_tags,
            word_count = EXCLUDED.word_count,
            html_lang_id = EXCLUDED.html_lang_id,
            internal_links_count = EXCLUDED.internal_links_count,
            external_links_count = EXCLUDED.external_links_count,
            internal_links_unique_count = EXCLUDED.internal_links_unique_count,
            external_links_unique_count = EXCLUDED.external_links_unique_count,
            crawl_depth = EXCLUDED.crawl_depth,
            inlinks_count = EXCLUDED.inlinks_count,
            inlinks_unique_count = EXCLUDED.inlinks_unique_count,
            content_hash_sha256 = EXCLUDED.content_hash_sha256,
            content_hash_simhash = EXCLUDED.content_hash_simhash,
            content_length = EXCLUDED.content_length
        """
        
        async with create_connection() as conn:
            # Extract base_domain from first tuple (all should have the same base_domain)
            # content_tuple format: (final_norm, content_item, base_domain, depth)
            if content_data:
                _, _, base_domain, _ = content_data[0]
            else:
                base_domain = ""
            
            # Collect all URLs that need ID resolution
            # content_tuple format: (final_norm, content_item, base_domain, depth)
            urls_to_resolve = [final_norm for final_norm, _, _, _ in content_data]
            
            # Batch resolve all URL IDs at once
            url_to_id_map = await batch_get_or_create_url_ids(urls_to_resolve, base_domain, config, conn)
            
            # Batch resolve meta_description and html_lang IDs upfront (much faster!)
            meta_descriptions_to_resolve = []
            html_langs_to_resolve = []
            for content_tuple in content_data:
                _, content_item, _, _ = content_tuple
                if content_item.get('meta_description'):
                    meta_descriptions_to_resolve.append(content_item['meta_description'])
                if content_item.get('html_lang'):
                    html_langs_to_resolve.append(content_item['html_lang'])
            
            # Batch resolve meta_description IDs using the same connection
            meta_desc_to_id = {}
            if meta_descriptions_to_resolve:
                unique_meta_descs = list(set(meta_descriptions_to_resolve))
                # Fetch existing IDs
                if unique_meta_descs:
                    existing_query = "SELECT id, description FROM meta_descriptions WHERE description = ANY($1::text[])"
                    existing_result = await conn.fetchall(existing_query, unique_meta_descs)
                    meta_desc_to_id = {row[1]: row[0] for row in existing_result}
                
                # Insert missing ones using batch insert
                missing_meta_descs = [md for md in unique_meta_descs if md not in meta_desc_to_id]
                if missing_meta_descs:
                    # Batch insert missing meta descriptions
                    insert_data = [(md,) for md in missing_meta_descs]
                    insert_query = "INSERT INTO meta_descriptions (description) VALUES ($1) ON CONFLICT (description) DO NOTHING"
                    chunk_size = 500
                    for i in range(0, len(insert_data), chunk_size):
                        chunk = insert_data[i:i + chunk_size]
                        await conn.executemany(insert_query, chunk)
                    
                    # Fetch all IDs (including ones that already existed due to race conditions)
                    fetch_query = "SELECT id, description FROM meta_descriptions WHERE description = ANY($1::text[])"
                    fetch_result = await conn.fetchall(fetch_query, missing_meta_descs)
                    for row in fetch_result:
                        meta_desc_to_id[row[1]] = row[0]
            
            # Batch resolve html_lang IDs using the same connection
            html_lang_to_id = {}
            if html_langs_to_resolve:
                unique_html_langs = list(set(html_langs_to_resolve))
                # Fetch existing IDs
                if unique_html_langs:
                    existing_query = "SELECT id, language_code FROM html_languages WHERE language_code = ANY($1::text[])"
                    existing_result = await conn.fetchall(existing_query, unique_html_langs)
                    html_lang_to_id = {row[1]: row[0] for row in existing_result}
                
                # Insert missing ones using batch insert
                missing_html_langs = [hl for hl in unique_html_langs if hl not in html_lang_to_id]
                if missing_html_langs:
                    # Batch insert missing HTML languages
                    insert_data = [(hl,) for hl in missing_html_langs]
                    insert_query = "INSERT INTO html_languages (language_code) VALUES ($1) ON CONFLICT (language_code) DO NOTHING"
                    chunk_size = 500
                    for i in range(0, len(insert_data), chunk_size):
                        chunk = insert_data[i:i + chunk_size]
                        await conn.executemany(insert_query, chunk)
                    
                    # Fetch all IDs (including ones that already existed due to race conditions)
                    fetch_query = "SELECT id, language_code FROM html_languages WHERE language_code = ANY($1::text[])"
                    fetch_result = await conn.fetchall(fetch_query, missing_html_langs)
                    for row in fetch_result:
                        html_lang_to_id[row[1]] = row[0]
            
            # Prepare batch data using the resolved IDs
            batch_data = []
            indexability_rows = []
            schema_rows = []  # (url_id, schema_items)
            
            async def get_or_create_hreflang_language_id_pg(language_code: str) -> int | None:
                """Get or create hreflang language ID for PostgreSQL."""
                if not language_code:
                    return None
                existing_row = await conn.fetchone(
                    "SELECT id FROM hreflang_languages WHERE language_code = $1",
                    language_code
                )
                if existing_row:
                    return existing_row[0]
                insert_row = await conn.fetchone(
                    """
                    INSERT INTO hreflang_languages (language_code)
                    VALUES ($1)
                    ON CONFLICT (language_code) DO UPDATE SET
                        language_code = EXCLUDED.language_code
                    RETURNING id
                    """,
                    language_code
                )
                return insert_row[0] if insert_row else None
            
            for content_tuple in content_data:
                # content_tuple format: (final_norm, content_item, base_domain, depth)
                final_norm, content_item, base_domain, crawl_depth = content_tuple
                
                # Get URL ID from the map
                url_id = url_to_id_map.get(final_norm, 0)
                
                if not url_id:
                    continue  # Skip if URL ID not found
                
                # Get meta description ID from batch map
                meta_description_id = None
                if content_item.get('meta_description'):
                    meta_description_id = meta_desc_to_id.get(content_item['meta_description'])
                
                # Get HTML language ID from batch map
                html_lang_id = None
                if content_item.get('html_lang'):
                    html_lang_id = html_lang_to_id.get(content_item['html_lang'])
                
                # Store h1/h2 as JSON strings (parity with SQLite)
                h1_tags_raw = content_item.get('h1_tags')
                if isinstance(h1_tags_raw, list):
                    h1_tags = json.dumps(h1_tags_raw, ensure_ascii=False)
                else:
                    h1_tags = h1_tags_raw
                
                h2_tags_raw = content_item.get('h2_tags')
                if isinstance(h2_tags_raw, list):
                    h2_tags = json.dumps(h2_tags_raw, ensure_ascii=False)
                else:
                    h2_tags = h2_tags_raw
                
                batch_data.append((
                    url_id,
                    content_item.get('title'),
                    meta_description_id,
                    h1_tags,
                    h2_tags,
                    content_item.get('word_count'),
                    html_lang_id,
                    content_item.get('internal_links_count', 0),
                    content_item.get('external_links_count', 0),
                    content_item.get('internal_links_unique_count', 0),
                    content_item.get('external_links_unique_count', 0),
                    crawl_depth,  # Use depth from frontier, not from content_item
                    content_item.get('inlinks_count', 0),
                    content_item.get('inlinks_unique_count', 0),
                    content_item.get('content_hash_sha256'),
                    content_item.get('content_hash_simhash'),
                    content_item.get('content_length')
                ))

                # ---- Robots directives (PostgreSQL) ----
                async def get_or_create_robots_directive_id_pg(directive: str) -> int:
                    """Normalize and upsert a robots directive string, return its id."""
                    directive_norm = directive.strip().lower()
                    # Use fetchone and extract the value since fetchval isn't available on the wrapper
                    existing_row = await conn.fetchone(
                        "SELECT id FROM robots_directive_strings WHERE directive = $1",
                        directive_norm
                    )
                    if existing_row:
                        return existing_row[0]
                    # Insert and return the new ID
                    insert_row = await conn.fetchone(
                        "INSERT INTO robots_directive_strings (directive) VALUES ($1) RETURNING id",
                        directive_norm
                    )
                    return insert_row[0] if insert_row else None

                # From HTML meta
                if content_item.get('html_meta_directives'):
                    for directive in content_item['html_meta_directives']:
                        try:
                            dir_id = await get_or_create_robots_directive_id_pg(directive)
                            await conn.execute(
                                """
                                INSERT INTO robots_directives (url_id, source, directive_id)
                                SELECT $1, 'html_meta', $2
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM robots_directives 
                                    WHERE url_id = $1 AND source = 'html_meta' AND directive_id = $2
                                )
                                """,
                                url_id, dir_id
                            )
                        except Exception as e:
                            print(f"Warning: could not insert html_meta directive '{directive}' for url_id {url_id}: {e}")

                # From HTTP headers
                if content_item.get('http_header_directives'):
                    for directive in content_item['http_header_directives']:
                        try:
                            dir_id = await get_or_create_robots_directive_id_pg(directive)
                            await conn.execute(
                                """
                                INSERT INTO robots_directives (url_id, source, directive_id)
                                SELECT $1, 'http_header', $2
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM robots_directives 
                                    WHERE url_id = $1 AND source = 'http_header' AND directive_id = $2
                                )
                                """,
                                url_id, dir_id
                            )
                        except Exception as e:
                            print(f"Warning: could not insert http_header directive '{directive}' for url_id {url_id}: {e}")

                # ---- Schema.org extraction (PostgreSQL) ----
                if content_item.get('schema_data'):
                    schema_rows.append((url_id, content_item['schema_data']))

                # ---- Canonical URL insertion (PostgreSQL) ----
                if content_item.get('canonical_url'):
                    try:
                        from .db import classify_url
                        from urllib.parse import urlparse
                        import time
                        
                        canonical_url = content_item['canonical_url']
                        
                        # Normalize protocol-relative URLs
                        normalized_canonical_url = canonical_url
                        if canonical_url.startswith('//'):
                            # Convert protocol-relative URL to use the same protocol as the base domain
                            if base_domain:
                                # Use the base domain's protocol (default to https)
                                base_protocol = 'https'
                                normalized_canonical_url = f"{base_protocol}:{canonical_url}"
                            else:
                                # Fallback to https
                                normalized_canonical_url = f"https:{canonical_url}"
                        
                        # Get or create canonical URL ID
                        canonical_url_row = await conn.fetchone(
                            "SELECT id FROM urls WHERE url = $1",
                            normalized_canonical_url
                        )
                        
                        if canonical_url_row:
                            canonical_url_id = canonical_url_row[0]
                        else:
                            # Create new URL entry for canonical URL
                            classification = classify_url(normalized_canonical_url, base_domain)
                            now = int(time.time())
                            canonical_url_insert_row = await conn.fetchone(
                                """
                                INSERT INTO urls (url, classification, first_seen, last_seen)
                                VALUES ($1, $2, $3, $4)
                                RETURNING id
                                """,
                                normalized_canonical_url, classification, now, now
                            )
                            canonical_url_id = canonical_url_insert_row[0] if canonical_url_insert_row else None
                            if not canonical_url_id:
                                continue  # Skip if we couldn't create the canonical URL
                        
                        # Insert canonical URL relationship (check if it already exists first)
                        existing = await conn.fetchone(
                            "SELECT id FROM canonical_urls WHERE url_id = $1 AND source = 'html_head'",
                            url_id
                        )
                        if not existing:
                            await conn.execute(
                                """
                                INSERT INTO canonical_urls(url_id, canonical_url_id, source)
                                VALUES ($1, $2, 'html_head')
                                """,
                                url_id, canonical_url_id
                            )
                    except Exception as e:
                        print(f"Warning: could not insert canonical URL for url_id {url_id}: {e}")

                # ---- Hreflang HTML head (PostgreSQL) ----
                if content_item.get('hreflang_urls'):
                    import time
                    from urllib.parse import urlparse
                    from .db import classify_url
                    
                    for hreflang_data in content_item['hreflang_urls']:
                        hreflang_url = hreflang_data.get('url')
                        hreflang_lang = hreflang_data.get('hreflang')
                        if not hreflang_url or not hreflang_lang:
                            continue
                        
                        normalized_hreflang_url = hreflang_url
                        if hreflang_url.startswith('//'):
                            if base_domain:
                                base_protocol = 'https'
                                normalized_hreflang_url = f"{base_protocol}:{hreflang_url}"
                            else:
                                normalized_hreflang_url = f"https:{hreflang_url}"
                        
                        hreflang_id = await get_or_create_hreflang_language_id_pg(hreflang_lang)
                        if hreflang_id is None:
                            continue
                        
                        # Get or create href URL ID
                        href_url_row = await conn.fetchone(
                            "SELECT id FROM urls WHERE url = $1",
                            normalized_hreflang_url
                        )
                        if href_url_row:
                            href_url_id = href_url_row[0]
                        else:
                            classification = classify_url(normalized_hreflang_url, base_domain, False, True)
                            now = int(time.time())
                            href_url_insert_row = await conn.fetchone(
                                """
                                INSERT INTO urls (url, classification, is_from_hreflang, kind, first_seen, last_seen)
                                VALUES ($1, $2, TRUE, 'other', $3, $3)
                                ON CONFLICT (url) DO UPDATE SET
                                    classification = EXCLUDED.classification,
                                    is_from_hreflang = EXCLUDED.is_from_hreflang
                                RETURNING id
                                """,
                                normalized_hreflang_url,
                                classification,
                                now
                            )
                            href_url_id = href_url_insert_row[0] if href_url_insert_row else None
                            if not href_url_id:
                                continue
                        
                        # Insert hreflang HTML head data if not already present
                        await conn.execute(
                            """
                            INSERT INTO hreflang_html_head (url_id, hreflang_id, href_url_id)
                            SELECT $1, $2, $3
                            WHERE NOT EXISTS (
                                SELECT 1 FROM hreflang_html_head 
                                WHERE url_id = $1 AND hreflang_id = $2 AND href_url_id = $3
                            )
                            """,
                            url_id, hreflang_id, href_url_id
                        )

                # ---- Indexability (PostgreSQL) ----
                try:
                    from .robots import is_url_crawlable

                    html_meta_directives = content_item.get('html_meta_directives', [])
                    http_header_directives = content_item.get('http_header_directives', [])

                    html_meta_allows = not any('noindex' in d.lower() for d in html_meta_directives)
                    http_header_allows = not any('noindex' in d.lower() for d in http_header_directives)

                    robots_txt_allows = is_url_crawlable(final_norm, "SQLiteCrawler/0.2")
                    robots_txt_directives = []
                    if not robots_txt_allows:
                        robots_txt_directives.append('disallow')
                    
                    # Store robots.txt directives in robots_directives table
                    from .robots import get_matching_robots_txt_rules
                    matching_rules = get_matching_robots_txt_rules(final_norm, "SQLiteCrawler/0.2")
                    for rule_type, rule_path in matching_rules:
                        try:
                            # Store the rule type (disallow/allow) as the directive
                            dir_id = await get_or_create_robots_directive_id_pg(rule_type)
                            await conn.execute(
                                """
                                INSERT INTO robots_directives (url_id, source, directive_id, value)
                                SELECT $1, 'robots_txt', $2, $3
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM robots_directives 
                                    WHERE url_id = $1 AND source = 'robots_txt' AND directive_id = $2 AND value = $3
                                )
                                """,
                                url_id, dir_id, rule_path
                            )
                        except Exception as e:
                            print(f"Warning: could not insert robots_txt directive '{rule_type}' (path: {rule_path}) for url_id {url_id}: {e}")

                    # Check canonical self
                    canonical_row = await conn.fetchone(
                        "SELECT canonical_url_id FROM canonical_urls WHERE url_id = $1",
                        url_id
                    )
                    is_self_canonical = canonical_row and canonical_row[0] == url_id

                    # Initial status - get from page_metadata
                    page_row = await conn.fetchone(
                        "SELECT initial_status_code FROM page_metadata WHERE url_id = $1",
                        url_id
                    )
                    initial_status_code = page_row[0] if page_row and page_row[0] is not None else None
                    
                    # If initial_status_code is None, we can't determine indexability accurately
                    # Set overall_indexable to False if we don't have status code
                    if initial_status_code is None:
                        overall_indexable = False
                    else:
                        overall_indexable = (
                            initial_status_code == 200 and
                            is_self_canonical and
                            robots_txt_allows and
                            html_meta_allows and
                            http_header_allows
                        )

                    # Always JSON encode arrays (parity with SQLite, avoid NULL)
                    robots_txt_directives_str = json.dumps(robots_txt_directives or [], ensure_ascii=False)
                    html_meta_directives_str = json.dumps(html_meta_directives or [], ensure_ascii=False)
                    http_header_directives_str = json.dumps(http_header_directives or [], ensure_ascii=False)
                    
                    indexability_rows.append((
                        url_id,
                        robots_txt_allows,
                        html_meta_allows,
                        http_header_allows,
                        robots_txt_directives_str,
                        html_meta_directives_str,
                        http_header_directives_str,
                        overall_indexable
                    ))
                    # Debug: log first few to verify values
                    if len(indexability_rows) <= 3:
                        print(f"  -> DEBUG indexability (content): url_id={url_id}, overall_indexable={overall_indexable} (type: {type(overall_indexable)}), initial_status={initial_status_code}, is_self_canonical={is_self_canonical}")
                except Exception as e:
                    # Best-effort; skip indexability on errors
                    print(f"Warning: could not calculate indexability for url_id {url_id} in content extraction: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Batch insert using executemany with chunking for better performance
            if batch_data:
                chunk_size = 500
                for i in range(0, len(batch_data), chunk_size):
                    chunk = batch_data[i:i + chunk_size]
                    await conn.executemany(query, chunk)

            # Upsert indexability rows
            if indexability_rows:
                print(f"  -> DEBUG: Upserting {len(indexability_rows)} indexability rows from content extraction...")
                indexability_query = """
                INSERT INTO indexability (
                    url_id, robots_txt_allows, html_meta_allows, http_header_allows,
                    robots_txt_directives, html_meta_directives, http_header_directives, overall_indexable
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (url_id) DO UPDATE SET
                    robots_txt_allows = EXCLUDED.robots_txt_allows,
                    html_meta_allows = EXCLUDED.html_meta_allows,
                    http_header_allows = EXCLUDED.http_header_allows,
                    robots_txt_directives = EXCLUDED.robots_txt_directives,
                    html_meta_directives = EXCLUDED.html_meta_directives,
                    http_header_directives = EXCLUDED.http_header_directives,
                    overall_indexable = EXCLUDED.overall_indexable
                """
                try:
                    await conn.executemany(indexability_query, indexability_rows)
                    print(f"  -> DEBUG: Successfully upserted {len(indexability_rows)} indexability rows")
                except Exception as e:
                    print(f"Warning: could not upsert indexability rows: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"  -> DEBUG: No indexability rows to upsert from content extraction")

            # Upsert schema data
            if schema_rows:
                try:
                    from .schema import identify_schema_relationships, create_schema_content_hash

                    async def get_or_create_schema_type_id(schema_type: str) -> int:
                        row = await conn.fetchone(
                            "SELECT id FROM schema_types WHERE type_name = $1",
                            schema_type
                        )
                        if row:
                            return row[0]
                        row = await conn.fetchone(
                            "INSERT INTO schema_types (type_name) VALUES ($1) RETURNING id",
                            schema_type
                        )
                        return row[0]

                    async def get_or_create_schema_instance(schema_data: dict) -> int:
                        content_hash = schema_data.get('content_hash') or create_schema_content_hash(
                            json.loads(schema_data.get('parsed_data') or '{}')
                        )
                        row = await conn.fetchone(
                            "SELECT id FROM schema_instances WHERE content_hash = $1",
                            content_hash
                        )
                        if row:
                            return row[0]
                        schema_type_id = await get_or_create_schema_type_id(schema_data['type'])
                        format_type = schema_data.get('format', 'json-ld')
                        if format_type not in ['json-ld', 'microdata', 'rdfa']:
                            format_type = 'json-ld'
                        row = await conn.fetchone(
                            """
                            INSERT INTO schema_instances
                            (content_hash, schema_type_id, format, raw_data, parsed_data, is_valid, validation_errors, severity, created_at)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                            RETURNING id
                            """,
                            content_hash,
                            schema_type_id,
                            format_type,
                            schema_data.get('raw_data', ''),
                            schema_data.get('parsed_data'),
                            schema_data.get('is_valid', True),
                            json.dumps(schema_data.get('validation_errors', [])),
                            schema_data.get('severity', 'info'),
                            datetime.datetime.now(datetime.timezone.utc)
                        )
                        return row[0]

                    async def create_page_schema_references_pg(url_id: int, schema_items: list[dict]):
                        relationships = identify_schema_relationships(schema_items)
                        main_entity = relationships['main_entity']
                        properties = relationships['properties']
                        related_entities = relationships['related_entities']
                        now_ts = int(time.time())

                        def parse_position(pos) -> int:
                            """Convert position to integer, handling string formats like '0-0' or '1-2'."""
                            if pos is None:
                                return 0
                            if isinstance(pos, int):
                                return pos
                            if isinstance(pos, str):
                                # Extract first number from strings like '0-0' or '1-2'
                                try:
                                    return int(pos.split('-')[0])
                                except (ValueError, AttributeError):
                                    return 0
                            return 0

                        async def insert_ref(schema_instance_id: int, position: int = 0, is_main: bool = False, property_name: str = None, parent_id: int = None):
                            await conn.execute(
                                """
                                INSERT INTO page_schema_references
                                (url_id, schema_instance_id, position, property_name, is_main_entity, parent_entity_id, discovered_at)
                                VALUES ($1,$2,$3,$4,$5,$6,$7)
                                """,
                                url_id, schema_instance_id, position, property_name, is_main, parent_id, now_ts
                            )

                        # Main entity
                        main_ref_id = None
                        if main_entity:
                            main_id = await get_or_create_schema_instance(main_entity)
                            await insert_ref(main_id, parse_position(main_entity.get('position', 0)), True, None, None)
                            row = await conn.fetchone("SELECT currval(pg_get_serial_sequence('page_schema_references','id'))")
                            main_ref_id = row[0] if row else None

                        # Properties
                        for prop in properties:
                            inst_id = await get_or_create_schema_instance(prop)
                            prop_position = parse_position(prop.get('position', 0))
                            prop_name = prop.get('type', '').lower() if prop.get('type') else None
                            await insert_ref(inst_id, prop_position, False, prop_name, main_ref_id)

                        # Related entities (siblings)
                        for rel in related_entities:
                            inst_id = await get_or_create_schema_instance(rel)
                            rel_position = parse_position(rel.get('position', 0))
                            await insert_ref(inst_id, rel_position, False, None, main_ref_id)

                    for url_id, schema_items in schema_rows:
                        try:
                            await create_page_schema_references_pg(url_id, schema_items)
                        except Exception as e:
                            print(f"Warning: could not upsert schema data for url_id {url_id}: {e}")

                except Exception as e:
                    print(f"Warning: schema upsert failed: {e}")
    else:
        # For SQLite, use the original implementation with the correct database path
        from .db import batch_write_content_with_url_resolution as sqlite_batch_write_content_with_url_resolution
        db_path = crawl_db_path if crawl_db_path else (config.sqlite_path if config and hasattr(config, 'sqlite_path') else None)
        if db_path:
            await sqlite_batch_write_content_with_url_resolution(content_data, db_path)
        else:
            await sqlite_batch_write_content_with_url_resolution(content_data)


async def get_or_create_meta_description_id(meta_description: str, config: DatabaseConfig = None) -> int:
    """Get or create meta description ID."""
    if config is None:
        config = get_database_config()
    
    async with create_connection() as conn:
        # Try to get existing ID
        result = await conn.fetchone("SELECT id FROM meta_descriptions WHERE description = $1", meta_description)
        if result:
            return result[0]
        
        # Create new meta description
        result = await conn.fetchone("INSERT INTO meta_descriptions (description) VALUES ($1) RETURNING id", meta_description)
        return result[0]


async def get_or_create_html_language_id(html_lang: str, config: DatabaseConfig = None) -> int:
    """Get or create HTML language ID."""
    if config is None:
        config = get_database_config()
    
    async with create_connection() as conn:
        # Try to get existing ID
        result = await conn.fetchone("SELECT id FROM html_languages WHERE language_code = $1", html_lang)
        if result:
            return result[0]
        
        # Create new HTML language
        result = await conn.fetchone("INSERT INTO html_languages (language_code) VALUES ($1) RETURNING id", html_lang)
        return result[0]


async def batch_write_internal_links(links_data: List[Dict], crawl_db_path: str = None, config: DatabaseConfig = None):
    """Write internal links data to the database."""
    if config is None:
        config = get_database_config()
    
    if not links_data:
        return
    
    if config.backend == "postgresql":
        query = """
        INSERT INTO internal_links (
            source_url_id, target_url_id, anchor_text_id, xpath_id,
            href_url_id, fragment_id, url_parameters, discovered_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """
        
        async with create_connection() as conn:
            # Collect all URLs that need ID resolution
            # IMPORTANT: Normalize all URLs before resolving to avoid duplicates
            from .parse import normalize_url_hardened
            urls_to_resolve = []
            for link_tuple in links_data:
                original_norm, detailed_links, base_domain = link_tuple
                urls_to_resolve.append(original_norm)
                for link_item in detailed_links:
                    if link_item.get('url'):
                        urls_to_resolve.append(link_item['url'])
                    # Normalize original_href to avoid creating duplicate URL records
                    # (original_href may have fragments, different protocols, etc.)
                    normalized_original_href = normalize_url_hardened(link_item['original_href'])
                    urls_to_resolve.append(normalized_original_href)
            
            # Remove duplicates from urls_to_resolve to avoid redundant lookups
            urls_to_resolve = list(dict.fromkeys(urls_to_resolve))  # Preserves order
            
            # Batch resolve all URL IDs at once
            url_to_id_map = await batch_get_or_create_url_ids(urls_to_resolve, base_domain, config, conn)
            
            # Prepare batch data and track link counts per source URL
            batch_data = []
            # Use a set to track unique links and prevent duplicates within this batch
            seen_links = set()
            current_time = int(time.time())
            # Track link counts per source_url_id: {source_url_id: {'internal': count, 'external': count, 'internal_unique': set, 'external_unique': set}}
            link_counts = {}
            
            from .db import classify_url
            
            for link_tuple in links_data:
                # link_tuple format: (original_norm, detailed_links, base_domain)
                original_norm, detailed_links, base_domain = link_tuple
                
                # Get source URL ID
                source_url_id = url_to_id_map.get(original_norm, 0)
                if not source_url_id:
                    continue
                
                # Initialize counts for this source URL
                if source_url_id not in link_counts:
                    link_counts[source_url_id] = {
                        'internal': 0,
                        'external': 0,
                        'internal_unique': set(),
                        'external_unique': set()
                    }
                
                # Process each detailed link
                for link_item in detailed_links:
                    # Get URL IDs from the map
                    target_url_id = url_to_id_map.get(link_item['url']) if link_item.get('url') else None
                    # Use normalized original_href to get href_url_id (matches what we added to urls_to_resolve)
                    normalized_original_href = normalize_url_hardened(link_item['original_href'])
                    href_url_id = url_to_id_map.get(normalized_original_href, 0)
                    
                    if not href_url_id:
                        continue  # Skip if essential IDs not found
                    
                    # Get or create anchor text ID if provided (still individual, but less frequent)
                    anchor_text_id = None
                    if link_item.get('anchor_text'):
                        anchor_text_id = await get_or_create_anchor_text_id(link_item['anchor_text'], config)
                    
                    # Get or create xpath ID if provided (still individual, but less frequent)
                    xpath_id = None
                    if link_item.get('xpath'):
                        xpath_id = await get_or_create_xpath_id(link_item['xpath'], config)
                    
                    # Get or create fragment ID if provided (still individual, but less frequent)
                    fragment_id = None
                    if link_item.get('fragment'):
                        fragment_id = await get_or_create_fragment_id(link_item['fragment'], config)
                    
                    # Create a unique key for this link to prevent duplicates within this batch
                    link_key = (
                        source_url_id,
                        target_url_id,
                        anchor_text_id,
                        xpath_id,
                        href_url_id,
                        fragment_id,
                        link_item.get('url_parameters')
                    )
                    
                    # Skip if we've already seen this exact link in this batch
                    if link_key in seen_links:
                        continue
                    seen_links.add(link_key)
                    
                    # Get target URL for classification
                    target_url = link_item.get('url')
                    if target_url:
                        # Classify the link using normalized URL
                        classification = classify_url(target_url, base_domain)
                        
                        # Count internal vs external for statistics
                        if classification == 'internal':
                            link_counts[source_url_id]['internal'] += 1
                            link_counts[source_url_id]['internal_unique'].add(target_url)
                        else:
                            link_counts[source_url_id]['external'] += 1
                            link_counts[source_url_id]['external_unique'].add(target_url)
                    
                    batch_data.append((
                        source_url_id,
                        target_url_id,
                        anchor_text_id,
                        xpath_id,
                        href_url_id,
                        fragment_id,
                        link_item.get('url_parameters'),
                        current_time
                    ))
            
            # Batch insert using executemany with chunking for better performance
            if batch_data:
                chunk_size = 500
                for i in range(0, len(batch_data), chunk_size):
                    chunk = batch_data[i:i + chunk_size]
                    await conn.executemany(query, chunk)
            
            # Update content table with link counts
            if link_counts:
                for source_url_id, counts in link_counts.items():
                    await conn.execute(
                        """
                        UPDATE content 
                        SET internal_links_count = $1, 
                            external_links_count = $2,
                            internal_links_unique_count = $3,
                            external_links_unique_count = $4
                        WHERE url_id = $5
                        """,
                        counts['internal'],
                        counts['external'],
                        len(counts['internal_unique']),
                        len(counts['external_unique']),
                        source_url_id
                    )
    else:
        # For SQLite, use the original implementation with the correct database path
        from .db import batch_write_internal_links as sqlite_batch_write_internal_links
        db_path = crawl_db_path if crawl_db_path else (config.sqlite_path if config and hasattr(config, 'sqlite_path') else None)
        if db_path:
            await sqlite_batch_write_internal_links(links_data, db_path)
        else:
            await sqlite_batch_write_internal_links(links_data)


async def get_or_create_anchor_text_id(anchor_text: str, config: DatabaseConfig = None) -> int:
    """Get or create anchor text ID.
    
    For PostgreSQL, anchor texts are truncated to 2000 bytes to avoid exceeding
    the btree index size limit (2704 bytes).
    """
    if config is None:
        config = get_database_config()
    
    # Truncate anchor text if it's too long (PostgreSQL btree index limit is 2704 bytes)
    # Use 2000 bytes to leave headroom for encoding overhead
    MAX_ANCHOR_TEXT_BYTES = 2000
    if len(anchor_text.encode('utf-8')) > MAX_ANCHOR_TEXT_BYTES:
        # Truncate to fit within byte limit, preserving UTF-8 encoding
        encoded = anchor_text.encode('utf-8')
        truncated = encoded[:MAX_ANCHOR_TEXT_BYTES]
        # Decode back, handling potential incomplete UTF-8 sequences at the end
        anchor_text = truncated.decode('utf-8', errors='ignore')
        # Remove any trailing incomplete characters
        anchor_text = anchor_text.rstrip('\ufffd')
    
    async with create_connection() as conn:
        # Try to get existing ID
        result = await conn.fetchone("SELECT id FROM anchor_texts WHERE text = $1", anchor_text)
        if result:
            return result[0]
        
        # Create new anchor text
        result = await conn.fetchone("INSERT INTO anchor_texts (text) VALUES ($1) RETURNING id", anchor_text)
        return result[0]


async def get_or_create_xpath_id(xpath: str, config: DatabaseConfig = None) -> int:
    """Get or create xpath ID."""
    if config is None:
        config = get_database_config()
    
    async with create_connection() as conn:
        # Try to get existing ID
        result = await conn.fetchone("SELECT id FROM xpaths WHERE xpath = $1", xpath)
        if result:
            return result[0]
        
        # Create new xpath
        result = await conn.fetchone("INSERT INTO xpaths (xpath) VALUES ($1) RETURNING id", xpath)
        return result[0]


async def get_or_create_fragment_id(fragment: str, config: DatabaseConfig = None) -> int:
    """Get or create fragment ID."""
    if config is None:
        config = get_database_config()
    
    async with create_connection() as conn:
        # Try to get existing ID
        result = await conn.fetchone("SELECT id FROM fragments WHERE fragment = $1", fragment)
        if result:
            return result[0]
        
        # Create new fragment
        result = await conn.fetchone("INSERT INTO fragments (fragment) VALUES ($1) RETURNING id", fragment)
        return result[0]


async def batch_write_redirects(redirect_data: List[Tuple[str, str, str, int, int]], crawl_db_path: str = None, config: DatabaseConfig = None):
    """Write redirect chain data to the database using the abstraction layer."""
    if not redirect_data:
        return
    
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        async with create_connection() as conn:
            now = int(time.time())
            
            # Collect all URLs that need ID resolution
            urls_to_resolve = []
            for source_url, target_url, redirect_chain_json, chain_length, final_status in redirect_data:
                urls_to_resolve.append(source_url)
                urls_to_resolve.append(target_url)
            
            # Extract base domain from the first source URL (redirects are typically within the same domain)
            from urllib.parse import urlparse
            first_base_domain = ""
            if redirect_data:
                first_source_url = redirect_data[0][0]
                parsed = urlparse(first_source_url)
                first_base_domain = parsed.netloc
            
            # Batch resolve all URL IDs at once
            url_to_id_map = await batch_get_or_create_url_ids(urls_to_resolve, first_base_domain, config, conn)
            
            # Prepare batch data for redirects
            redirect_batch_data = []
            for source_url, target_url, redirect_chain_json, chain_length, final_status in redirect_data:
                source_url_id = url_to_id_map.get(source_url)
                target_url_id = url_to_id_map.get(target_url)
                
                if source_url_id and target_url_id:
                    redirect_batch_data.append((
                        source_url_id,
                        target_url_id,
                        redirect_chain_json,
                        chain_length,
                        final_status
                    ))
            
            # Batch insert redirects with chunking and retry logic
            if redirect_batch_data:
                # Sort by source_url_id to ensure consistent lock ordering
                redirect_batch_data.sort(key=lambda x: x[0])
                
                query = """
                INSERT INTO redirects(source_url_id, target_url_id, redirect_chain, chain_length, final_status_code)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (source_url_id) DO UPDATE SET
                    target_url_id = EXCLUDED.target_url_id,
                    redirect_chain = EXCLUDED.redirect_chain,
                    chain_length = EXCLUDED.chain_length,
                    final_status_code = EXCLUDED.final_status_code
                """
                
                chunk_size = 500
                max_retries = 3
                retry_delay = 0.1
                
                for i in range(0, len(redirect_batch_data), chunk_size):
                    chunk = redirect_batch_data[i:i + chunk_size]
                    
                    # Retry logic for deadlock errors
                    for attempt in range(max_retries):
                        try:
                            await conn.executemany(query, chunk)
                            break  # Success, exit retry loop
                        except Exception as e:
                            error_msg = str(e).lower()
                            if "deadlock" in error_msg and attempt < max_retries - 1:
                                # Exponential backoff with jitter
                                delay = retry_delay * (2 ** attempt) + random.uniform(0, 0.05)
                                await asyncio.sleep(delay)
                                continue
                            else:
                                # Re-raise if not a deadlock or out of retries
                                raise
    else:
        # SQLite implementation - use the original function
        from .db import batch_write_redirects as sqlite_batch_write_redirects
        if crawl_db_path:
            await sqlite_batch_write_redirects(redirect_data, crawl_db_path)
        else:
            # Fallback to default path
            from .config import get_db_paths
            _, crawl_path = get_db_paths()
            await sqlite_batch_write_redirects(redirect_data, crawl_path)


async def batch_write_sitemaps_and_urls(sitemap_data: List[Tuple[str, List[Tuple[str, int]]]], crawl_db_path: str = None, config: DatabaseConfig = None, base_domain: str = ""):
    """Write sitemap records and URL-sitemap relationships.
    
    Supports both PostgreSQL and SQLite backends.
    
    Args:
        sitemap_data: List of tuples (sitemap_url, [(url, position), ...])
        crawl_db_path: Database path (for SQLite only)
        config: Database configuration
        base_domain: Base domain for URL classification (optional)
    """
    if config is None:
        config = get_database_config()
    
    if not sitemap_data:
        return
    
    if config.backend == "postgresql":
        # PostgreSQL implementation
        # Use None for timestamp columns to let PostgreSQL use DEFAULT CURRENT_TIMESTAMP
        async with create_connection() as conn:
            for sitemap_url, url_positions in sitemap_data:
                # Insert or get sitemap ID
                # Use to_timestamp() to convert Unix timestamp to PostgreSQL timestamp
                sitemap_query = """
                INSERT INTO sitemaps (sitemap_url, discovered_at, last_crawled_at, total_urls_found)
                VALUES ($1, to_timestamp($2), to_timestamp($2), $3)
                ON CONFLICT (sitemap_url) DO UPDATE SET
                    last_crawled_at = EXCLUDED.last_crawled_at,
                    total_urls_found = EXCLUDED.total_urls_found
                RETURNING id
                """
                now = int(time.time())
                result = await conn.fetchone(
                    sitemap_query,
                    sitemap_url, now, len(url_positions)
                )
                
                if not result:
                    # If no result, try to get existing ID
                    result = await conn.fetchone(
                        "SELECT id FROM sitemaps WHERE sitemap_url = $1",
                        sitemap_url
                    )
                
                if not result:
                    continue
                    
                sitemap_id = result[0]
                
                # Batch get or create all URL IDs at once (handles missing URLs automatically)
                urls_list = [url for url, _ in url_positions]
                if urls_list:
                    if len(urls_list) > 100:
                        print(f"  Resolving {len(urls_list)} URL IDs for sitemap relationships...")
                    # Use batch_get_or_create_url_ids to ensure all URLs exist and get their IDs
                    url_to_id_map = await batch_get_or_create_url_ids(urls_list, base_domain, config, conn)
                    
                    # Prepare batch data for URL-sitemap relationships
                    relationship_data = []
                    for url, position in url_positions:
                        url_id = url_to_id_map.get(url)
                        if url_id:
                            relationship_data.append((url_id, sitemap_id, position, now))
                    
                    # Batch insert URL-sitemap relationships with chunking
                    if relationship_data:
                        if len(relationship_data) > 100:
                            print(f"  Inserting {len(relationship_data)} URL-sitemap relationships...")
                        chunk_size = 500
                        for i in range(0, len(relationship_data), chunk_size):
                            chunk = relationship_data[i:i + chunk_size]
                            await conn.executemany(
                                """
                                INSERT INTO url_sitemaps(url_id, sitemap_id, position, discovered_at)
                                VALUES ($1, $2, $3, to_timestamp($4))
                                ON CONFLICT (url_id, sitemap_id) DO NOTHING
                                """,
                                chunk
                            )
    else:
        # SQLite implementation - use the original function
        from .db import batch_write_sitemaps_and_urls as sqlite_batch_write_sitemaps_and_urls
        db_path = crawl_db_path if crawl_db_path else (config.sqlite_path if config and hasattr(config, 'sqlite_path') else None)
        if db_path:
            await sqlite_batch_write_sitemaps_and_urls(sitemap_data, db_path)
        else:
            # Fallback to default path
            from .config import CRAWL_DB_PATH
            await sqlite_batch_write_sitemaps_and_urls(sitemap_data, CRAWL_DB_PATH)


async def add_hreflang_urls_to_frontier(crawl_db_path: str = None, base_domain: str = "", config: DatabaseConfig = None):
    """Add hreflang URLs to the frontier for crawling.
    
    Supports both PostgreSQL and SQLite backends.
    
    Args:
        crawl_db_path: Database path (for SQLite only)
        base_domain: Base domain for URL classification
        config: Database configuration
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        # PostgreSQL implementation
        async with create_connection() as conn:
            # Get all hreflang URLs from both HTML head and sitemap that are not already in the frontier
            query = """
                SELECT DISTINCT u.url 
                FROM (
                    SELECT href_url_id FROM hreflang_html_head
                    UNION
                    SELECT href_url_id FROM hreflang_sitemap
                ) hreflang_urls
                JOIN urls u ON hreflang_urls.href_url_id = u.id
                WHERE u.id NOT IN (SELECT url_id FROM frontier)
            """
            result = await conn.fetchall(query)
            
            if result:
                hreflang_urls = [row[0] for row in result]
                print(f"Adding {len(hreflang_urls)} hreflang URLs to frontier...")
                for url in hreflang_urls:
                    await frontier_seed(url, base_domain, reset=False, config=config, depth=0)
    else:
        # SQLite implementation - use the original function
        from .db import add_hreflang_urls_to_frontier as sqlite_add_hreflang_urls_to_frontier
        db_path = crawl_db_path if crawl_db_path else (config.sqlite_path if config and hasattr(config, 'sqlite_path') else None)
        if db_path:
            await sqlite_add_hreflang_urls_to_frontier(db_path, base_domain)
        else:
            # Fallback to default path
            from .config import CRAWL_DB_PATH
            await sqlite_add_hreflang_urls_to_frontier(CRAWL_DB_PATH, base_domain)


async def backfill_missing_frontier_entries(base_domain: str, config: DatabaseConfig = None):
    """Add URLs to frontier that are in urls table but not in frontier (unknown status).
    
    This ensures all internal/network URLs in the urls table have a frontier entry.
    
    Args:
        base_domain: Base domain for URL classification
        config: Database configuration
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        async with create_connection() as conn:
            # Find all internal/network URLs that are in urls table but not in frontier
            # Also get their classification and kind to help debug why they weren't enqueued
            query = """
                SELECT u.id, u.url, u.classification, u.kind, 
                       CASE WHEN cu.canonical_url_id = u.id THEN TRUE ELSE FALSE END as is_canonical_target,
                       CASE WHEN hh.href_url_id = u.id THEN TRUE ELSE FALSE END as is_hreflang_target,
                       CASE WHEN r_source.source_url_id = u.id THEN TRUE ELSE FALSE END as is_redirect_source,
                       CASE WHEN r_target.target_url_id = u.id THEN TRUE ELSE FALSE END as is_redirect_target
                FROM urls u
                LEFT JOIN canonical_urls cu ON u.id = cu.canonical_url_id
                LEFT JOIN hreflang_html_head hh ON u.id = hh.href_url_id
                LEFT JOIN redirects r_source ON u.id = r_source.source_url_id
                LEFT JOIN redirects r_target ON u.id = r_target.target_url_id
                WHERE u.classification IN ('internal', 'network')
                  AND u.id NOT IN (SELECT url_id FROM frontier)
                ORDER BY u.id
            """
            result = await conn.fetchall(query)
            
            if result:
                missing_urls = [(row[0], row[1]) for row in result]
                print(f"Found {len(missing_urls)} URLs in urls table but not in frontier - adding to frontier...")
                
                # Debug: Show first few URLs and why they might have been missed
                if len(result) <= 10:
                    print("  Debug - Missing URLs:")
                    for row in result:
                        url_id, url, classification, kind, is_canonical_target, is_hreflang_target, is_redirect_source, is_redirect_target = row
                        reasons = []
                        if is_canonical_target:
                            reasons.append("canonical_target")
                        if is_hreflang_target:
                            reasons.append("hreflang_target")
                        if is_redirect_source:
                            reasons.append("redirect_source")
                        if is_redirect_target:
                            reasons.append("redirect_target")
                        reason_str = f" ({', '.join(reasons)})" if reasons else ""
                        print(f"    - {url} [classification: {classification}, kind: {kind}{reason_str}]")
                elif len(result) > 10:
                    # Show sample
                    print("  Debug - Sample of missing URLs:")
                    for row in result[:5]:
                        url_id, url, classification, kind, is_canonical_target, is_hreflang_target, is_redirect_source, is_redirect_target = row
                        reasons = []
                        if is_canonical_target:
                            reasons.append("canonical_target")
                        if is_hreflang_target:
                            reasons.append("hreflang_target")
                        if is_redirect_source:
                            reasons.append("redirect_source")
                        if is_redirect_target:
                            reasons.append("redirect_target")
                        reason_str = f" ({', '.join(reasons)})" if reasons else ""
                        print(f"    - {url} [classification: {classification}, kind: {kind}{reason_str}]")
                    print(f"    ... and {len(result) - 5} more")
                
                # Add them to frontier in batches
                for url_id, url in missing_urls:
                    await frontier_seed(url, base_domain, reset=False, config=config, depth=0)
                
                print(f"Added {len(missing_urls)} missing URLs to frontier")
    else:
        # SQLite - similar approach
        from .config import CRAWL_DB_PATH
        async with create_connection() as conn:
            query = """
                SELECT u.id, u.url
                FROM urls u
                WHERE u.classification IN ('internal', 'network')
                  AND u.id NOT IN (SELECT url_id FROM frontier)
            """
            result = await conn.fetchall(query)
            
            if result:
                missing_urls = [(row[0], row[1]) for row in result]
                print(f"Found {len(missing_urls)} URLs in urls table but not in frontier - adding to frontier...")
                
                for url_id, url in missing_urls:
                    await frontier_seed(url, base_domain, reset=False, config=config, depth=0)
                
                print(f"Added {len(missing_urls)} missing URLs to frontier")


async def remove_failed_url(url_id: int, config: DatabaseConfig = None):
    """Remove a URL from the failed_urls table (when it succeeds).
    
    Supports both PostgreSQL and SQLite backends.
    
    Args:
        url_id: The URL ID to remove from failed_urls
        config: Database configuration
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        async with create_connection() as conn:
            await conn.execute("DELETE FROM failed_urls WHERE url_id = $1", url_id)
    else:
        # SQLite implementation - use the original function
        from .db import remove_failed_url as sqlite_remove_failed_url
        db_path = config.sqlite_path if config and hasattr(config, 'sqlite_path') else None
        if db_path:
            async with create_connection() as conn:
                # For SQLite, we need to pass the connection directly
                # But create_connection() returns our abstraction, so we need to handle this differently
                # Actually, let's just use aiosqlite directly for SQLite here
                import aiosqlite
                async with aiosqlite.connect(db_path) as sqlite_conn:
                    await sqlite_remove_failed_url(url_id, sqlite_conn)
        else:
            from .config import CRAWL_DB_PATH
            import aiosqlite
            async with aiosqlite.connect(CRAWL_DB_PATH) as sqlite_conn:
                await sqlite_remove_failed_url(url_id, sqlite_conn)


async def get_urls_ready_for_retry(max_retries: int = 3, config: DatabaseConfig = None) -> list[tuple[int, str]]:
    """Get URLs that are ready for retry (next_retry_at <= now and retry_count < max_retries).
    
    Supports both PostgreSQL and SQLite backends.
    
    Args:
        max_retries: Maximum number of retries allowed
        config: Database configuration
    
    Returns:
        List of tuples (url_id, url) ready for retry
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        # PostgreSQL implementation
        import time
        now = int(time.time())
        
        async with create_connection() as conn:
            query = """
                SELECT fu.url_id, u.url 
                FROM failed_urls fu
                JOIN urls u ON fu.url_id = u.id
                WHERE fu.next_retry_at <= to_timestamp($1) AND fu.retry_count < $2
                ORDER BY fu.next_retry_at ASC
            """
            result = await conn.fetchall(query, now, max_retries)
            return [(row[0], row[1]) for row in result] if result else []
    else:
        # SQLite implementation - use the original function
        from .db import get_urls_ready_for_retry as sqlite_get_urls_ready_for_retry
        db_path = config.sqlite_path if config and hasattr(config, 'sqlite_path') else None
        if db_path:
            import aiosqlite
            async with aiosqlite.connect(db_path) as sqlite_conn:
                return await sqlite_get_urls_ready_for_retry(sqlite_conn, max_retries)
        else:
            from .config import CRAWL_DB_PATH
            import aiosqlite
            async with aiosqlite.connect(CRAWL_DB_PATH) as sqlite_conn:
                return await sqlite_get_urls_ready_for_retry(sqlite_conn, max_retries)


async def get_retry_statistics(config: DatabaseConfig = None) -> dict:
    """Get comprehensive retry statistics.
    
    Supports both PostgreSQL and SQLite backends.
    
    Args:
        config: Database configuration
    
    Returns:
        Dictionary with retry statistics
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        # PostgreSQL implementation
        import time
        stats = {}
        
        async with create_connection() as conn:
            # Total failed URLs
            result = await conn.fetchone("SELECT COUNT(*) FROM failed_urls")
            stats['total_failed'] = result[0] if result else 0
            
            # Failed URLs by status code
            result = await conn.fetchall(
                "SELECT status_code, COUNT(*) FROM failed_urls GROUP BY status_code ORDER BY status_code"
            )
            stats['by_status'] = {row[0]: row[1] for row in result} if result else {}
            
            # Failed URLs by retry count
            result = await conn.fetchall(
                "SELECT retry_count, COUNT(*) FROM failed_urls GROUP BY retry_count ORDER BY retry_count"
            )
            stats['by_retry_count'] = {row[0]: row[1] for row in result} if result else {}
            
            # URLs ready for retry
            now = int(time.time())
            result = await conn.fetchone(
                "SELECT COUNT(*) FROM failed_urls WHERE next_retry_at <= to_timestamp($1)",
                now
            )
            stats['ready_for_retry'] = result[0] if result else 0
            
            return stats
    else:
        # SQLite implementation - use the original function
        from .db import get_retry_statistics as sqlite_get_retry_statistics
        db_path = config.sqlite_path if config and hasattr(config, 'sqlite_path') else None
        if db_path:
            import aiosqlite
            async with aiosqlite.connect(db_path) as sqlite_conn:
                return await sqlite_get_retry_statistics(sqlite_conn)
        else:
            from .config import CRAWL_DB_PATH
            import aiosqlite
            async with aiosqlite.connect(CRAWL_DB_PATH) as sqlite_conn:
                return await sqlite_get_retry_statistics(sqlite_conn)


async def frontier_update_priority_scores(config: DatabaseConfig = None):
    """Update priority scores for all queued URLs based on current inlinks count.
    
    Supports both PostgreSQL and SQLite backends.
    
    Args:
        config: Database configuration
    """
    if config is None:
        config = get_database_config()
    
    if config.backend == "postgresql":
        async with create_connection() as conn:
            # Get inlinks count for each URL
            query = """
                SELECT f.url_id, f.depth, f.sitemap_priority, f.content_type_score, u.url,
                       COALESCE(COUNT(il.target_url_id), 0) as inlinks_count
                FROM frontier f
                JOIN urls u ON f.url_id = u.id
                LEFT JOIN internal_links il ON f.url_id = il.target_url_id
                WHERE f.status = 'queued'
                GROUP BY f.url_id, f.depth, f.sitemap_priority, f.content_type_score, u.url
            """
            rows = await conn.fetchall(query)
            
            if not rows:
                return
            
            # Import calculate_priority_score function
            from .db import calculate_priority_score
            
            # Prepare batch updates
            update_data = []
            for row in rows:
                url_id, depth, sitemap_priority, content_type_score, url, inlinks_count = row
                priority_score = calculate_priority_score(url, depth, sitemap_priority, inlinks_count)
                update_data.append((priority_score, inlinks_count, url_id))
            
            # Batch update priority scores
            update_query = """
                UPDATE frontier 
                SET priority_score = $1, inlinks_count = $2
                WHERE url_id = $3
            """
            
            # Use executemany for batch updates
            chunk_size = 500
            for i in range(0, len(update_data), chunk_size):
                chunk = update_data[i:i + chunk_size]
                await conn.executemany(update_query, chunk)
    elif config.backend == "sqlite":
        # SQLite implementation - only use if backend is explicitly SQLite
        from .db import frontier_update_priority_scores as sqlite_frontier_update_priority_scores
        # Only use SQLite if we have a valid SQLite path
        if config and hasattr(config, 'sqlite_path') and config.sqlite_path:
            await sqlite_frontier_update_priority_scores(config.sqlite_path)
        else:
            # Try to get SQLite path from config
            from .config import get_db_paths, CRAWL_DB_PATH
            try:
                # Try to get paths from config if available
                _, crawl_path = get_db_paths()
                if crawl_path and os.path.exists(crawl_path):
                    await sqlite_frontier_update_priority_scores(crawl_path)
                elif os.path.exists(CRAWL_DB_PATH):
                    await sqlite_frontier_update_priority_scores(CRAWL_DB_PATH)
                else:
                    # No valid SQLite database found, skip silently
                    return
            except Exception:
                # If we can't determine the SQLite path, skip silently
                return
    else:
        # Unknown backend, skip silently
        return
