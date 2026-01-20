#!/usr/bin/env python3
"""
Generate OpenAI embeddings from stored HTML page content.

This script:
1. Connects to a crawl database (SQLite or PostgreSQL)
2. Retrieves HTML from the pages table
3. Extracts text content (similar to document.body.innerText)
4. Generates embeddings using OpenAI API
5. Stores embeddings in a new table

Usage:
    # SQLite
    python generate_embeddings.py --db-path data/example_crawl.db --api-key YOUR_API_KEY

    # PostgreSQL
    python generate_embeddings.py --postgres-host localhost --postgres-db crawler_db \
        --postgres-user sql_crawler --postgres-password PASSWORD --api-key YOUR_API_KEY

    # Process specific URLs only
    python generate_embeddings.py --db-path data/example_crawl.db --api-key YOUR_API_KEY \
        --urls https://example.com/page1 https://example.com/page2

    # Batch processing with rate limiting
    python generate_embeddings.py --db-path data/example_crawl.db --api-key YOUR_API_KEY \
        --batch-size 10 --delay 1.0
"""

import argparse
import asyncio
import base64
import json
import os
import sys
import time
import zlib
from typing import List, Optional, Tuple
from urllib.parse import urlparse

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    import aiosqlite
    import asyncpg
    from bs4 import BeautifulSoup
    import requests
except ImportError as e:
    print(f"Error: Missing required package. Install with: pip install aiosqlite asyncpg beautifulsoup4 requests")
    print(f"Missing: {e.name}")
    sys.exit(1)


# ============================================================================
# Database Connection & HTML Extraction
# ============================================================================

def decompress_html(encoded: bytes) -> str:
    """Decompress HTML from bytes to string."""
    try:
        return zlib.decompress(base64.b64decode(encoded)).decode("utf-8")
    except Exception as e:
        print(f"Error decompressing HTML: {e}")
        return ""


def extract_text_from_html(html: str) -> str:
    """
    Extract text content from HTML (similar to document.body.innerText).
    Removes script and style tags, then extracts visible text.
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script and style elements
        for element in soup(["script", "style", "noscript"]):
            element.decompose()
        
        # Get text content
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean up multiple whitespace
        import re
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    except Exception as e:
        print(f"Error extracting text from HTML: {e}")
        return ""


async def get_pages_sqlite(db_path: str, urls: Optional[List[str]] = None) -> List[Tuple[int, str, str]]:
    """
    Get pages from SQLite database.
    Returns list of (url_id, url, html_text) tuples.
    """
    pages = []
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found: {db_path}")
        return pages
    
    try:
        async with aiosqlite.connect(db_path) as db:
            # Get pages with their URLs
            if urls:
                # Filter by specific URLs
                placeholders = ','.join('?' * len(urls))
                query = f"""
                    SELECT p.url_id, u.url, p.html_compressed
                    FROM pages p
                    JOIN urls u ON p.url_id = u.id
                    WHERE u.url IN ({placeholders})
                """
                async with db.execute(query, urls) as cursor:
                    rows = await cursor.fetchall()
            else:
                # Get all pages
                query = """
                    SELECT p.url_id, u.url, p.html_compressed
                    FROM pages p
                    JOIN urls u ON p.url_id = u.id
                    WHERE p.html_compressed IS NOT NULL
                """
                async with db.execute(query) as cursor:
                    rows = await cursor.fetchall()
            
            for row in rows:
                url_id, url, html_compressed = row
                if html_compressed:
                    html = decompress_html(html_compressed)
                    text = extract_text_from_html(html)
                    if text:
                        pages.append((url_id, url, text))
    
    except Exception as e:
        print(f"Error reading SQLite database: {e}")
    
    return pages


async def get_pages_postgresql(
    host: str,
    database: str,
    user: str,
    password: str,
    schema: str = "public",
    urls: Optional[List[str]] = None
) -> List[Tuple[int, str, str]]:
    """
    Get pages from PostgreSQL database.
    Returns list of (url_id, url, html_text) tuples.
    """
    pages = []
    
    try:
        conn = await asyncpg.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        # Set search path to schema
        await conn.execute(f"SET search_path TO {schema}, public")
        
        if urls:
            # Filter by specific URLs
            query = """
                SELECT p.url_id, u.url, p.html_compressed
                FROM pages p
                JOIN urls u ON p.url_id = u.id
                WHERE u.url = ANY($1::text[])
            """
            rows = await conn.fetch(query, urls)
        else:
            # Get all pages
            query = """
                SELECT p.url_id, u.url, p.html_compressed
                FROM pages p
                JOIN urls u ON p.url_id = u.id
                WHERE p.html_compressed IS NOT NULL
            """
            rows = await conn.fetch(query)
        
        for row in rows:
            url_id = row['url_id']
            url = row['url']
            html_compressed = row['html_compressed']
            
            if html_compressed:
                # PostgreSQL stores as BYTEA, which is already bytes
                html = decompress_html(html_compressed)
                text = extract_text_from_html(html)
                if text:
                    pages.append((url_id, url, text))
        
        await conn.close()
    
    except Exception as e:
        print(f"Error reading PostgreSQL database: {e}")
    
    return pages


# ============================================================================
# Embeddings Generation
# ============================================================================

def generate_embedding(text: str, api_key: str, model: str = "text-embedding-3-small") -> Optional[List[float]]:
    """
    Generate embedding for text using OpenAI API.
    
    Args:
        text: Text content to embed
        api_key: OpenAI API key
        model: Embedding model to use (default: text-embedding-3-small)
    
    Returns:
        List of floats representing the embedding, or None on error
    """
    url = "https://api.openai.com/v1/embeddings"
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    data = {
        "model": model,
        "input": text,
        "encoding_format": "float",
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        response.raise_for_status()
        
        result = response.json()
        if result.get("data") and len(result["data"]) > 0:
            return result["data"][0]["embedding"]
        else:
            print(f"Warning: No embedding data in response")
            return None
    
    except requests.exceptions.RequestException as e:
        print(f"Error calling OpenAI API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.json()
                print(f"Error details: {error_detail}")
            except:
                print(f"Response text: {e.response.text}")
        return None


# ============================================================================
# Database Storage
# ============================================================================

async def create_embeddings_table_sqlite(db_path: str):
    """Create embeddings table in SQLite database if it doesn't exist."""
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS page_embeddings (
                    url_id INTEGER PRIMARY KEY,
                    embedding_json TEXT NOT NULL,
                    model TEXT NOT NULL,
                    text_length INTEGER,
                    created_at INTEGER DEFAULT (strftime('%s', 'now')),
                    FOREIGN KEY (url_id) REFERENCES urls (id)
                )
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_page_embeddings_model 
                ON page_embeddings(model)
            """)
            await db.commit()
    except Exception as e:
        print(f"Error creating embeddings table: {e}")


async def create_embeddings_table_postgresql(
    host: str,
    database: str,
    user: str,
    password: str,
    schema: str = "public"
):
    """Create embeddings table in PostgreSQL database if it doesn't exist."""
    try:
        conn = await asyncpg.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        await conn.execute(f"SET search_path TO {schema}, public")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS page_embeddings (
                url_id INTEGER PRIMARY KEY,
                embedding_json TEXT NOT NULL,
                model TEXT NOT NULL,
                text_length INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (url_id) REFERENCES urls (id)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_page_embeddings_model 
            ON page_embeddings(model)
        """)
        
        await conn.close()
    except Exception as e:
        print(f"Error creating embeddings table: {e}")


async def store_embedding_sqlite(
    db_path: str,
    url_id: int,
    embedding: List[float],
    model: str,
    text_length: int
):
    """Store embedding in SQLite database."""
    try:
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                INSERT INTO page_embeddings (url_id, embedding_json, model, text_length)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(url_id) DO UPDATE SET
                    embedding_json = excluded.embedding_json,
                    model = excluded.model,
                    text_length = excluded.text_length,
                    created_at = (strftime('%s', 'now'))
            """, (url_id, json.dumps(embedding), model, text_length))
            await db.commit()
    except Exception as e:
        print(f"Error storing embedding for url_id {url_id}: {e}")


async def store_embedding_postgresql(
    host: str,
    database: str,
    user: str,
    password: str,
    schema: str,
    url_id: int,
    embedding: List[float],
    model: str,
    text_length: int
):
    """Store embedding in PostgreSQL database."""
    try:
        conn = await asyncpg.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        
        await conn.execute(f"SET search_path TO {schema}, public")
        await conn.execute("""
            INSERT INTO page_embeddings (url_id, embedding_json, model, text_length)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (url_id) DO UPDATE SET
                embedding_json = EXCLUDED.embedding_json,
                model = EXCLUDED.model,
                text_length = EXCLUDED.text_length,
                created_at = CURRENT_TIMESTAMP
        """, url_id, json.dumps(embedding), model, text_length)
        
        await conn.close()
    except Exception as e:
        print(f"Error storing embedding for url_id {url_id}: {e}")


# ============================================================================
# Main Processing
# ============================================================================

async def process_embeddings(
    pages: List[Tuple[int, str, str]],
    api_key: str,
    model: str,
    db_type: str,
    db_path: Optional[str] = None,
    postgres_config: Optional[dict] = None,
    batch_size: int = 10,
    delay: float = 1.0,
    skip_existing: bool = True
):
    """
    Process pages and generate embeddings.
    
    Args:
        pages: List of (url_id, url, text) tuples
        api_key: OpenAI API key
        model: Embedding model name
        db_type: 'sqlite' or 'postgresql'
        db_path: Path to SQLite database (if db_type is 'sqlite')
        postgres_config: Dict with postgres connection info (if db_type is 'postgresql')
        batch_size: Number of pages to process before checking for existing embeddings
        delay: Delay in seconds between API calls
        skip_existing: Skip URLs that already have embeddings
    """
    total = len(pages)
    processed = 0
    skipped = 0
    errors = 0
    
    print(f"\nProcessing {total} pages...")
    print(f"Model: {model}")
    print(f"Batch size: {batch_size}")
    print(f"Delay between requests: {delay}s\n")
    
    # Check for existing embeddings if skip_existing is True
    existing_url_ids = set()
    if skip_existing:
        print("Checking for existing embeddings...")
        try:
            if db_type == "sqlite" and db_path:
                async with aiosqlite.connect(db_path) as db:
                    async with db.execute("SELECT url_id FROM page_embeddings") as cursor:
                        rows = await cursor.fetchall()
                        existing_url_ids = {row[0] for row in rows}
            elif db_type == "postgresql" and postgres_config:
                conn = await asyncpg.connect(
                    host=postgres_config["host"],
                    database=postgres_config["database"],
                    user=postgres_config["user"],
                    password=postgres_config["password"]
                )
                await conn.execute(f"SET search_path TO {postgres_config.get('schema', 'public')}, public")
                rows = await conn.fetch("SELECT url_id FROM page_embeddings")
                existing_url_ids = {row['url_id'] for row in rows}
                await conn.close()
            
            if existing_url_ids:
                print(f"Found {len(existing_url_ids)} existing embeddings")
        except Exception as e:
            print(f"Warning: Could not check for existing embeddings: {e}")
    
    for i, (url_id, url, text) in enumerate(pages, 1):
        # Skip if already exists
        if skip_existing and url_id in existing_url_ids:
            skipped += 1
            if i % 10 == 0:
                print(f"Progress: {i}/{total} (skipped: {skipped}, processed: {processed}, errors: {errors})")
            continue
        
        # Generate embedding
        embedding = generate_embedding(text, api_key, model)
        
        if embedding:
            # Store embedding
            if db_type == "sqlite" and db_path:
                await store_embedding_sqlite(db_path, url_id, embedding, model, len(text))
            elif db_type == "postgresql" and postgres_config:
                await store_embedding_postgresql(
                    postgres_config["host"],
                    postgres_config["database"],
                    postgres_config["user"],
                    postgres_config["password"],
                    postgres_config.get("schema", "public"),
                    url_id,
                    embedding,
                    model,
                    len(text)
                )
            
            processed += 1
            print(f"[{i}/{total}] ✓ {url} (embedding dim: {len(embedding)})")
        else:
            errors += 1
            print(f"[{i}/{total}] ✗ {url} (failed to generate embedding)")
        
        # Rate limiting
        if i < total:
            await asyncio.sleep(delay)
        
        # Progress update
        if i % batch_size == 0:
            print(f"\nProgress: {i}/{total} (processed: {processed}, skipped: {skipped}, errors: {errors})\n")
    
    print(f"\n{'='*60}")
    print(f"Completed!")
    print(f"Total pages: {total}")
    print(f"Processed: {processed}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {errors}")
    print(f"{'='*60}\n")


async def main():
    parser = argparse.ArgumentParser(
        description="Generate OpenAI embeddings from stored HTML page content",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    # Database connection options
    db_group = parser.add_mutually_exclusive_group(required=True)
    db_group.add_argument("--db-path", help="Path to SQLite database file")
    db_group.add_argument("--postgres-host", help="PostgreSQL host")
    
    parser.add_argument("--postgres-db", help="PostgreSQL database name")
    parser.add_argument("--postgres-user", help="PostgreSQL username")
    parser.add_argument("--postgres-password", help="PostgreSQL password")
    parser.add_argument("--postgres-schema", default="public", help="PostgreSQL schema (default: public)")
    
    # API options
    parser.add_argument("--api-key", required=True, help="OpenAI API key")
    parser.add_argument("--model", default="text-embedding-3-small", 
                       help="Embedding model (default: text-embedding-3-small)")
    
    # Processing options
    parser.add_argument("--urls", nargs="+", help="Specific URLs to process (optional)")
    parser.add_argument("--batch-size", type=int, default=10, 
                       help="Batch size for progress updates (default: 10)")
    parser.add_argument("--delay", type=float, default=1.0,
                       help="Delay in seconds between API calls (default: 1.0)")
    parser.add_argument("--no-skip-existing", action="store_true",
                       help="Regenerate embeddings for URLs that already have them")
    
    args = parser.parse_args()
    
    # Determine database type and get pages
    if args.db_path:
        db_type = "sqlite"
        print(f"Connecting to SQLite database: {args.db_path}")
        
        # Create embeddings table
        await create_embeddings_table_sqlite(args.db_path)
        
        # Get pages
        pages = await get_pages_sqlite(args.db_path, args.urls)
        
        if not pages:
            print("No pages found in database")
            return
        
        # Process embeddings
        await process_embeddings(
            pages,
            args.api_key,
            args.model,
            db_type,
            db_path=args.db_path,
            batch_size=args.batch_size,
            delay=args.delay,
            skip_existing=not args.no_skip_existing
        )
    
    elif args.postgres_host:
        db_type = "postgresql"
        
        if not all([args.postgres_db, args.postgres_user, args.postgres_password]):
            parser.error("--postgres-db, --postgres-user, and --postgres-password are required for PostgreSQL")
        
        print(f"Connecting to PostgreSQL: {args.postgres_host}/{args.postgres_db}")
        
        postgres_config = {
            "host": args.postgres_host,
            "database": args.postgres_db,
            "user": args.postgres_user,
            "password": args.postgres_password,
            "schema": args.postgres_schema
        }
        
        # Create embeddings table
        await create_embeddings_table_postgresql(**postgres_config)
        
        # Get pages
        pages = await get_pages_postgresql(
            args.postgres_host,
            args.postgres_db,
            args.postgres_user,
            args.postgres_password,
            args.postgres_schema,
            args.urls
        )
        
        if not pages:
            print("No pages found in database")
            return
        
        # Process embeddings
        await process_embeddings(
            pages,
            args.api_key,
            args.model,
            db_type,
            postgres_config=postgres_config,
            batch_size=args.batch_size,
            delay=args.delay,
            skip_existing=not args.no_skip_existing
        )


if __name__ == "__main__":
    asyncio.run(main())





