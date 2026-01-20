#!/usr/bin/env python3
"""
Script to recreate database views with the latest definitions.
This is useful when views have been updated but the database still has old versions.
"""
import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.sqlitecrawler.database_views import get_sqlite_views, get_postgres_views
from src.sqlitecrawler.database import create_connection
from src.sqlitecrawler.config import get_database_config, set_global_config


async def recreate_views_postgresql():
    """Recreate PostgreSQL views."""
    config = get_database_config()
    if config.backend != "postgresql":
        print("Error: This script is for PostgreSQL. Use recreate_views_sqlite.py for SQLite.")
        return
    
    print("Recreating PostgreSQL views...")
    async with create_connection() as conn:
        view_statements = get_postgres_views()
        for view_stmt in view_statements:
            if view_stmt:
                try:
                    await conn.execute(view_stmt)
                    # Extract view name from statement
                    view_name = view_stmt.split('VIEW')[1].split()[0] if 'VIEW' in view_stmt else 'unknown'
                    print(f"  ✓ Recreated view: {view_name}")
                except Exception as e:
                    print(f"  ✗ Failed to recreate view: {e}")
        await conn.commit()
    print("Done!")


async def recreate_views_sqlite(db_path: str):
    """Recreate SQLite views."""
    from src.sqlitecrawler.database import DatabaseConfig
    from src.sqlitecrawler.database import set_global_config
    
    config = DatabaseConfig(backend="sqlite", sqlite_path=db_path)
    set_global_config(config)
    
    print(f"Recreating SQLite views in {db_path}...")
    async with create_connection() as conn:
        # Drop existing views first (SQLite doesn't support CREATE OR REPLACE)
        view_names = [
            'view_crawl_overview', 'view_links_internal', 'view_links_external',
            'view_links_network', 'view_links_subdomain', 'view_sitemap_statistics',
            'view_schema_analysis', 'view_schema_hierarchy', 'view_crawl_status',
            'view_utm_links', 'view_hubs', 'view_exact_duplicates',
            'view_near_duplicates', 'view_content_hash_stats'
        ]
        
        for view_name in view_names:
            try:
                await conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            except Exception:
                pass  # View might not exist
        
        # Create new views
        view_statements = get_sqlite_views()
        for view_stmt in view_statements:
            if view_stmt:
                try:
                    await conn.execute(view_stmt)
                    # Extract view name from statement
                    view_name = view_stmt.split('VIEW IF NOT EXISTS')[1].split()[0] if 'VIEW IF NOT EXISTS' in view_stmt else 'unknown'
                    print(f"  ✓ Recreated view: {view_name}")
                except Exception as e:
                    print(f"  ✗ Failed to recreate view: {e}")
        await conn.commit()
    print("Done!")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # SQLite mode
        db_path = sys.argv[1]
        asyncio.run(recreate_views_sqlite(db_path))
    else:
        # PostgreSQL mode (uses environment variables)
        asyncio.run(recreate_views_postgresql())





