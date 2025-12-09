"""
Database abstraction layer for supporting both SQLite and PostgreSQL backends.

This module provides a unified interface for database operations that can work
with both SQLite (via aiosqlite) and PostgreSQL (via asyncpg).
"""

from __future__ import annotations
import asyncio
import json
import zlib
import base64
import time
from abc import ABC, abstractmethod
from typing import Optional, Iterable, Tuple, List, Dict, Any, Union
from dataclasses import dataclass
import os

# Import database-specific modules
try:
    import aiosqlite
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False

try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


@dataclass
class DatabaseConfig:
    """Configuration for database connections."""
    backend: str = "sqlite"  # "sqlite" or "postgresql"
    
    # SQLite configuration
    sqlite_path: str = ""
    
    # PostgreSQL configuration
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_database: str = "crawler_db"
    postgres_user: str = "crawler_user"
    postgres_password: str = ""
    postgres_schema: str = "public"
    postgres_pool_size: int = 10
    postgres_max_queries: int = 50000
    postgres_max_inactive_connection_lifetime: float = 300.0


class DatabaseConnection(ABC):
    """Abstract base class for database connections."""
    
    @abstractmethod
    async def execute(self, query: str, *args) -> Any:
        """Execute a query and return the result."""
        pass
    
    @abstractmethod
    async def executemany(self, query: str, args_list: List[Tuple]) -> Any:
        """Execute a query multiple times with different parameters."""
        pass
    
    @abstractmethod
    async def fetchone(self, query: str, *args) -> Optional[Tuple]:
        """Fetch one row from a query."""
        pass
    
    @abstractmethod
    async def fetchall(self, query: str, *args) -> List[Tuple]:
        """Fetch all rows from a query."""
        pass
    
    @abstractmethod
    async def commit(self) -> None:
        """Commit the current transaction."""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close the database connection."""
        pass
    
    @abstractmethod
    async def __aenter__(self):
        """Async context manager entry."""
        pass
    
    @abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        pass


class SQLiteConnection(DatabaseConnection):
    """SQLite database connection wrapper."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def __aenter__(self):
        if not SQLITE_AVAILABLE:
            raise ImportError("aiosqlite is not available")
        self.conn = await aiosqlite.connect(self.db_path)
        await self._optimize_connection()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            await self.conn.close()
    
    async def _optimize_connection(self):
        """Apply SQLite performance optimizations."""
        if self.conn:
            await self.conn.execute("PRAGMA journal_mode=WAL")
            await self.conn.execute("PRAGMA synchronous=NORMAL")
            await self.conn.execute("PRAGMA cache_size=10000")
            await self.conn.execute("PRAGMA temp_store=MEMORY")
    
    async def execute(self, query: str, *args) -> aiosqlite.Cursor:
        if not self.conn:
            raise RuntimeError("Connection not established")
        return await self.conn.execute(query, args)
    
    async def executemany(self, query: str, args_list: List[Tuple]) -> aiosqlite.Cursor:
        if not self.conn:
            raise RuntimeError("Connection not established")
        return await self.conn.executemany(query, args_list)
    
    async def fetchone(self, query: str, *args) -> Optional[Tuple]:
        if not self.conn:
            raise RuntimeError("Connection not established")
        cursor = await self.conn.execute(query, args)
        return await cursor.fetchone()
    
    async def fetchall(self, query: str, *args) -> List[Tuple]:
        if not self.conn:
            raise RuntimeError("Connection not established")
        cursor = await self.conn.execute(query, args)
        return await cursor.fetchall()
    
    async def commit(self) -> None:
        if self.conn:
            await self.conn.commit()
    
    async def close(self) -> None:
        if self.conn:
            await self.conn.close()


class PostgreSQLConnection(DatabaseConnection):
    """PostgreSQL database connection wrapper."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.conn: Optional[asyncpg.Connection] = None
    
    async def __aenter__(self):
        if not POSTGRES_AVAILABLE:
            raise ImportError("asyncpg is not available")
        
        self.conn = await asyncpg.connect(
            host=self.config.postgres_host,
            port=self.config.postgres_port,
            database=self.config.postgres_database,
            user=self.config.postgres_user,
            password=self.config.postgres_password
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            await self.conn.close()
    
    async def execute(self, query: str, *args) -> str:
        if not self.conn:
            raise RuntimeError("Connection not established")
        return await self.conn.execute(query, *args)
    
    async def executemany(self, query: str, args_list: List[Tuple]) -> str:
        if not self.conn:
            raise RuntimeError("Connection not established")
        return await self.conn.executemany(query, args_list)
    
    async def fetchone(self, query: str, *args) -> Optional[Tuple]:
        if not self.conn:
            raise RuntimeError("Connection not established")
        return await self.conn.fetchrow(query, *args)
    
    async def fetchall(self, query: str, *args) -> List[Tuple]:
        if not self.conn:
            raise RuntimeError("Connection not established")
        return await self.conn.fetch(query, *args)
    
    async def commit(self) -> None:
        # PostgreSQL auto-commits by default in asyncpg
        pass
    
    async def close(self) -> None:
        if self.conn:
            await self.conn.close()


class DatabasePool:
    """Database connection pool for managing multiple connections."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.pool: Optional[asyncpg.Pool] = None
        self._initialized = False
    
    async def initialize(self):
        """Initialize the connection pool."""
        if self.config.backend == "postgresql":
            if not POSTGRES_AVAILABLE:
                raise ImportError("asyncpg is not available")
            
            self.pool = await asyncpg.create_pool(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                database=self.config.postgres_database,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
                min_size=1,
                max_size=self.config.postgres_pool_size,
                max_queries=self.config.postgres_max_queries,
                max_inactive_connection_lifetime=self.config.postgres_max_inactive_connection_lifetime
            )
        self._initialized = True
    
    async def acquire(self) -> DatabaseConnection:
        """Acquire a database connection from the pool."""
        if not self._initialized:
            await self.initialize()
        
        if self.config.backend == "postgresql":
            if not self.pool:
                raise RuntimeError("PostgreSQL pool not initialized")
            conn = await self.pool.acquire()
            return PostgreSQLConnectionWrapper(conn)
        else:
            # For SQLite, create a new connection
            return SQLiteConnection(self.config.sqlite_path)
    
    async def release(self, conn: DatabaseConnection):
        """Release a database connection back to the pool."""
        if self.config.backend == "postgresql" and isinstance(conn, PostgreSQLConnectionWrapper):
            if self.pool:
                await self.pool.release(conn.conn)
        else:
            await conn.close()
    
    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()


class PostgreSQLConnectionWrapper(DatabaseConnection):
    """Wrapper for asyncpg connection to match our interface."""
    
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass  # Connection is managed by the pool
    
    async def execute(self, query: str, *args) -> str:
        return await self.conn.execute(query, *args)
    
    async def executemany(self, query: str, args_list: List[Tuple]) -> str:
        return await self.conn.executemany(query, args_list)
    
    async def fetchone(self, query: str, *args) -> Optional[Tuple]:
        return await self.conn.fetchrow(query, *args)
    
    async def fetchall(self, query: str, *args) -> List[Tuple]:
        return await self.conn.fetch(query, *args)
    
    async def commit(self) -> None:
        # PostgreSQL auto-commits by default in asyncpg
        pass
    
    async def close(self) -> None:
        # Connection is managed by the pool
        pass


class DatabaseFactory:
    """Factory for creating database connections and pools."""
    
    @staticmethod
    def create_connection(config: DatabaseConfig) -> DatabaseConnection:
        """Create a database connection based on configuration."""
        if config.backend == "sqlite":
            if not SQLITE_AVAILABLE:
                raise ImportError("aiosqlite is not available")
            return SQLiteConnection(config.sqlite_path)
        elif config.backend == "postgresql":
            if not POSTGRES_AVAILABLE:
                raise ImportError("asyncpg is not available")
            return PostgreSQLConnection(config)
        else:
            raise ValueError(f"Unsupported database backend: {config.backend}")
    
    @staticmethod
    def create_pool(config: DatabaseConfig) -> DatabasePool:
        """Create a database connection pool based on configuration."""
        return DatabasePool(config)


# Utility functions for data compression (shared between backends)
def compress_html(html: str) -> bytes:
    """Compress HTML using zlib with maximum compression level for smaller file sizes."""
    return base64.b64encode(zlib.compress(html.encode("utf-8"), level=9))


def decompress_html(encoded: bytes) -> str:
    """Decompress HTML from bytes to string."""
    try:
        return zlib.decompress(base64.b64decode(encoded)).decode("utf-8")
    except Exception:
        try:
            return encoded.decode("utf-8")  # type: ignore[arg-type]
        except Exception:
            return ""


def compress_headers(headers: dict) -> bytes:
    """Compress headers dictionary to bytes with maximum compression for smaller file sizes."""
    return base64.b64encode(zlib.compress(json.dumps(headers, ensure_ascii=False).encode("utf-8"), level=9))


def decompress_headers(encoded: bytes) -> dict:
    """Decompress headers from bytes to dictionary."""
    try:
        return json.loads(zlib.decompress(base64.b64decode(encoded)).decode("utf-8"))
    except Exception:
        return {}


# Global database configuration
_global_config: Optional[DatabaseConfig] = None
_global_pool: Optional[DatabasePool] = None


def set_global_config(config: DatabaseConfig):
    """Set the global database configuration."""
    global _global_config, _global_pool
    _global_config = config
    # Initialize pool for PostgreSQL
    if config.backend == "postgresql":
        _global_pool = DatabaseFactory.create_pool(config)
        # Initialize the pool asynchronously (will be done on first acquire)
    else:
        _global_pool = None


def get_global_config() -> Optional[DatabaseConfig]:
    """Get the global database configuration."""
    return _global_config


class PooledConnection:
    """Context manager wrapper for pooled connections."""
    
    def __init__(self, pool: DatabasePool):
        self.conn: Optional[DatabaseConnection] = None
        self.pool = pool
    
    async def __aenter__(self) -> DatabaseConnection:
        self.conn = await self.pool.acquire()
        return self.conn
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            await self.pool.release(self.conn)


def create_connection():
    """Create a database connection using the global configuration.
    
    For PostgreSQL, uses connection pooling for better performance.
    For SQLite, creates a new connection.
    
    Returns a context manager that handles connection acquisition/release.
    """
    if not _global_config:
        raise RuntimeError("Global database configuration not set")
    
    if _global_config.backend == "postgresql":
        # Use connection pool for PostgreSQL
        if _global_pool is None:
            raise RuntimeError("PostgreSQL pool not initialized. Call set_global_config() first.")
        return PooledConnection(_global_pool)  # Will acquire on __aenter__
    else:
        # For SQLite, create a new connection
        return DatabaseFactory.create_connection(_global_config)


def create_pool() -> DatabasePool:
    """Create a database connection pool using the global configuration."""
    if not _global_config:
        raise RuntimeError("Global database configuration not set")
    return DatabaseFactory.create_pool(_global_config)
