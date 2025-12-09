#!/usr/bin/env python3
"""
Speed test script for PostgreSQLCrawler
Runs 4 different crawl configurations and records results in speedtests.db
"""

import asyncio
import glob
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

# Add venv to path for asyncpg
venv_path = Path(__file__).parent / "venv" / "lib" / "python3.12" / "site-packages"
if venv_path.exists():
    sys.path.insert(0, str(venv_path))

# Test configurations
TEST_URL = "https://whiskipedia.com"
SPEEDTESTS_DB = "data/speedtests.db"

# Ensure data directory exists
Path("data").mkdir(exist_ok=True)

# Initialize speedtests database
def init_speedtests_db():
    conn = sqlite3.connect(SPEEDTESTS_DB)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS runs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT,
            backend TEXT,
            http_backend TEXT,
            js_enabled INTEGER,
            start_ts INTEGER,
            end_ts INTEGER,
            duration_s REAL,
            urls_total INTEGER,
            frontier_done INTEGER,
            frontier_queued INTEGER,
            frontier_pending INTEGER,
            pages_written INTEGER,
            status_200 INTEGER,
            status_non200 INTEGER,
            failures INTEGER,
            retries INTEGER,
            pages_per_sec REAL,
            notes TEXT
        )
    """)
    
    # Migrate existing table if needed (add missing columns)
    try:
        cur.execute("ALTER TABLE runs ADD COLUMN failures INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    try:
        cur.execute("ALTER TABLE runs ADD COLUMN retries INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    try:
        cur.execute("ALTER TABLE runs ADD COLUMN pages_per_sec REAL DEFAULT 0.0")
    except sqlite3.OperationalError:
        pass  # Column already exists
    
    conn.commit()
    conn.close()

async def get_crawl_stats_postgresql(host, db, user, password, schema):
    """Get statistics from PostgreSQL crawl database"""
    import asyncpg
    
    try:
        conn = await asyncpg.connect(
            host=host,
            database=db,
            user=user,
            password=password
        )
        
        stats = {}
        
        # Set search path to schema
        await conn.execute(f"SET search_path TO {schema}, public")
        
        # Total URLs
        result = await conn.fetchval("SELECT COUNT(*) FROM urls")
        stats['urls_total'] = result or 0
        
        # Frontier stats
        result = await conn.fetch("""
            SELECT status, COUNT(*) 
            FROM frontier 
            GROUP BY status
        """)
        frontier_stats = {row['status']: row['count'] for row in result}
        stats['frontier_done'] = frontier_stats.get('done', 0)
        stats['frontier_queued'] = frontier_stats.get('queued', 0)
        stats['frontier_pending'] = frontier_stats.get('pending', 0)
        
        # Pages written
        result = await conn.fetchval("SELECT COUNT(*) FROM content")
        stats['pages_written'] = result or 0
        
        # Status codes
        result = await conn.fetch("""
            SELECT final_status_code, COUNT(*) 
            FROM page_metadata 
            GROUP BY final_status_code
        """)
        status_counts = {row['final_status_code']: row['count'] for row in result}
        stats['status_200'] = status_counts.get(200, 0)
        stats['status_non200'] = sum(v for k, v in status_counts.items() if k != 200)
        
        # Failures and retries
        result = await conn.fetchval("SELECT COUNT(*) FROM failed_urls")
        stats['failures'] = result or 0
        
        result = await conn.fetchval("SELECT COALESCE(SUM(retry_count), 0) FROM failed_urls")
        stats['retries'] = result or 0
        
        await conn.close()
        return stats
    except Exception as e:
        print(f"Error querying PostgreSQL: {e}")
        return None

def get_crawl_stats(db_path, backend="sqlite"):
    """Get statistics from the crawl database"""
    if backend == "postgresql":
        # This will be called asynchronously
        return None
    else:
        # SQLite
        if not os.path.exists(db_path):
            return None
        
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        stats = {}
        
        # Total URLs
        cur.execute("SELECT COUNT(*) FROM urls")
        stats['urls_total'] = cur.fetchone()[0]
        
        # Frontier stats
        cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
        frontier_stats = dict(cur.fetchall())
        stats['frontier_done'] = frontier_stats.get('done', 0)
        stats['frontier_queued'] = frontier_stats.get('queued', 0)
        stats['frontier_pending'] = frontier_stats.get('pending', 0)
        
        # Pages written
        cur.execute("SELECT COUNT(*) FROM content")
        stats['pages_written'] = cur.fetchone()[0]
        
        # Status codes
        cur.execute("SELECT final_status_code, COUNT(*) FROM page_metadata GROUP BY final_status_code")
        status_counts = dict(cur.fetchall())
        stats['status_200'] = status_counts.get(200, 0)
        stats['status_non200'] = sum(v for k, v in status_counts.items() if k != 200)
        
        # Failures and retries
        cur.execute("SELECT COUNT(*) FROM failed_urls")
        stats['failures'] = cur.fetchone()[0]
        
        cur.execute("SELECT SUM(retry_count) FROM failed_urls")
        retry_sum = cur.fetchone()[0]
        stats['retries'] = retry_sum if retry_sum else 0
        
        conn.close()
        return stats

def monitor_progress(db_name, backend, schema, stop_event, log_file):
    """Monitor crawl progress by polling the database and show periodic updates"""
    # Wait a bit for database to initialize
    time.sleep(5)
    
    last_pages = 0
    last_urls = 0
    last_time = time.time()
    last_update_time = time.time()
    update_interval = 60  # Show update every 60 seconds
    
    while not stop_event.is_set():
        try:
            if backend == "postgresql":
                # For PostgreSQL, we'd need asyncpg - skip for now or use psql
                time.sleep(10)
                continue
            else:
                db_path = f"data/{db_name}_crawl.db"
                if not os.path.exists(db_path):
                    time.sleep(5)
                    continue
                
                conn = sqlite3.connect(db_path)
                cur = conn.cursor()
                
                # Get current stats
                cur.execute("SELECT COUNT(*) FROM content")
                pages = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM urls")
                urls = cur.fetchone()[0]
                
                cur.execute("SELECT status, COUNT(*) FROM frontier GROUP BY status")
                frontier_stats = dict(cur.fetchall())
                done = frontier_stats.get('done', 0)
                queued = frontier_stats.get('queued', 0)
                pending = frontier_stats.get('pending', 0)
                
                # Calculate total (done + queued + pending)
                total = done + queued + pending
                
                conn.close()
                
                # Check for errors in log file
                errors = []
                if os.path.exists(log_file):
                    try:
                        with open(log_file, 'r') as f:
                            lines = f.readlines()
                            # Get last 50 lines and look for errors
                            for line in lines[-50:]:
                                if any(keyword in line.lower() for keyword in ['error', 'exception', 'traceback', 'failed', 'failure']):
                                    if line.strip() and line not in errors[-5:]:  # Avoid duplicates
                                        errors.append(line.strip())
                    except:
                        pass
                
                # Show update every update_interval seconds
                current_time = time.time()
                elapsed = current_time - last_update_time
                
                if elapsed >= update_interval:
                    pages_rate = (pages - last_pages) / elapsed if elapsed > 0 else 0
                    
                    # Show progress update
                    if total > 0:
                        progress_pct = (done / total * 100) if total > 0 else 0
                        progress_msg = f"📊 {done}/{total} pages ({progress_pct:.1f}%) | {pages} crawled | {pages_rate:.1f} pages/s | {urls} URLs found"
                    else:
                        progress_msg = f"📊 {pages} pages crawled | {pages_rate:.1f} pages/s | {urls} URLs found"
                    
                    sys.stderr.write(f"\r{progress_msg}" + " " * 20 + "\n")
                    
                    # Show errors if any
                    if errors:
                        for error in errors[-3:]:  # Show last 3 errors
                            sys.stderr.write(f"⚠️  {error[:100]}\n")
                    
                    sys.stderr.flush()
                    
                    last_pages = pages
                    last_urls = urls
                    last_update_time = current_time
                
                time.sleep(5)  # Poll every 5 seconds
        except Exception as e:
            # Silently handle errors (database might be locked or not exist yet)
            time.sleep(5)
    
    # Clear the progress line
    sys.stderr.write("\r" + " " * 100 + "\r")
    sys.stderr.flush()

def run_crawl_test(label, backend, http_backend, js_enabled, env_vars=None):
    """Run a single crawl test and return statistics"""
    print(f"\n{'='*60}")
    print(f"Running test: {label}")
    print(f"  Backend: {backend}")
    print(f"  HTTP Backend: {http_backend}")
    print(f"  JS Enabled: {js_enabled}")
    print(f"{'='*60}\n")
    
    # Build command
    cmd = ["venv/bin/python", "main.py", TEST_URL, "--quiet"]
    
    if http_backend == "curl":
        cmd.extend(["--http-backend", "curl"])
    
    if js_enabled:
        cmd.extend(["--js"])
    
    # Set environment variables
    env = os.environ.copy()
    if env_vars:
        env.update(env_vars)
    
    # Use the same database naming logic as the crawler
    from urllib.parse import urlparse
    parsed = urlparse(TEST_URL)
    domain = parsed.netloc.lower()
    # Remove www. prefix if present (same as get_website_db_name)
    if domain.startswith('www.'):
        domain = domain[4:]
    # Replace dots with underscores for database name (same as get_website_db_name)
    db_name = domain.replace('.', '_').replace('-', '_')
    schema = db_name  # For PostgreSQL schema
    
    # Clear any existing PostgreSQL env vars first
    pg_env_vars = [
        "PostgreSQLCrawler_DB_BACKEND",
        "SQLITECRAWLER_DB_BACKEND",
        "PostgreSQLCrawler_POSTGRES_HOST",
        "SQLITECRAWLER_POSTGRES_HOST",
        "PostgreSQLCrawler_POSTGRES_DB",
        "SQLITECRAWLER_POSTGRES_DB",
        "PostgreSQLCrawler_POSTGRES_USER",
        "SQLITECRAWLER_POSTGRES_USER",
        "PostgreSQLCrawler_POSTGRES_PASSWORD",
        "SQLITECRAWLER_POSTGRES_PASSWORD"
    ]
    for var in pg_env_vars:
        env.pop(var, None)
    
    if backend == "postgresql":
        env.update({
            "PostgreSQLCrawler_DB_BACKEND": "postgresql",
            "PostgreSQLCrawler_POSTGRES_HOST": "localhost",
            "PostgreSQLCrawler_POSTGRES_DB": "crawler_db",
            "PostgreSQLCrawler_POSTGRES_USER": "sql_crawler",
            "PostgreSQLCrawler_POSTGRES_PASSWORD": "bad_password"
        })
    else:
        # Explicitly set SQLite backend for SQLite tests
        env["PostgreSQLCrawler_DB_BACKEND"] = "sqlite"
    
    # Record start time
    start_ts = int(time.time())
    start_time = time.time()
    
    # Clear old log file
    log_file = f"data/{label}_crawl.log"
    if os.path.exists(log_file):
        os.remove(log_file)
    
    # Start progress monitor
    stop_event = threading.Event()
    monitor_thread = threading.Thread(
        target=monitor_progress,
        args=(db_name, backend, schema, stop_event, log_file),
        daemon=True
    )
    monitor_thread.start()
    
    try:
        # Run the crawl (blocking, but that's fine - we want to wait for it)
        # Redirect crawl output to a log file so progress monitor is visible
        print(f"🚀 Starting crawl... (errors logged to {log_file})\n")
        with open(log_file, 'w') as log:
            result = subprocess.run(
                cmd,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False
            )
        
        # Stop progress monitor
        stop_event.set()
        monitor_thread.join(timeout=1)
        
        end_time = time.time()
        end_ts = int(time.time())
        duration = end_time - start_time
        
        # Wait a moment for database to flush
        time.sleep(1)
        
        # Get statistics
        if backend == "postgresql":
            # Use asyncio to call the async PostgreSQL stats function
            stats = asyncio.run(get_crawl_stats_postgresql(
                "localhost", "crawler_db", "sql_crawler", "bad_password", schema
            ))
        else:
            db_path = f"data/{db_name}_crawl.db"
            # Verify database exists and has data
            if os.path.exists(db_path):
                stats = get_crawl_stats(db_path, backend="sqlite")
            else:
                print(f"⚠️  Database file not found: {db_path}")
                stats = None
        
        if stats is None:
            print(f"⚠️  Warning: Could not retrieve stats for {label}")
            stats = {}
        
        pages_per_sec = stats.get('pages_written', 0) / duration if duration > 0 else 0
        
        print(f"\n✅ Test completed: {label}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Pages: {stats.get('pages_written', 0)}")
        print(f"   Pages/sec: {pages_per_sec:.2f}")
        print(f"   URLs found: {stats.get('urls_total', 0)}")
        
        return {
            'label': label,
            'backend': backend,
            'http_backend': http_backend,
            'js_enabled': 1 if js_enabled else 0,
            'start_ts': start_ts,
            'end_ts': end_ts,
            'duration_s': duration,
            'urls_total': stats.get('urls_total', 0),
            'frontier_done': stats.get('frontier_done', 0),
            'frontier_queued': stats.get('frontier_queued', 0),
            'frontier_pending': stats.get('frontier_pending', 0),
            'pages_written': stats.get('pages_written', 0),
            'status_200': stats.get('status_200', 0),
            'status_non200': stats.get('status_non200', 0),
            'failures': stats.get('failures', 0),
            'retries': stats.get('retries', 0),
            'pages_per_sec': pages_per_sec,
            'notes': f"Exit code: {result.returncode}"
        }
    except Exception as e:
        print(f"❌ Error running test {label}: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_test_result(result):
    """Save test result to speedtests.db"""
    if result is None:
        return
    
    conn = sqlite3.connect(SPEEDTESTS_DB)
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO runs (
            label, backend, http_backend, js_enabled,
            start_ts, end_ts, duration_s,
            urls_total, frontier_done, frontier_queued, frontier_pending,
            pages_written, status_200, status_non200,
            failures, retries, pages_per_sec, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        result['label'],
        result['backend'],
        result['http_backend'],
        result['js_enabled'],
        result['start_ts'],
        result['end_ts'],
        result['duration_s'],
        result['urls_total'],
        result['frontier_done'],
        result['frontier_queued'],
        result['frontier_pending'],
        result['pages_written'],
        result['status_200'],
        result['status_non200'],
        result['failures'],
        result['retries'],
        result['pages_per_sec'],
        result['notes']
    ))
    
    conn.commit()
    conn.close()
    print(f"💾 Saved results for {result['label']}")

def delete_sqlite_databases():
    """Delete SQLite crawl databases"""
    # Use the same database naming logic as the crawler
    from urllib.parse import urlparse
    parsed = urlparse(TEST_URL)
    domain = parsed.netloc.lower()
    # Remove www. prefix if present
    if domain.startswith('www.'):
        domain = domain[4:]
    # Replace dots with underscores for database name
    db_name = domain.replace('.', '_').replace('-', '_')
    sqlite_files = [
        f"data/{db_name}_crawl.db",
        f"data/{db_name}_crawl.db-shm",
        f"data/{db_name}_crawl.db-wal",
        f"data/{db_name}_pages.db"
    ]
    
    # Check for locks first
    import subprocess
    try:
        result = subprocess.run(
            ["lsof"] + sqlite_files,
            capture_output=True,
            text=True,
            check=False
        )
        if result.stdout:
            print(f"   ⚠️  Warning: Database files may be locked by other processes")
            print(f"   Please close SQLiteBrowser or other tools accessing these files")
    except:
        pass  # lsof might not be available
    
    deleted = False
    for db_file in sqlite_files:
        if os.path.exists(db_file):
            try:
                os.remove(db_file)
                deleted = True
            except PermissionError:
                print(f"   ❌ Could not delete {db_file} - file is locked")
                print(f"   Please close any programs (like SQLiteBrowser) that have it open")
            except Exception as e:
                print(f"   ⚠️  Error deleting {db_file}: {e}")
    
    if deleted:
        print(f"   ✅ Cleared SQLite databases for {db_name}")

def reset_postgresql_database():
    """Reset PostgreSQL database"""
    print("   Resetting PostgreSQL database...")
    try:
        subprocess.run(
            ["psql", "-h", "localhost", "-U", "sql_crawler", "-d", "postgres", "-c", 
             f"DROP DATABASE IF EXISTS crawler_db;"],
            env={"PGPASSWORD": "bad_password"},
            capture_output=True,
            check=False
        )
        subprocess.run(
            ["psql", "-h", "localhost", "-U", "sql_crawler", "-d", "postgres", "-c",
             f"CREATE DATABASE crawler_db;"],
            env={"PGPASSWORD": "bad_password"},
            capture_output=True,
            check=False
        )
        print("   ✅ PostgreSQL database reset")
    except Exception as e:
        print(f"   ⚠️  Warning: Could not reset PostgreSQL database: {e}")

def main():
    print("🚀 Starting Speed Tests for PostgreSQLCrawler")
    print(f"   Test URL: {TEST_URL}\n")
    
    # Initialize speedtests database
    init_speedtests_db()
    
    # Clear all databases and log files at start
    print("🗑️  Clearing existing databases and logs...")
    delete_sqlite_databases()
    reset_postgresql_database()
    
    # Clear old log files
    for log_file in glob.glob("data/*_crawl.log"):
        try:
            os.remove(log_file)
        except:
            pass
    print()
    
    # Define test configurations
    tests = [
        {
            'label': 'curl_cffi_sqlite',
            'backend': 'sqlite',
            'http_backend': 'curl',
            'js_enabled': False
        },
        {
            'label': 'default_http_sqlite',
            'backend': 'sqlite',
            'http_backend': 'auto',
            'js_enabled': False
        },
        {
            'label': 'js_rendering_sqlite',
            'backend': 'sqlite',
            'http_backend': 'auto',
            'js_enabled': True
        },
        {
            'label': 'postgresql_backend',
            'backend': 'postgresql',
            'http_backend': 'auto',
            'js_enabled': False
        }
    ]
    
    # Run each test
    results = []
    for i, test in enumerate(tests):
        result = run_crawl_test(
            test['label'],
            test['backend'],
            test['http_backend'],
            test['js_enabled']
        )
        
        if result:
            save_test_result(result)
            results.append(result)
        
        # Clear databases AFTER reading stats, before next test
        if test != tests[-1]:
            print("\n⏳ Waiting 2 seconds, then clearing databases for next test...\n")
            time.sleep(2)
            
            # Clear SQLite databases before next SQLite test (they share the same files)
            next_test = tests[tests.index(test) + 1]
            if next_test['backend'] == 'sqlite':
                print(f"🗑️  Clearing SQLite databases before {next_test['label']}...")
                delete_sqlite_databases()
            
            # Clear PostgreSQL before next PostgreSQL test
            if next_test['backend'] == 'postgresql':
                print(f"🗑️  Resetting PostgreSQL database before {next_test['label']}...")
                reset_postgresql_database()
            
            time.sleep(3)
    
    # Print summary
    print(f"\n{'='*60}")
    print("📊 Speed Test Summary")
    print(f"{'='*60}\n")
    
    for result in results:
        print(f"{result['label']}:")
        print(f"  Duration: {result['duration_s']:.2f}s")
        print(f"  Pages: {result['pages_written']}")
        print(f"  Pages/sec: {result['pages_per_sec']:.2f}")
        print(f"  URLs found: {result['urls_total']}")
        print()
    
    print(f"✅ All tests completed! Results saved to {SPEEDTESTS_DB}")

if __name__ == "__main__":
    main()

