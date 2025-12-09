import pytest
import os
import sys
from unittest.mock import MagicMock

# Add src to python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.sqlitecrawler.config import HttpConfig, CrawlLimits, AuthConfig

@pytest.fixture
def http_config():
    return HttpConfig(
        user_agent="TestBot/1.0",
        timeout=10,
        max_concurrency=2,
        delay_between_requests=0.1
    )

@pytest.fixture
def crawl_limits():
    return CrawlLimits(
        max_pages=10,
        max_depth=2,
        same_host_only=True
    )

@pytest.fixture
def auth_config():
    return AuthConfig(
        username="testuser",
        password="testpassword",
        auth_type="basic"
    )
