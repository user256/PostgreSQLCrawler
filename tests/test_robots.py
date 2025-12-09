import pytest
import time
from unittest.mock import MagicMock, patch
from src.sqlitecrawler.robots import RobotsCache, calculate_cache_ttl, is_url_crawlable

class TestRobotsCache:
    def test_calculate_cache_ttl(self):
        # Test Cache-Control max-age
        assert calculate_cache_ttl({'cache-control': 'max-age=3600'}) == 3600
        assert calculate_cache_ttl({'Cache-Control': 'max-age=60'}) == 60
        
        # Test no-cache
        assert calculate_cache_ttl({'cache-control': 'no-cache'}) == 0
        
        # Test default
        assert calculate_cache_ttl({}) == 3600
        
    def test_robots_cache_expiry(self):
        cache = RobotsCache(default_ttl=1)
        parser = MagicMock()
        
        # Set cache
        cache.set_robots_parser("example.com", parser, {}, {})
        
        # Should be available immediately
        assert cache.get_robots_parser("example.com") == parser
        
        # Wait for expiry
        time.sleep(1.1)
        
        # Should be expired
        assert cache.get_robots_parser("example.com") is None

@pytest.mark.asyncio
async def test_is_url_crawlable():
    # Mock robots_cache
    with patch('src.sqlitecrawler.robots.robots_cache') as mock_cache:
        # Case 1: No robots.txt (failed or missing) -> Allow
        mock_cache.is_failed.return_value = True
        assert is_url_crawlable("https://example.com/page") is True
        
        # Case 2: Parser available, allowed
        mock_cache.is_failed.return_value = False
        mock_parser = MagicMock()
        # We need to mock the internal structure of the parser as used in is_url_crawlable
        # The function accesses parser._entries directly
        mock_parser._entries = {'*': [('allow', '/')]}
        mock_cache.get_robots_parser.return_value = mock_parser
        
        assert is_url_crawlable("https://example.com/page") is True
        
        # Case 3: Parser available, disallowed
        mock_parser._entries = {'*': [('disallow', '/')]}
        assert is_url_crawlable("https://example.com/page") is False
