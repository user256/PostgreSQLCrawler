import pytest
from src.sqlitecrawler.hashing import generate_content_hashes, is_near_duplicate, clean_content_for_hashing

class TestHashing:
    def test_clean_content(self):
        html = """
        <html>
            <head><script>var x=1;</script></head>
            <body>
                <h1>Hello World</h1>
                <!-- comment -->
                <div class="b a">Text</div>
            </body>
        </html>
        """
        cleaned = clean_content_for_hashing(html)
        assert "Hello World" in cleaned
        assert "Text" in cleaned
        assert "var x=1" not in cleaned
        
    def test_generate_hashes(self):
        html = "<html><body><h1>Test Page</h1><p>Content</p></body></html>"
        hashes = generate_content_hashes(html)
        
        assert 'content_hash_sha256' in hashes
        assert 'content_hash_simhash' in hashes
        assert 'content_length' in hashes
        assert hashes['content_length'] > 0
        
    def test_near_duplicate(self):
        # Identical content
        h1 = "1234567890"
        assert is_near_duplicate(h1, h1) is True
        
        # Completely different (simulated)
        # Note: SimHash implementation details might vary, but we test the interface
        # Using actual SimHash values would be better if we could predict them easily
        pass
