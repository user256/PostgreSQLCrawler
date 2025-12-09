import pytest
import time
from src.sqlitecrawler.circuit_breaker import CircuitBreaker, CircuitState

class TestCircuitBreaker:
    def test_initial_state(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True
        
    def test_failure_threshold(self):
        cb = CircuitBreaker(failure_threshold=2)
        
        # First failure
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        assert cb.allow_request() is True
        
        # Second failure -> OPEN
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.allow_request() is False
        
    def test_recovery(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        
        # Fail to open
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery
        time.sleep(0.2)
        
        # Should be HALF_OPEN
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.allow_request() is True
        
        # Success -> CLOSED
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        
    def test_half_open_failure(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        
        # Fail to open
        cb.record_failure()
        time.sleep(0.2)
        assert cb.state == CircuitState.HALF_OPEN
        
        # Fail again -> OPEN immediately
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
