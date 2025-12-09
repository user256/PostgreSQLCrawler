"""
Circuit breaker pattern for handling failing hosts.
"""
import time
from enum import Enum
from typing import Dict, Optional

class CircuitState(Enum):
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing, reject requests
    HALF_OPEN = "HALF_OPEN" # Testing recovery

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._last_failure_time = 0.0
        
    @property
    def state(self) -> CircuitState:
        # Check if we should transition from OPEN to HALF_OPEN
        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time > self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state
        
    def allow_request(self) -> bool:
        """Check if a request should be allowed."""
        state = self.state
        return state == CircuitState.CLOSED or state == CircuitState.HALF_OPEN
        
    def record_success(self):
        """Record a successful request."""
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            self._failures = 0
        elif self._state == CircuitState.CLOSED:
            self._failures = 0
            
    def record_failure(self):
        """Record a failed request."""
        self._failures += 1
        self._last_failure_time = time.time()
        
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
        elif self._state == CircuitState.CLOSED:
            if self._failures >= self.failure_threshold:
                self._state = CircuitState.OPEN

class CircuitBreakerRegistry:
    """Registry to manage circuit breakers for multiple hosts."""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._breakers: Dict[str, CircuitBreaker] = {}
        
    def get_breaker(self, host: str) -> CircuitBreaker:
        if host not in self._breakers:
            self._breakers[host] = CircuitBreaker(
                failure_threshold=self.failure_threshold,
                recovery_timeout=self.recovery_timeout
            )
        return self._breakers[host]
