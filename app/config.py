"""Application configuration, all driven by environment variables."""
import os

# Shared secret used to verify inbound HMAC-SHA256 signatures and to sign
# outbound deliveries. MUST be set in real deployments.
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "dev-secret-change-me")

# SQLite database file location.
DB_PATH = os.environ.get("WEBHOOK_DB_PATH", "/data/webhooks.db")

# How often the scheduler scans for events due for delivery (seconds).
WORKER_POLL_INTERVAL = float(os.environ.get("WORKER_POLL_INTERVAL", "0.2"))

# How many different endpoints can be delivered to concurrently.
# Ordering is always preserved per endpoint regardless of this value.
WORKER_MAX_CONCURRENCY = int(os.environ.get("WORKER_MAX_CONCURRENCY", "8"))

# Outbound HTTP request timeout (seconds).
DELIVERY_TIMEOUT = float(os.environ.get("DELIVERY_TIMEOUT", "10"))

# Backoff schedule after the first failed attempt:
# attempt 1 happens immediately, attempts 2/3/4 wait 1/2/4 seconds.
RETRY_BACKOFF = (1.0, 2.0, 4.0)
MAX_ATTEMPTS = 4

# Per-target circuit breaker. After BREAKER_FAILURE_THRESHOLD consecutive
# failed deliveries to one target its breaker opens for BREAKER_OPEN_SECONDS,
# during which no event for that target is claimable. When the deadline has
# passed, only the due head event may go out as the single half-open probe:
# a 2xx closes the breaker and clears the failure count, any other outcome
# reopens it for another BREAKER_OPEN_SECONDS.
BREAKER_FAILURE_THRESHOLD = int(os.environ.get("BREAKER_FAILURE_THRESHOLD", "3"))
BREAKER_OPEN_SECONDS = float(os.environ.get("BREAKER_OPEN_SECONDS", "10"))

# Header carrying the hex HMAC-SHA256 signature, prefixed with "sha256=".
SIGNATURE_HEADER = "X-Webhook-Signature"
