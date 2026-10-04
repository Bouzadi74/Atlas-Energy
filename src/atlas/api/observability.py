from prometheus_client import Counter, Gauge, Histogram, Info

from atlas import __version__

HTTP_REQUESTS = Counter(
    "atlas_api_http_requests_total",
    "HTTP requests processed by Atlas API.",
    ("method", "route", "status"),
)
HTTP_DURATION = Histogram(
    "atlas_api_http_request_duration_seconds",
    "Atlas API HTTP request duration in seconds.",
    ("method", "route"),
)
HTTP_IN_PROGRESS = Gauge(
    "atlas_api_http_requests_in_progress",
    "Atlas API HTTP requests currently in progress.",
    ("method",),
)
API_BUILD = Info("atlas_api_build", "Atlas API build information.")
API_BUILD.info({"version": __version__})
