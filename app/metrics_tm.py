from prometheus_client import Counter, Gauge, start_http_server

tm_ws_connected = Gauge("tm_ws_connected", "WS connected (1/0)")
tm_open_positions = Gauge("tm_open_positions", "Open virtual positions count")
tm_subscribed_symbols = Gauge("tm_subscribed_symbols", "Subscribed symbols count")
tm_last_tick_age_seconds = Gauge("tm_last_tick_age_seconds", "Seconds since last tick")
tm_last_heartbeat_age_seconds = Gauge("tm_last_heartbeat_age_seconds", "Seconds since last heartbeat")

tm_dropped_ticks_total = Counter("tm_dropped_ticks_total", "Dropped ticks total")
tm_reconnect_total = Counter("tm_reconnect_total", "WS reconnect total")
tm_exceptions_total = Counter("tm_exceptions_total", "Exceptions total")

_started = False


def start_metrics_server(port: int = 9101, addr: str = "127.0.0.1") -> None:
    global _started
    if _started:
        return
    start_http_server(port, addr=addr)
    _started = True
