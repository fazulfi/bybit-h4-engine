from telegram_sidecar.bot.callbacks import parse_callback
from telegram_sidecar.bot.hashutil import compute_hash
from telegram_sidecar.bot.pollutil import safe_poll_interval
from telegram_sidecar.bot.session_store import cleanup_sessions, new_session


def main() -> None:
    view, params = parse_callback("v:sig:sid:AB12:from:open:p:2")
    assert view == "sig", view
    assert params == {"sid": "AB12", "from": "open", "p": "2"}, params
    assert compute_hash("hello") == compute_hash("hello")
    assert compute_hash("hello") != compute_hash("world")
    assert safe_poll_interval(5) == 5
    assert safe_poll_interval(0) == 1
    assert safe_poll_interval("abc") == 5
    sample_sessions = {(1, 1): new_session(), (2, 2): new_session()}
    sample_sessions[(1, 1)]["updated_at"] = 0
    removed = cleanup_sessions(sample_sessions)
    assert removed >= 1
    assert (1, 1) not in sample_sessions
    print("ok")


if __name__ == "__main__":
    main()
