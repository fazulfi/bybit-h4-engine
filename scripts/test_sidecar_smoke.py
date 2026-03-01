from telegram_sidecar.bot.callbacks import parse_callback
from telegram_sidecar.bot.hashutil import compute_hash


def main() -> None:
    view, params = parse_callback("v:sig:sid:AB12:from:open:p:2")
    assert view == "sig", view
    assert params == {"sid": "AB12", "from": "open", "p": "2"}, params
    assert compute_hash("hello") == compute_hash("hello")
    assert compute_hash("hello") != compute_hash("world")
    print("ok")


if __name__ == "__main__":
    main()
