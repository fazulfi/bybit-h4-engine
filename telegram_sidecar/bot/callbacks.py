from __future__ import annotations


def parse_callback(raw: str) -> tuple[str, dict[str, str]]:
    if not raw:
        return "home", {}

    tokens = raw.split(":")
    if len(tokens) < 2 or tokens[0] != "v":
        return "home", {}

    view = tokens[1]
    params: dict[str, str] = {}

    i = 2
    while i + 1 < len(tokens):
        key = tokens[i]
        value = tokens[i + 1]
        if key in {"p", "r", "sid", "from"}:
            params[key] = value
        i += 2

    return view, params
