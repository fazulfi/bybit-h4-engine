from __future__ import annotations

from telegram_sidecar.models.viewmodels import EngineViewModel


def format_engine(vm: EngineViewModel) -> str:
    status_emoji = "🟢" if vm.state.upper() in {"RUNNING", "OK", "CONNECTED"} else "🔴"
    stale_line = "\n⚠️ Data stale" if vm.stale else ""
    return (
        "🩺 <b>Engine Status</b>\n\n"
        f"State: {status_emoji} {vm.state}\n"
        f"WS: {vm.ws}\n"
        f"Heartbeat: {vm.heartbeat_ms} ms\n"
        f"Dropped (5m): {vm.dropped_5m}\n"
        f"Open Positions: {vm.open_positions}"
        f"{stale_line}"
    )
