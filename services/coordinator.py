"""Lightweight coordination service for single-node Lighter grid bots.

This module offers an HTTP API compatible with the trimmed-down requirements:

* Register each VPS and track its most recent metrics (position, trading volume,
  balances).
* Allow operators to pause/resume all registered bots from a central dashboard.
* Provide a JSON status endpoint for automation as well as a small web dashboard.

The implementation reuses the aiohttp stack similar to the one in
``perp-dex-tools`` but omits hedge/primary role logic and global hedging rules.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional

from aiohttp import web

LOGGER = logging.getLogger("services.coordinator")
BASE_DIR = Path(__file__).resolve().parent
DASHBOARD_PATH = BASE_DIR / "dashboard.html"

Action = Literal["RUN", "PAUSE"]


def _to_decimal(value: Any, *, default: str = "0") -> Decimal:
    """Best-effort parsing for decimal-like payload values."""

    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


@dataclass
class VPSState:
    """Persistent metrics for a registered VPS."""

    vps_id: str
    display_name: Optional[str] = None
    position: Decimal = Decimal("0")
    position_symbol: Optional[str] = None
    position_value: Optional[Decimal] = None
    trading_volume: Decimal = Decimal("0")
    balance: Decimal = Decimal("0")
    total_value: Optional[Decimal] = None
    last_report_ts: float = field(default_factory=lambda: 0.0)

    def as_payload(self) -> Dict[str, Any]:
        return {
            "vps_id": self.vps_id,
            "display_name": self.display_name,
            "position": str(self.position),
            "position_symbol": self.position_symbol,
            "position_value": str(self.position_value) if self.position_value is not None else None,
            "trading_volume": str(self.trading_volume),
            "balance": str(self.balance),
            "total_value": str(self.total_value) if self.total_value is not None else None,
            "last_report_ts": self.last_report_ts,
        }


class CoordinatorState:
    """In-memory state machine for the simplified coordinator."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._agents: Dict[str, VPSState] = {}
        self._mode: Action = "RUN"
        self._reason: Optional[str] = None
        self._last_transition: float = time.time()
        self._agent_overrides: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Registration & inspection helpers
    async def register(self, *, vps_id: str, display_name: Optional[str]) -> Dict[str, Any]:
        async with self._lock:
            state = self._agents.get(vps_id)
            if state is None:
                state = VPSState(vps_id=vps_id, display_name=display_name)
                self._agents[vps_id] = state
                LOGGER.info("Registered new VPS %s", vps_id)
            else:
                if display_name:
                    state.display_name = display_name
                LOGGER.debug("VPS %s re-registered", vps_id)
            return {
                "mode": self._mode,
                "reason": self._reason,
                "issued_at": self._last_transition,
                "agent": state.as_payload(),
            }

    async def update_metrics(
        self,
        *,
        vps_id: str,
        position: Decimal,
        position_symbol: Optional[str],
        position_value: Optional[Decimal],
        trading_volume: Decimal,
        balance: Decimal,
        total_value: Optional[Decimal],
        timestamp: float,
    ) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self._agents:
                raise web.HTTPNotFound(text=f"vps '{vps_id}' is not registered")
            state = self._agents[vps_id]
            state.position = position
            state.position_symbol = position_symbol
            state.position_value = position_value
            state.trading_volume = trading_volume
            state.balance = balance
            state.total_value = total_value
            state.last_report_ts = timestamp
            LOGGER.debug(
                "Metrics update %s pos=%s vol=%s balance=%s",
                vps_id,
                position,
                trading_volume,
                balance,
            )
            return self._status_locked()

    async def next_command(self, *, vps_id: str) -> Dict[str, Any]:
        async with self._lock:
            if vps_id not in self._agents:
                raise web.HTTPNotFound(text=f"vps '{vps_id}' is not registered")
            override = self._agent_overrides.get(vps_id)
            return {
                "action": override.get("mode") if override else self._mode,
                "reason": override.get("reason") if override else self._reason,
                "issued_at": override.get("issued_at") if override else self._last_transition,
                "scope": "agent" if override else "global",
            }

    async def set_mode(
        self,
        *,
        mode: Action,
        reason: Optional[str],
        target_vps_ids: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        async with self._lock:
            now = time.time()
            if target_vps_ids is None:
                if mode == self._mode and reason == self._reason:
                    return self._status_locked()
                self._mode = mode
                self._reason = reason
                self._last_transition = now
                self._agent_overrides.clear()
                LOGGER.warning("Global mode switched to %s: %s", mode, reason)
                return self._status_locked()

            updated_targets = []
            for vps_id in target_vps_ids:
                if vps_id not in self._agents:
                    LOGGER.warning("Attempted to set mode for unknown VPS %s", vps_id)
                    continue
                if mode == self._mode:
                    if vps_id in self._agent_overrides:
                        del self._agent_overrides[vps_id]
                        updated_targets.append(vps_id)
                else:
                    self._agent_overrides[vps_id] = {
                        "mode": mode,
                        "reason": reason,
                        "issued_at": now,
                    }
                    updated_targets.append(vps_id)

            if updated_targets:
                LOGGER.warning(
                    "Override mode %s applied to: %s",
                    mode,
                    ", ".join(updated_targets),
                )
            return self._status_locked()

    async def status(self) -> Dict[str, Any]:
        async with self._lock:
            return self._status_locked()

    # ------------------------------------------------------------------
    def _status_locked(self) -> Dict[str, Any]:
        totals = {
            "position": Decimal("0"),
            "position_value": Decimal("0"),
            "trading_volume": Decimal("0"),
            "balance": Decimal("0"),
            "total_value": Decimal("0"),
            "has_position_value": False,
            "has_total_value": False,
        }
        agents_snapshot = []
        for agent in self._agents.values():
            payload = agent.as_payload()
            override = self._agent_overrides.get(agent.vps_id)
            payload.update(
                {
                    "command": override.get("mode") if override else self._mode,
                    "command_reason": override.get("reason") if override else self._reason,
                    "command_scope": "agent" if override else "global",
                    "command_issued_at": override.get("issued_at") if override else self._last_transition,
                }
            )
            agents_snapshot.append(payload)
            totals["position"] += agent.position
            if agent.position_value is not None:
                totals["position_value"] += agent.position_value
                totals["has_position_value"] = True
            totals["trading_volume"] += agent.trading_volume
            totals["balance"] += agent.balance
            if agent.total_value is not None:
                totals["total_value"] += agent.total_value
                totals["has_total_value"] = True

        return {
            "mode": self._mode,
            "reason": self._reason,
            "issued_at": self._last_transition,
            "agents": agents_snapshot,
            "totals": {
                "position": str(totals["position"]),
                "position_value": str(totals["position_value"]) if totals["has_position_value"] else None,
                "trading_volume": str(totals["trading_volume"]),
                "balance": str(totals["balance"]),
                "total_value": str(totals["total_value"]) if totals["has_total_value"] else None,
            },
        }


class CoordinatorApp:
    """aiohttp application exposing the coordinator API and dashboard."""

    def __init__(
        self,
        *,
        state: CoordinatorState,
        dashboard_user: Optional[str],
        dashboard_password: Optional[str],
    ) -> None:
        self.state = state
        self.dashboard_user = dashboard_user
        self.dashboard_password = dashboard_password
        self.app = web.Application()
        self.app.add_routes(
            [
                web.get("/healthz", self.handle_healthz),
                web.post("/register", self.handle_register),
                web.post("/metrics", self.handle_metrics),
                web.get("/command", self.handle_command),
                web.get("/status", self.handle_status),
                web.post("/manual_pause", self.handle_manual_pause),
                web.post("/manual_resume", self.handle_manual_resume),
                web.get("/dashboard", self.handle_dashboard),
            ]
        )

    # ------------------------------------------------------------------
    async def handle_healthz(self, _: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def handle_register(self, request: web.Request) -> web.Response:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON object required")
        vps_id = payload.get("vps_id")
        if not vps_id or not isinstance(vps_id, str):
            raise web.HTTPBadRequest(text="vps_id required")
        display_name = payload.get("display_name")
        if display_name is not None and not isinstance(display_name, str):
            raise web.HTTPBadRequest(text="display_name must be string")
        snapshot = await self.state.register(vps_id=vps_id, display_name=display_name)
        return web.json_response(snapshot)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        payload = await request.json()
        if not isinstance(payload, dict):
            raise web.HTTPBadRequest(text="JSON object required")
        vps_id = payload.get("vps_id")
        if not vps_id or not isinstance(vps_id, str):
            raise web.HTTPBadRequest(text="vps_id required")

        position = _to_decimal(payload.get("position"))
        position_symbol = payload.get("position_symbol")
        if position_symbol is not None and not isinstance(position_symbol, str):
            raise web.HTTPBadRequest(text="position_symbol must be string when provided")
        trading_volume = _to_decimal(payload.get("trading_volume"))
        balance = _to_decimal(payload.get("balance"))
        total_value_raw = payload.get("total_value")
        total_value = _to_decimal(total_value_raw) if total_value_raw is not None else None
        position_value_raw = payload.get("position_value")
        position_value = _to_decimal(position_value_raw) if position_value_raw is not None else None
        timestamp = float(payload.get("timestamp", time.time()))

        status = await self.state.update_metrics(
            vps_id=vps_id,
            position=position,
            position_symbol=position_symbol,
            position_value=position_value,
            trading_volume=trading_volume,
            balance=balance,
            total_value=total_value,
            timestamp=timestamp,
        )
        return web.json_response(status)

    async def handle_command(self, request: web.Request) -> web.Response:
        vps_id = request.query.get("vps_id")
        if not vps_id:
            raise web.HTTPBadRequest(text="vps_id query parameter required")
        command = await self.state.next_command(vps_id=vps_id)
        return web.json_response(command)

    async def handle_status(self, _: web.Request) -> web.Response:
        snapshot = await self.state.status()
        return web.json_response(snapshot)

    async def handle_manual_pause(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload: Dict[str, Any]
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        reason = payload.get("reason") if isinstance(payload, dict) else None
        targets = self._extract_target_vps(payload)
        default_reason = "Manual pause (selected)" if targets else "Manual pause triggered"
        reason_text = reason if isinstance(reason, str) and reason.strip() else default_reason
        status = await self.state.set_mode(mode="PAUSE", reason=reason_text, target_vps_ids=targets)
        return web.json_response(status)

    async def handle_manual_resume(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        payload: Dict[str, Any]
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        reason = payload.get("reason") if isinstance(payload, dict) else None
        targets = self._extract_target_vps(payload)
        default_reason = "Manual resume (selected)" if targets else "Manual resume triggered"
        reason_text = reason if isinstance(reason, str) and reason.strip() else default_reason
        status = await self.state.set_mode(mode="RUN", reason=reason_text, target_vps_ids=targets)
        return web.json_response(status)

    async def handle_dashboard(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        try:
            html = DASHBOARD_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            raise web.HTTPNotFound(text="dashboard asset missing; ensure dashboard.html exists")
        return web.Response(text=html, content_type="text/html")

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_target_vps(payload: Dict[str, Any]) -> Optional[List[str]]:
        if not isinstance(payload, dict):
            return None
        candidates: List[str] = []
        single = payload.get("vps_id")
        if isinstance(single, str) and single.strip():
            candidates.append(single.strip())
        multiple = payload.get("vps_ids")
        if isinstance(multiple, list):
            for item in multiple:
                if isinstance(item, str) and item.strip():
                    candidates.append(item.strip())
        unique = list(dict.fromkeys(candidates))
        return unique or None

    def _require_dashboard_auth(self, request: web.Request) -> None:
        if not self.dashboard_user and not self.dashboard_password:
            return
        header = request.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Coordinator"'})
        token = header[6:]
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Coordinator"'})
        username, _, password = decoded.partition(":")
        if username != (self.dashboard_user or "") or password != (self.dashboard_password or ""):
            raise web.HTTPUnauthorized(headers={"WWW-Authenticate": 'Basic realm="Coordinator"'})


async def _start_app(host: str, port: int, *, user: Optional[str], password: Optional[str]) -> None:
    state = CoordinatorState()
    app_wrapper = CoordinatorApp(state=state, dashboard_user=user, dashboard_password=password)
    runner = web.AppRunner(app_wrapper.app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    LOGGER.info("Starting coordinator on %s:%s", host, port)
    await site.start()
    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await runner.cleanup()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run single-cluster coordination service")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8787, help="TCP port to bind (default: 8787)")
    parser.add_argument("--dashboard-user", help="Basic auth username for dashboard/API")
    parser.add_argument("--dashboard-password", help="Basic auth password for dashboard/API")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )

    try:
        asyncio.run(
            _start_app(
                args.host,
                args.port,
                user=args.dashboard_user,
                password=args.dashboard_password,
            )
        )
    except KeyboardInterrupt:
        LOGGER.info("Coordinator interrupted; shutting down")


if __name__ == "__main__":
    main()
