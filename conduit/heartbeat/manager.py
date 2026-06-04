"""
Heartbeat Manager

Background task that sends application-level heartbeat pings and tracks
connection liveness — completely independent from QUIC's transport-level
keep-alive. Designed so heartbeat failures never block or cancel the main
connection flow.
"""

import asyncio
import time
import logging
from typing import Callable, Dict, Optional, Awaitable

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """
    Application-level heartbeat manager.

    Runs as a background asyncio task. Sends periodic PING messages to a
    set of targets and tracks when each last responded with a PONG. If a
    target misses `max_missed` consecutive pings it is considered stale and
    the optional `on_stale` callback fires.

    Design principles:
    - Never raises into the caller's stack
    - Self-contained asyncio Task with graceful shutdown
    - Can track multiple targets (server-side: per-connection, client-side: server)
    """

    def __init__(
        self,
        interval: float = 30.0,
        timeout: float = 90.0,
        max_missed: int = 3,
        on_stale: Optional[Callable[[str], Awaitable[None]]] = None,
    ):
        """
        Args:
            interval:   Seconds between heartbeat pings.
            timeout:    Seconds without a pong before a target is considered stale.
            max_missed: Consecutive missed pings before `on_stale` fires.
            on_stale:   Async callback(target_id) called when a target goes stale.
        """
        self._interval   = interval
        self._timeout    = timeout
        self._max_missed = max_missed
        self._on_stale   = on_stale

        # target_id → last pong timestamp (monotonic)
        self._last_pong: Dict[str, float]    = {}
        # target_id → consecutive missed count
        self._missed:    Dict[str, int]      = {}
        # target_id → async send function(bytes)
        self._senders:   Dict[str, Callable] = {}

        self._task:    Optional[asyncio.Task] = None
        self._running: bool                   = False

    def register(self, target_id: str, send_fn: Callable[[bytes], None]) -> None:
        """Register a target to heartbeat. `send_fn` must accept raw encoded bytes."""
        self._last_pong[target_id] = time.monotonic()
        self._missed[target_id]    = 0
        self._senders[target_id]   = send_fn

    def unregister(self, target_id: str) -> None:
        """Remove a target (e.g. on disconnect)."""
        self._last_pong.pop(target_id, None)
        self._missed.pop(target_id, None)
        self._senders.pop(target_id, None)

    def record_pong(self, target_id: str) -> None:
        """Call this when a HEARTBEAT_PONG is received from a target."""
        self._last_pong[target_id] = time.monotonic()
        self._missed[target_id]    = 0
        logger.debug(f"[Heartbeat] Pong from {target_id}")

    def is_alive(self, target_id: str) -> bool:
        """Return True if the target responded within `timeout` seconds."""
        last = self._last_pong.get(target_id)
        return last is not None and (time.monotonic() - last) < self._timeout

    def start(self, ping_encoder: Callable[[], bytes]) -> None:
        """Start the background heartbeat loop. `ping_encoder` returns an encoded PING frame."""
        if self._running:
            return
        self._running = True
        self._ping_encoder = ping_encoder
        self._task = asyncio.create_task(self._loop(), name="heartbeat-manager")
        self._task.add_done_callback(self._on_task_done)
        logger.debug(f"[Heartbeat] Manager started (interval={self._interval}s)")

    async def stop(self) -> None:
        """Gracefully stop the heartbeat loop."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.debug("[Heartbeat] Manager stopped")

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                if not self._running:
                    break
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning(f"[Heartbeat] Loop error (non-fatal): {exc}")

    async def _tick(self) -> None:
        """Send pings to all registered targets and check for stale ones."""
        now  = time.monotonic()
        ping = self._ping_encoder()

        for target_id, send_fn in list(self._senders.items()):
            try:
                # Send ping (fire-and-forget, never blocks the loop)
                if asyncio.iscoroutinefunction(send_fn):
                    asyncio.create_task(send_fn(ping))
                else:
                    send_fn(ping)
            except Exception as exc:
                logger.debug(f"[Heartbeat] Send to {target_id} failed: {exc}")

            # Check liveness
            last = self._last_pong.get(target_id, now)
            age  = now - last
            if age > self._timeout:
                self._missed[target_id] = self._missed.get(target_id, 0) + 1
                logger.debug(
                    f"[Heartbeat] {target_id} missed={self._missed[target_id]} "
                    f"age={age:.1f}s"
                )
                if self._missed[target_id] >= self._max_missed and self._on_stale:
                    try:
                        await self._on_stale(target_id)
                    except Exception as exc:
                        logger.warning(f"[Heartbeat] on_stale({target_id}) raised: {exc}")

    def _on_task_done(self, task: asyncio.Task) -> None:
        if not task.cancelled() and task.exception():
            logger.error(f"[Heartbeat] Task died unexpectedly: {task.exception()}")
        self._running = False
