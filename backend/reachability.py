"""Neighbour reachability probing with per-peer latency history.

This is a *read-only* health probe layered on top of the P2P surface: it GETs
each peer's ``/p2p/status`` and records the outcome (latency or failure) in a
bounded in-memory history per peer.  It deliberately keeps its own bookkeeping
instead of mutating :class:`~backend.p2p.Peer` fields, so the existing node
status views, chain sync and topology scan are unaffected by probing.
"""

import threading
import time

import requests

from .config import PEER_DIAL_TIMEOUT

# How many probe samples are kept per peer (also the avg-latency window).
HISTORY_LIMIT = 60


def _classify_error(exc):
    """Map a requests exception to a short, UI-friendly reason string."""
    if isinstance(exc, requests.ConnectionError):
        return "连接被拒绝"
    if isinstance(exc, requests.ConnectTimeout):
        return "连接超时"
    if isinstance(exc, requests.ReadTimeout):
        return "响应超时"
    if isinstance(exc, requests.Timeout):
        return "超时"
    if isinstance(exc, requests.HTTPError):
        return f"HTTP {exc.response.status_code if exc.response is not None else '错误'}"
    return str(exc)[:120]


class ReachabilityMonitor:
    """Tracks probe history and rolling stats for every known peer."""

    def __init__(self, peer_registry, history_limit=HISTORY_LIMIT):
        self._peers = peer_registry
        self._limit = history_limit
        self._lock = threading.Lock()
        # "host:port" -> {"history": [...], "failures": int, "probes": int}
        self._stats = {}
        # Guards against overlapping probes of the same peer.
        self._inflight = set()

    # ------------------------------------------------------------------ #
    # Recording
    # ------------------------------------------------------------------ #
    def _record(self, key, ok, latency_ms, error):
        with self._lock:
            st = self._stats.setdefault(
                key, {"history": [], "failures": 0, "probes": 0})
            st["probes"] += 1
            if not ok:
                st["failures"] += 1
            st["history"].append({
                "ts": time.time(),
                "ok": ok,
                "latency_ms": latency_ms,
                "error": error,
            })
            if len(st["history"]) > self._limit:
                st["history"] = st["history"][-self._limit:]

    def _summary(self, key):
        st = self._stats.get(key)
        if not st:
            return None
        history = list(st["history"])
        successes = [h["latency_ms"] for h in history
                     if h["ok"] and h["latency_ms"] is not None]
        last = history[-1] if history else None
        return {
            "probes": st["probes"],
            "failures": st["failures"],
            "avg_latency_ms": (round(sum(successes) / len(successes), 1)
                               if successes else None),
            "last_ok": bool(last and last["ok"]),
            "last_latency_ms": last["latency_ms"] if last else None,
            "last_error": last["error"] if last else None,
            "last_probe_ts": last["ts"] if last else None,
            "history": history,
        }

    # ------------------------------------------------------------------ #
    # Probing
    # ------------------------------------------------------------------ #
    def probe_peer(self, host, port):
        """Probe a single peer once; returns a result dict for the API."""
        peer = self._peers.by_key(host, port)
        if peer is None:
            return None
        key = f"{peer.host}:{peer.port}"
        with self._lock:
            if key in self._inflight:
                return {"skipped": True, "reason": "probe already in flight"}
            self._inflight.add(key)
        try:
            started = time.time()
            try:
                resp = requests.get(f"{peer.url}/p2p/status",
                                    timeout=PEER_DIAL_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                latency_ms = round((time.time() - started) * 1000, 1)
                ok, error = True, None
            except Exception as e:  # noqa: BLE001 — any failure means "down"
                latency_ms = None
                data = None
                ok, error = False, _classify_error(e)
            self._record(key, ok, latency_ms, error)
            return {
                "id": peer.id, "host": peer.host, "port": peer.port,
                "ok": ok, "latency_ms": latency_ms, "error": error,
                "ts": time.time(),
                "peer_height": data.get("height") if data else None,
            }
        finally:
            with self._lock:
                self._inflight.discard(key)

    def probe_all(self):
        """Probe every known peer once (sequentially); returns result list."""
        results = []
        for peer in self._peers.all():
            r = self.probe_peer(peer.host, peer.port)
            if r:
                results.append(r)
        return results

    # ------------------------------------------------------------------ #
    # Reporting
    # ------------------------------------------------------------------ #
    def report(self):
        """Full snapshot: one entry per known peer, newest stats attached."""
        out = []
        for peer in self._peers.all():
            key = f"{peer.host}:{peer.port}"
            with self._lock:
                summary = self._summary(key)
            entry = {
                "id": peer.id, "host": peer.host, "port": peer.port,
                "url": peer.url,
                "reachable": bool(summary and summary["last_ok"]),
                "probed": summary is not None,
            }
            if summary:
                entry.update(summary)
            out.append(entry)
        up = sum(1 for e in out if e["reachable"])
        return {
            "peers": out,
            "total": len(out),
            "up": up,
            "down": sum(1 for e in out if e["probed"] and not e["reachable"]),
            "unprobed": sum(1 for e in out if not e["probed"]),
            "history_limit": self._limit,
            "generated_at": time.time(),
        }

    def drop(self, host, port):
        """Forget stats for a removed peer."""
        with self._lock:
            self._stats.pop(f"{host}:{int(port)}", None)
