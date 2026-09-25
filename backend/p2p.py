"""P2P networking: peer registry and node-to-node HTTP transport.

Nodes communicate over plain HTTP (``requests``).  Each node exposes a small
``/p2p/*`` surface (status, blocks, block, tx, chain-head) and knows a list of
peers.  Broadcasting a new block/transaction fans it out to every reachable
peer; conflict resolution happens on the receiving node via the blockchain's
fork/reorg rules rather than by trusting the sender.
"""

import threading
import time
from collections import deque

import requests

from .config import PEER_DIAL_TIMEOUT, PROBE_HISTORY_LIMIT

# Message type tags used for logging / UI.
MSG_STATUS = "status"
MSG_BLOCK = "block"
MSG_TX = "tx"
MSG_BLOCKS = "blocks"
MSG_HEAD = "head"


class Peer:
    def __init__(self, peer_id, host, port):
        self.id = peer_id or f"peer-{port}"
        self.host = host
        self.port = int(port)
        self.url = f"http://{host}:{port}"
        self.status = "unknown"
        self.height = None
        self.head_hash = None
        self.chainwork = None
        self.last_seen = 0
        self.latency_ms = None
        self.last_error = None
        # Reachability probe bookkeeping: a bounded history of recent probes
        # plus lifetime counters, feeding the probe dashboard.
        self.probe_history = deque(maxlen=PROBE_HISTORY_LIMIT)
        self.probe_total = 0
        self.probe_failures = 0

    def probe_summary(self):
        """Aggregated reachability stats + recent history for the dashboard."""
        history = list(self.probe_history)
        latencies = [h["latency_ms"] for h in history
                     if h["ok"] and h["latency_ms"] is not None]
        window_failures = sum(1 for h in history if not h["ok"])
        return {
            "id": self.id,
            "host": self.host,
            "port": self.port,
            "url": self.url,
            "status": self.status,
            "reachable": self.status == "up",
            "last_latency_ms": self.latency_ms,
            "avg_latency_ms": (round(sum(latencies) / len(latencies), 1)
                               if latencies else None),
            "window": len(history),
            "window_failures": window_failures,
            "total_probes": self.probe_total,
            "total_failures": self.probe_failures,
            "last_probe_at": history[-1]["time"] if history else None,
            "last_error": self.last_error,
            "history": history,
        }

    def to_dict(self):
        return {
            "id": self.id,
            "host": self.host,
            "port": self.port,
            "url": self.url,
            "status": self.status,
            "height": self.height,
            "head_hash": self.head_hash,
            "chainwork": self.chainwork,
            "last_seen": self.last_seen,
            "latency_ms": self.latency_ms,
            "last_error": self.last_error,
        }


class PeerRegistry:
    def __init__(self):
        self.peers = {}          # "host:port" -> Peer
        self._log = []

    def add(self, peer_id, host, port):
        key = f"{host}:{port}"
        if key not in self.peers:
            self.peers[key] = Peer(peer_id, host, port)
        return self.peers[key]

    def remove(self, host, port):
        key = f"{host}:{port}"
        return self.peers.pop(key, None) is not None

    def all(self):
        return list(self.peers.values())

    def by_key(self, host, port):
        return self.peers.get(f"{host}:{port}")

    def known_urls(self):
        return [p.url for p in self.peers.values()]


def http_get_json(url, timeout=PEER_DIAL_TIMEOUT):
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def http_post_json(url, payload, timeout=PEER_DIAL_TIMEOUT):
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def dial_peer(peer, timeout=PEER_DIAL_TIMEOUT):
    """Fetch a peer's status, updating its bookkeeping fields."""
    started = time.time()
    try:
        data = http_get_json(f"{peer.url}/p2p/status", timeout=timeout)
        peer.status = "up"
        peer.height = data.get("height")
        peer.head_hash = data.get("head_hash")
        peer.chainwork = data.get("chainwork")
        peer.last_seen = time.time()
        peer.latency_ms = int((time.time() - started) * 1000)
        peer.last_error = None
        return True, data
    except Exception as e:  # noqa: BLE001
        peer.status = "down"
        peer.last_seen = time.time()
        peer.latency_ms = None
        peer.last_error = str(e)[:200]
        return False, {"error": str(e)}


def probe_peer(peer, timeout=PEER_DIAL_TIMEOUT):
    """Run one reachability probe against ``peer`` and record the outcome.

    Reuses :func:`dial_peer` so the peer's status/height/latency stay
    consistent with the rest of the UI, then appends the result to the
    peer's bounded probe history.  Returns ``(ok, entry)``.
    """
    ok, _data = dial_peer(peer, timeout=timeout)
    entry = {
        "time": time.time(),
        "ok": ok,
        "latency_ms": peer.latency_ms if ok else None,
        "error": None if ok else peer.last_error,
    }
    peer.probe_history.append(entry)
    peer.probe_total += 1
    if not ok:
        peer.probe_failures += 1
    return ok, entry


def probe_all(peers, timeout=PEER_DIAL_TIMEOUT):
    """Probe every peer concurrently so one slow peer can't stall the rest."""
    threads = [threading.Thread(target=probe_peer, args=(p, timeout),
                                daemon=True)
               for p in peers]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
