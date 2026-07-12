#!/usr/bin/env python3
import json
import os
import socket
import time
import threading
import requests
from flask import Flask, request, jsonify, send_from_directory

app = Flask(__name__)

PORT = int(os.environ.get("PORT", 8080))
TELNET_PORT = int(os.environ.get("TELNET_PORT", 7300))
TELNET_CLIENTS = []
TELNET_LOCK = threading.Lock()

RBN_API = "https://www.reversebeacon.net/spots.php"

_user_spots = []
_user_spots_lock = threading.Lock()
_user_spot_id = 0
_user_spot_id_lock = threading.Lock()

_cache = {}
_cache_lock = threading.Lock()
_cache_ttl = 3

_rbn_hash = "ab6db5"
_rbn_hash_lock = threading.Lock()
_rbn_last_id = 0
_rbn_last_id_lock = threading.Lock()

_session = requests.Session()
_session.headers.update({
    "User-Agent": "Mozilla/5.0 (DX Cluster)",
    "Connection": "keep-alive",
})


def _rbn_fetch(params):
    global _rbn_hash
    with _rbn_hash_lock:
        current_hash = _rbn_hash
    params["h"] = current_hash
    resp = _session.get(RBN_API, params=params, timeout=10)
    try:
        data = resp.json()
    except Exception:
        return None
    if isinstance(data, dict) and data.get("error") == 888:
        new_hash = data.get("ver_h")
        if new_hash:
            with _rbn_hash_lock:
                _rbn_hash = new_hash
                print(f"[RBN] Hash updated: {new_hash}", flush=True)
            params["h"] = new_hash
            resp = _session.get(RBN_API, params=params, timeout=10)
            try:
                return resp.json()
            except Exception:
                return None
        return None
    return data


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/rbn")
def api_rbn():
    de = request.args.get("de", "").strip().upper()
    if not de:
        return jsonify({"error": "missing de parameter"}), 400

    age = request.args.get("age", "3600")
    cache_key = f"{de}:{age}"

    with _cache_lock:
        if cache_key in _cache:
            ts, data = _cache[cache_key]
            if time.time() - ts < _cache_ttl:
                return jsonify(data)

    params = {"cde": de, "r": 200, "s": 0, "ma": age}
    try:
        data = _rbn_fetch(params)
        if data:
            with _cache_lock:
                _cache[cache_key] = (time.time(), data)
            return jsonify(data)
        return jsonify({"error": "upstream unavailable"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/user-spots", methods=["GET"])
def get_user_spots():
    age = int(request.args.get("age", "1800"))
    cutoff = int(time.time()) - age
    with _user_spots_lock:
        filtered = [s for s in _user_spots if s["time"] >= cutoff]
        return jsonify(filtered[-200:])


@app.route("/api/user-spots", methods=["POST"])
def add_user_spot():
    global _user_spot_id
    body = request.get_json(force=True, silent=True) or {}
    de = (body.get("de") or "").strip().upper()
    dx = (body.get("dx") or "").strip().upper()
    freq = (body.get("freq") or "").strip()
    comment = (body.get("comment") or "").strip()[:120]

    if not de or not dx:
        return jsonify({"error": "DE and DX required"}), 400

    with _user_spot_id_lock:
        spot_id = _user_spot_id
        _user_spot_id += 1

    spot = {
        "id": spot_id,
        "de": de[:12],
        "dx": dx[:12],
        "freq": freq[:10],
        "comment": comment,
        "time": int(time.time()),
    }
    with _user_spots_lock:
        _user_spots.append(spot)
        if len(_user_spots) > 500:
            _user_spots.pop(0)
    _broadcast_telnet(spot)
    return jsonify(spot), 201


@app.route("/api/user-spots", methods=["DELETE"])
def clear_user_spots():
    with _user_spots_lock:
        _user_spots.clear()
    return jsonify({"ok": True})


def _rbn_poller():
    global _rbn_last_id
    print("[RBN-POLL] Poller started", flush=True)
    time.sleep(2)
    while True:
        try:
            with _rbn_last_id_lock:
                last_id = _rbn_last_id
            params = {"cde": "R4NCU", "r": 200, "s": last_id, "ma": 1800}
            data = _rbn_fetch(params)
            if data and isinstance(data, dict):
                spots = data.get("spots", {})
                if spots:
                    max_id = max(int(k) for k in spots.keys())
                    new_count = 0
                    for sid, s in spots.items():
                        if int(sid) <= last_id:
                            continue
                        spot = {
                            "de": s[0],
                            "freq": s[1],
                            "dx": s[2],
                            "comment": f"SNR {s[3]} dB",
                            "time": int(s[10]),
                        }
                        _broadcast_telnet(spot)
                        new_count += 1
                    with _rbn_last_id_lock:
                        _rbn_last_id = max_id
                    if new_count > 0:
                        print(f"[RBN-POLL] Broadcast {new_count} new spots", flush=True)
        except Exception as e:
            print(f"[RBN-POLL] Error: {e}", flush=True)
        time.sleep(5)


def _format_telnet_spot(spot):
    de = spot.get("de", "")
    dx = spot.get("dx", "")
    freq = spot.get("freq", "")
    comment = spot.get("comment", "")
    t = time.gmtime(spot.get("time", 0))
    utc_str = f"{t.tm_hour:02d}{t.tm_min:02d}"
    return f"DX de {de}:  {freq:>9s}  {dx:<10s}  {comment}  {utc_str}Z\r\n"


def _broadcast_telnet(spot):
    line = _format_telnet_spot(spot)
    with TELNET_LOCK:
        dead = []
        for client in TELNET_CLIENTS:
            try:
                client.sendall(line.encode("utf-8"))
            except Exception:
                dead.append(client)
        for c in dead:
            TELNET_CLIENTS.remove(c)


def _telnet_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        srv.bind(("0.0.0.0", TELNET_PORT))
    except OSError as e:
        print(f"[TELNET] Cannot bind port {TELNET_PORT}: {e}", flush=True)
        return
    srv.listen(5)
    srv.settimeout(1)
    print(f"[TELNET] Listening on port {TELNET_PORT}", flush=True)

    banner = (
        "Welcome to 72 DX Cluster telnet server\r\n"
        "Spots from RBN (R4NCU) + user spots\r\n"
        "Type Q to disconnect.\r\n"
    )

    while True:
        try:
            client, addr = srv.accept()
            print(f"[TELNET] Client connected: {addr}", flush=True)
            client.settimeout(300)
            try:
                client.sendall(banner.encode("utf-8"))
            except Exception:
                client.close()
                continue
            with TELNET_LOCK:
                TELNET_CLIENTS.append(client)
            threading.Thread(target=_telnet_client_handler, args=(client,), daemon=True).start()
        except socket.timeout:
            continue
        except Exception as e:
            print(f"[TELNET] Accept error: {e}", flush=True)
            time.sleep(1)


def _telnet_client_handler(client):
    buf = b""
    try:
        while True:
            try:
                data = client.recv(256)
                if not data:
                    break
                print(f"[TELNET] Raw bytes: {data!r}", flush=True)
                buf += data
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.decode("utf-8", errors="ignore").strip()
                    if not line:
                        continue
                    print(f"[TELNET] Received line: {line!r}", flush=True)
                    cmd = line.upper()
                    if cmd in ("Q", "QUIT", "BYE", "EXIT"):
                        return
                    parsed = _parse_telnet_spot(line)
                    if parsed:
                        _ingest_telnet_spot(parsed)
                    else:
                        print(f"[TELNET] Unrecognized: {line!r}", flush=True)
            except socket.timeout:
                continue
            except Exception:
                break
    finally:
        with TELNET_LOCK:
            if client in TELNET_CLIENTS:
                TELNET_CLIENTS.remove(client)
        try:
            client.close()
        except Exception:
            pass
        print(f"[TELNET] Client disconnected", flush=True)


def _parse_telnet_spot(line):
    import re
    # Standard: DX de R4NCU: 14035.0 DL5AW CQ 0515Z
    m = re.match(r"DX\s+de\s+(\S+?):\s+([\d.]+)\s+(\S+)\s+(.*?)\s+(\d{4})Z", line, re.IGNORECASE)
    if m:
        return {
            "de": m.group(1).upper(),
            "freq": m.group(2),
            "dx": m.group(3).upper(),
            "comment": m.group(4).strip(),
            "time": int(time.time()),
        }
    # RumlogNG: dx 14018.0 RT5T CW
    m = re.match(r"dx\s+([\d.]+)\s+(\S+)\s*(.*)", line, re.IGNORECASE)
    if m:
        return {
            "de": "R4NCU",
            "freq": m.group(1),
            "dx": m.group(2).upper(),
            "comment": m.group(3).strip(),
            "time": int(time.time()),
        }
    return None


def _ingest_telnet_spot(parsed):
    global _user_spot_id
    spot = {
        "id": _user_spot_id,
        "de": parsed["de"][:12],
        "dx": parsed["dx"][:12],
        "freq": parsed["freq"][:10],
        "comment": parsed.get("comment", ""),
        "time": parsed["time"],
    }
    with _user_spot_id_lock:
        _user_spot_id += 1
    with _user_spots_lock:
        _user_spots.append(spot)
        if len(_user_spots) > 500:
            _user_spots.pop(0)
    _broadcast_telnet(spot)
    print(f"[TELNET] Spot ingested: {spot['de']} -> {spot['dx']} @ {spot['freq']}", flush=True)


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    print(f"DX Cluster running at http://localhost:{PORT}")
    print(f"Telnet on port {TELNET_PORT}")
    threading.Thread(target=_telnet_server, daemon=True).start()
    threading.Thread(target=_rbn_poller, daemon=True).start()
    app.run(host="::", port=PORT, debug=debug)
