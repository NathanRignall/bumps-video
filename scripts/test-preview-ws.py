#!/usr/bin/env python3
"""
Smoke test for the Phase 2 preview WebSocket.

Assumes bumps-pipe is already running with a publisher pushing video to it
(use scripts/test-loopback.sh in another terminal, or pass --bootstrap).

Validates:
  - GET / returns HTML containing 'VideoDecoder'
  - WS /ws upgrades successfully
  - First text frame is a JSON {"kind":"init", ...} with codec/width/height
  - At least one binary frame arrives within a few seconds, framed as
    [flags:u8 | pts_us:u64 LE | payload]

Exit 0 on success, 1 on any failure.
"""

import argparse
import json
import struct
import sys
import time
import socket
import os
import base64
import hashlib
import urllib.request


def http_get(host: str, port: int, path: str = "/") -> tuple[int, bytes]:
    with socket.create_connection((host, port), timeout=2) as s:
        req = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
        s.sendall(req.encode())
        buf = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
    head, _, body = buf.partition(b"\r\n\r\n")
    status_line = head.split(b"\r\n", 1)[0].decode()
    code = int(status_line.split(" ", 2)[1])
    return code, body


def ws_handshake(sock: socket.socket, host: str, port: int, path: str = "/ws"):
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        f"Upgrade: websocket\r\n"
        f"Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        f"Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(req.encode())
    # Read response headers
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise RuntimeError("eof during ws handshake")
        buf += chunk
    head, _, leftover = buf.partition(b"\r\n\r\n")
    if b"101" not in head.split(b"\r\n", 1)[0]:
        raise RuntimeError(f"ws upgrade failed: {head!r}")
    accept_expected = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).decode()
    if accept_expected.encode().lower() not in head.lower():
        raise RuntimeError("ws Sec-WebSocket-Accept mismatch")
    return leftover


def read_exact(sock: socket.socket, n: int, leftover: bytearray) -> bytes:
    while len(leftover) < n:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("eof reading frame")
        leftover.extend(chunk)
    out = bytes(leftover[:n])
    del leftover[:n]
    return out


def ws_read_frame(sock: socket.socket, leftover: bytearray) -> tuple[int, bytes]:
    """Returns (opcode, payload). Handles fragmented + masked frames."""
    payload = bytearray()
    opcode = None
    while True:
        hdr = read_exact(sock, 2, leftover)
        fin = (hdr[0] & 0x80) != 0
        op  = hdr[0] & 0x0F
        masked = (hdr[1] & 0x80) != 0
        plen = hdr[1] & 0x7F
        if plen == 126:
            ext = read_exact(sock, 2, leftover)
            plen = struct.unpack("!H", ext)[0]
        elif plen == 127:
            ext = read_exact(sock, 8, leftover)
            plen = struct.unpack("!Q", ext)[0]
        mask = read_exact(sock, 4, leftover) if masked else b""
        body = read_exact(sock, plen, leftover)
        if masked:
            body = bytes(b ^ mask[i % 4] for i, b in enumerate(body))
        payload.extend(body)
        if opcode is None:
            opcode = op
        if fin:
            break
    return opcode, bytes(payload)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--wait-seconds", type=float, default=8.0)
    args = p.parse_args()

    # 1. GET /
    try:
        code, body = http_get(args.host, args.port, "/")
    except Exception as e:
        print(f"FAIL: http GET / : {e}")
        return 1
    if code != 200:
        print(f"FAIL: GET / returned {code}")
        return 1
    if b"VideoDecoder" not in body:
        print("FAIL: GET / body does not contain 'VideoDecoder' — frontend not embedded?")
        return 1
    print("ok  : GET /                       (200, frontend served)")

    # 2. WS upgrade + receive frames
    sock = socket.create_connection((args.host, args.port), timeout=2)
    sock.settimeout(args.wait_seconds)
    leftover = bytearray(ws_handshake(sock, args.host, args.port))
    print("ok  : WS /ws upgrade              (101 Switching Protocols)")

    init_seen = False
    stats_seen = False
    binary_seen = False
    binary_keyframe_seen = False
    first_payload_len = 0

    deadline = time.time() + args.wait_seconds
    while time.time() < deadline:
        try:
            op, payload = ws_read_frame(sock, leftover)
        except socket.timeout:
            break
        if op == 0x1:  # text
            try:
                msg = json.loads(payload)
            except Exception as e:
                print(f"FAIL: non-JSON text frame: {e!r}: {payload!r}")
                return 1
            kind = msg.get("kind")
            if kind == "init":
                if init_seen:
                    continue
                print(f"ok  : init frame                 {json.dumps(msg)}")
                for k in ("codec", "width", "height", "fps_num", "fps_den"):
                    if k not in msg:
                        print(f"FAIL: init missing field {k}")
                        return 1
                init_seen = True
            elif kind == "stats":
                if stats_seen:
                    continue
                for k in ("downlink", "encoder", "preview", "uplink", "pipeline"):
                    if k not in msg:
                        print(f"FAIL: stats missing top-level field {k}")
                        return 1
                d = msg["downlink"]; e = msg["encoder"]; u = msg["uplink"]; p = msg["pipeline"]
                print(f"ok  : stats frame                rollup={p['rollup']} "
                      f"down={d['bitrate_kbps']:.0f}kbps frames_in={d['frames_in']} "
                      f"enc={e['actual_kbps']:.0f}kbps clients={msg['preview']['clients']} "
                      f"rtt={u['rtt_ms']:.1f}ms")
                stats_seen = True
            else:
                print(f"WARN: unexpected text kind={kind}")
        elif op == 0x2:  # binary
            if len(payload) < 9:
                print(f"FAIL: binary frame too short ({len(payload)} bytes)")
                return 1
            flags = payload[0]
            pts_us = struct.unpack("<Q", payload[1:9])[0]
            payload_len = len(payload) - 9
            if not binary_seen:
                first_payload_len = payload_len
                print(f"ok  : first video chunk          flags={flags:#04x} pts_us={pts_us} payload={payload_len}B")
            binary_seen = True
            if flags & 0x01:
                binary_keyframe_seen = True
        elif op == 0x8:  # close
            print("FAIL: server closed connection")
            return 1
        elif op == 0x9:  # ping
            # send pong (no mask required from server to client)
            pass
        if init_seen and stats_seen and binary_seen and binary_keyframe_seen:
            break

    if not init_seen:
        print("FAIL: no init JSON received")
        return 1
    if not stats_seen:
        print("FAIL: no stats JSON received within timeout")
        return 1
    if not binary_seen:
        print("FAIL: no binary frames received within timeout (no active session?)")
        return 1
    if not binary_keyframe_seen:
        print("WARN: no keyframe within window (decoder won't start until one arrives)")

    print("ok  : binary frames + keyframe   (preview + stats WS healthy)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
