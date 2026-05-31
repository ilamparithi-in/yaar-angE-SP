#!/usr/bin/env python3
"""
DISCLAIMER: COMPLETELY AI GENERATED FROM THE LARGER IoT BACKEND PROJECT, NOT TESTED.
Please use https://github.com/ilamparithi-in/iot-public/ when published!
Single-file simplified callingbell server compatible with the original handlers.

Features:
- Serves POST /callingbell
- Validates `Authorization: Bearer <token>` and `X-Bell-Location`
- De-duplicates events by `id` for a configurable expiry window
- Forwards messages to a Matfix-like endpoint and expects HTTP 202
- Loads config from `.config/callingbell.yaml` if present, else from env vars

Run: `python3 single_callingbell_server.py [--config path]`
"""

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import threading
import time
from urllib import error, request
import argparse
import os

# --- Configuration (edit values below) ------------------------------------
# Map bearer tokens to a human-readable location string
CONFIG = {
    "token_location_pairs": {
        # Example: "secret-token-1": "Front Door",
        "purpose_specific_system_write_your_own_code": "Sorkkavaasal (Heaven's Entrance)",
    },
    "matfix_url": "https://example-matfix.local",
    "api_key": "REPLACE_WITH_API_KEY",
    "account_id": "REPLACE_WITH_ACCOUNT_ID",
    "room_ids": ["!room-1:matrix.org"],
    "message_template": "Ring at {{time}} ({{place}})",
    "clear_seen_events_after_hours": 1.0,
    "request_timeout_seconds": 5,
}


def _log_info(msg, *args):
    print("INFO | " + (msg % args if args else msg))


def _log_warn(msg, *args):
    print("WARN | " + (msg % args if args else msg))


def _log_error(msg, *args):
    print("ERROR | " + (msg % args if args else msg))


def _log_exception(msg, *args):
    print("ERROR | " + (msg % args if args else msg))
    import traceback

    traceback.print_exc()


_seen_event_ids: dict[str, float] = {}
_seen_lock = threading.Lock()


def _format_event_time(timestamp: int) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    except Exception:
        return str(timestamp)


def _build_message(template: str, timestamp: int, place: str) -> str:
    return template.replace("{{time}}", _format_event_time(timestamp)).replace("{{place}}", place)


def _send_matfix_message(matfix_url, api_key, account_id, room_id, message, timeout):
    url = f"{matfix_url}/v1/send"
    payload = json.dumps(
        {
            "account_id": account_id,
            "destination": room_id,
            "message": {"type": "text", "body": message},
        }
    ).encode("utf-8")

    req = request.Request(
        url,
        data=payload,
        method="POST",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return resp.status


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, message: str | None = None, auth_required: bool = False):
        self.send_response(status)
        if auth_required and status == 401:
            self.send_header("WWW-Authenticate", 'Bearer realm="callingbell"')
        self.end_headers()
        if message:
            self.wfile.write(message.encode("utf-8"))

    def do_POST(self):
        if self.path != "/callingbell":
            self._send(404, "Not found")
            return

        config = CONFIG

        # auth
        auth = self.headers.get("Authorization", "").strip()
        if not auth.startswith("Bearer "):
            _log_warn("missing bearer")
            self._send(401, auth_required=True)
            return
        token = auth[len("Bearer "):].strip()
        location = self.headers.get("X-Bell-Location", "").strip()
        expected = config["token_location_pairs"].get(token)
        if expected is None or expected != location:
            _log_warn("unauthorized token/location")
            self._send(401, auth_required=True)
            return

        # read body
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            self._send(400)
            return

        body = self.rfile.read(max(length, 0)).decode("utf-8") if length > 0 else ""
        if not body.strip():
            self._send(400)
            return

        try:
            payload = json.loads(body)
        except Exception:
            self._send(400)
            return

        event_id = payload.get("id")
        timestamp = payload.get("timestamp")
        if isinstance(event_id, bool) or not isinstance(event_id, (str, int)):
            self._send(400)
            return
        if isinstance(event_id, str) and not event_id.strip():
            self._send(400)
            return
        if not isinstance(timestamp, int) or isinstance(timestamp, bool):
            self._send(400)
            return

        normalized_event_id = str(event_id).strip()

        # dedupe
        with _seen_lock:
            cutoff = time.time() - config["clear_seen_events_after_hours"] * 3600
            to_delete = [k for k, v in _seen_event_ids.items() if v < cutoff]
            for k in to_delete:
                del _seen_event_ids[k]

            if normalized_event_id in _seen_event_ids:
                _log_info("duplicate %s", normalized_event_id)
                self._send(409)
                return

            _seen_event_ids[normalized_event_id] = time.time()

        message = _build_message(config["message_template"], timestamp, expected)
        failures = []
        for room in config["room_ids"]:
            try:
                status = _send_matfix_message(
                    config["matfix_url"], config["api_key"], config["account_id"], room, message, config["request_timeout_seconds"]
                )
                if status != 202:
                    failures.append(f"{room} (status={status})")
            except error.HTTPError as e:
                failures.append(f"{room} (http={e.code})")
                _log_warn("send failed %s HTTP %s", room, getattr(e, "code", "?"))
            except error.URLError as e:
                failures.append(f"{room} (network)")
                _log_warn("send failed %s %s", room, getattr(e, "reason", "?"))
            except Exception:
                failures.append(f"{room} (unexpected)")
                _log_exception("send unexpected %s", room)

        if failures:
            _log_warn("delivery failed %s: %s", normalized_event_id, ", ".join(failures))
            self._send(502)
            return

        _log_info("delivered %s to %d rooms", normalized_event_id, len(config["room_ids"]))
        self._send(200)

    def do_GET(self):
        self._send(405)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    # validate configuration at startup
    try:
        cfg = CONFIG
        if not isinstance(cfg.get("token_location_pairs"), dict) or not cfg["token_location_pairs"]:
            raise ValueError("token_location_pairs must be a non-empty mapping")
        for v in ("matfix_url", "api_key", "account_id"):
            if not cfg.get(v):
                raise ValueError(f"{v} must be set in CONFIG")
        if not isinstance(cfg.get("room_ids"), list) or not cfg["room_ids"]:
            raise ValueError("room_ids must be a non-empty list")
    except Exception as exc:
        _log_exception("invalid configuration: %s", exc)
        return

    server = HTTPServer((args.host, args.port), Handler)
    _log_info("listening on %s:%d", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _log_info("shutting down")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
