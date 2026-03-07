# jlab_tray.py  (Windows)
# Advanced JupyterLab tray helper.
#
# USAGE:
#   python jlab_tray.py              -> Launches tray in background.
#   python jlab_tray.py [notebook]   -> Opens notebook.
#
# TROUBLESHOOTING:
#   Check log at: %TEMP%\jlab_tray.log  (rotating)

from __future__ import annotations

import argparse
import functools
import glob
import json
import logging
import os
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import uuid
import webbrowser
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

# ---------------- Constants ----------------
APP_ID = "jlab_tray"
LOG_FILE = Path(tempfile.gettempdir()) / "jlab_tray.log"
IPC_INFO_FILE = Path(tempfile.gettempdir()) / "jlab_tray_ipc.json"

# Timeouts / tuning knobs
IPC_CONNECT_TIMEOUT = 0.5
IPC_READ_TIMEOUT = 0.8
DISCOVERY_MONITOR_INTERVAL = 2.0
OPEN_WAIT_FOR_SERVER_SEC = 15.0
WAIT_FOR_TOKEN_SEC = 2.0
HTTP_SHUTDOWN_TIMEOUT = 1.0


# ---------------- Logging ----------------
# Rotating logs makes intermittent failures easier to debug.
# Set env var JLAB_TRAY_LOG_LEVEL=DEBUG for more detail.
LOG_LEVEL_NAME = (os.environ.get("JLAB_TRAY_LOG_LEVEL") or "INFO").upper().strip()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)

_handler = RotatingFileHandler(LOG_FILE, maxBytes=512 * 1024, backupCount=3, encoding="utf-8")
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[_handler],
)


# ---------------- Imports & Dep Check ----------------
try:
    import pystray
    from PIL import Image, ImageDraw
    from pystray import Menu, MenuItem

    import win32api  # check for pywin32
except ImportError as e:
    # If this is the main process (foreground), warn the user
    if "--foreground" in sys.argv or "jlab_tray.py" in sys.argv[0]:
        print(f"!! Missing dependencies: {e}")
        print(f"   Run: {sys.executable} -m pip install -U pystray pillow pywin32")
    sys.exit(1)


# ---------------- Small utilities ----------------

def _redact_token(url: str) -> str:
    """Return a URL with any `token=` query param value replaced."""
    try:
        p = urlsplit(url)
        if not p.query or "token=" not in p.query:
            return url
        q = [(k, "REDACTED" if k == "token" else v) for k, v in parse_qsl(p.query, keep_blank_values=True)]
        return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q, doseq=True), p.fragment))
    except Exception:
        # Best-effort fallback
        return url.replace("token=", "token=REDACTED")


# ---------------- Helpers ----------------

def _tcp_alive(url: str, timeout: float = 0.2) -> bool:
    try:
        p = urlsplit(url)
        host = p.hostname or "127.0.0.1"
        if host in ("0.0.0.0", "::"):
            host = "127.0.0.1"
        port = p.port or (443 if p.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_jupyter_runtime_dir() -> Path | None:
    # 1. Ask jupyter --paths
    try:
        cmd = [sys.executable, "-m", "jupyter", "--paths", "--json"]
        out = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
        data = json.loads(out)
        runtime_dirs = data.get("runtime", [])
        if runtime_dirs:
            return Path(runtime_dirs[0])
    except Exception:
        logging.debug("Failed to get runtime dir via jupyter --paths: %s", traceback.format_exc())

    # 2. Fallback
    appdata = os.environ.get("APPDATA")
    if appdata:
        default_path = Path(appdata) / "jupyter" / "runtime"
        if default_path.exists():
            return default_path
    return None


def _norm_host(host: str | None) -> str:
    """Normalize hostnames so the same local server doesn't show up multiple times."""
    if not host:
        return "127.0.0.1"
    h = str(host).strip().lower()
    if h in ("localhost", "127.0.0.1", "0.0.0.0", "::", "::1"):
        return "127.0.0.1"
    return h


def _extract_host_port(url: str, fallback_port: int | str | None = None) -> tuple[str, int | None]:
    try:
        p = urlsplit(url)
        host = _norm_host(p.hostname)
        port = p.port if p.port is not None else fallback_port
        if port is None:
            return host, None
        return host, int(port)
    except Exception:
        return "127.0.0.1", None


def _pid_int(pid) -> int | None:
    try:
        if pid is None:
            return None
        return int(pid)
    except Exception:
        return None


def _server_score(d: dict) -> int:
    """Heuristic for choosing the best record among duplicates."""
    score = 0
    if d.get("token"):
        score += 5
    if d.get("root_dir"):
        score += 3
    if d.get("base_url"):
        score += 2
    if d.get("_source_file", "").lower().startswith("jpserver-"):
        score += 1
    if d.get("pid") is not None:
        score += 1
    return score


def list_live_servers(runtime_dir: Path | None):
    """Discover running Jupyter servers via runtime JSON files, deduped by (host, port)."""
    if not runtime_dir or not runtime_dir.exists():
        return []

    patterns = [runtime_dir / "nbserver-*.json", runtime_dir / "jpserver-*.json"]
    files: list[str] = []
    for pat in patterns:
        files.extend(glob.glob(str(pat)))

    unique: dict[tuple[str, int], dict] = {}

    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)

            url = data.get("url")
            if not url:
                continue

            host, port = _extract_host_port(url, data.get("port"))
            if port is None:
                continue

            # Normalize/augment
            data["port"] = port
            data["pid"] = _pid_int(data.get("pid"))
            data["_host"] = host
            data["_source_file"] = Path(fpath).name

            if not _tcp_alive(url):
                continue

            key = (host, port)
            if key not in unique or _server_score(data) > _server_score(unique[key]):
                unique[key] = data

        except json.JSONDecodeError:
            logging.debug("Bad JSON in runtime file %s", fpath)
        except OSError:
            logging.debug("Failed to read runtime file %s", fpath)
        except Exception:
            logging.debug("Unexpected error while scanning %s: %s", fpath, traceback.format_exc())

    final_list = list(unique.values())
    final_list.sort(key=lambda d: (int(d.get("port", 0)), str(d.get("_host", ""))))
    return final_list


# ---------------- URL + server selection helpers ----------------

def _server_root_url(server: dict) -> str:
    """Base URL for API requests (scheme://host:port + base_url)."""
    base = str(server.get("url", "") or "")
    if not base.endswith("/"):
        base += "/"

    base_url = str(server.get("base_url") or "/")
    if not base_url.startswith("/"):
        base_url = "/" + base_url
    if not base_url.endswith("/"):
        base_url += "/"

    return urljoin(base, base_url)


def lab_url(server: dict, target: Path | None = None) -> str:
    root = _server_root_url(server)
    token = str(server.get("token", "") or "")

    if target is None:
        url = urljoin(root, "lab")
    else:
        root_dir = str(server.get("root_dir", "") or "")
        try:
            rel = target.resolve().relative_to(Path(root_dir).resolve())
            lab_path = rel.as_posix()
        except Exception:
            lab_path = target.resolve().as_posix()
        url = urljoin(root, "lab/tree/") + quote(lab_path, safe="/-._~")

    if token and "token=" not in url:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}token={quote(token)}"
    return url


def _server_can_see_target(server: dict, target: Path) -> bool:
    root_dir = server.get("root_dir")
    if not root_dir:
        return False
    try:
        target.resolve().relative_to(Path(str(root_dir)).resolve())
        return True
    except Exception:
        return False


def _choose_best_server(servers: list[dict], target: Path | None) -> dict | None:
    if not servers:
        return None
    if target is None:
        return max(servers, key=_server_score)

    visible = [s for s in servers if _server_can_see_target(s, target)]
    if visible:
        return max(visible, key=_server_score)

    return max(servers, key=_server_score)


def _best_server_for_host_port(
    runtime_dir: Path | None,
    host: str,
    port: int,
    wait_for_token: float = WAIT_FOR_TOKEN_SEC,
) -> dict | None:
    """Re-read runtime JSON at click time to avoid capturing token-less records."""
    deadline = time.time() + max(0.0, float(wait_for_token))
    best: dict | None = None

    while True:
        try:
            servers = list_live_servers(runtime_dir)
            matches = [s for s in servers if s.get("_host") == host and int(s.get("port", -1)) == int(port)]
            if matches:
                best = max(matches, key=_server_score)
                if best.get("token") or time.time() >= deadline:
                    return best
        except Exception:
            logging.debug("_best_server_for_host_port error: %s", traceback.format_exc())

        if time.time() >= deadline:
            return best
        time.sleep(0.1)


# ---------------- Start / Open actions ----------------

def open_browser_action(icon, item, url=None):
    if not url:
        return
    logging.info("Opening: %s", _redact_token(str(url)))
    try:
        webbrowser.open(str(url), new=2)
    except Exception:
        try:
            os.startfile(str(url))
        except Exception:
            logging.error("Failed to open browser for %s", _redact_token(str(url)))


def open_server_action(icon, item, app=None, host=None, port=None, target: Path | None = None):
    """Open a server by (host, port), computing the URL at click time."""
    if app is None or host is None or port is None:
        return

    try:
        host_n = _norm_host(str(host))
        port_n = int(port)
    except Exception:
        return

    s = _best_server_for_host_port(getattr(app, "runtime_dir", None), host_n, port_n)
    if not s:
        # Fallback: open a reasonable base URL (may still require manual auth).
        base = f"http://{host_n}:{port_n}/"
        open_browser_action(icon, item, url=urljoin(base, "lab"))
        return

    open_browser_action(icon, item, url=lab_url(s, target))


def _root_for_new_server(target: Path | None) -> Path | None:
    if target is None:
        return None
    try:
        # Existing paths: directories are the root, files use parent.
        if target.exists():
            return target if target.is_dir() else target.parent

        # Non-existent paths: best-effort heuristic.
        # If it looks like a file (has a suffix), use its parent.
        return target.parent if target.suffix else target
    except Exception:
        return None


def start_server_action(icon, item, root: Path | None = None):
    """Start a new JupyterLab server (optionally rooted at `root`)."""
    try:
        root_path = (root or Path.home()).expanduser().resolve()
    except Exception:
        root_path = Path.home()

    logging.info("Starting new server (root=%s)...", str(root_path))

    flags = 0
    if os.name == "nt":
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)

    cmd = [
        sys.executable,
        "-m",
        "jupyterlab",
        "--no-browser",
        "--ServerApp.open_browser=False",
    ]

    # If the user is opening a specific notebook/path, root the server there so
    # /lab/tree/<relative> is reliable.
    if root is not None:
        cmd += ["--ServerApp.root_dir", str(root_path)]

    try:
        subprocess.Popen(
            cmd,
            cwd=str(root_path),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=flags,
        )
    except Exception:
        logging.error("Failed to start server: %s", traceback.format_exc())


def quit_action(icon, item, app=None):
    if app:
        app.quit()


# ---------------- Shutdown (Priority 1) ----------------

def _shutdown_api_url(server: dict) -> str:
    return urljoin(_server_root_url(server), "api/shutdown")


def _http_shutdown(server: dict, timeout: float = HTTP_SHUTDOWN_TIMEOUT) -> bool:
    """Best-effort graceful shutdown via Jupyter's /api/shutdown."""
    base = _shutdown_api_url(server)
    token = str(server.get("token") or "")

    # A few variants; the exact auth expectations can vary by server flavor.
    attempts: list[tuple[str, str, dict[str, str]]] = []

    if token:
        # Query-param token is commonly supported.
        attempts.append(("query", f"{base}?token={quote(token)}", {}))
        # Header-based auth is supported by some setups.
        attempts.append(("auth-token", base, {"Authorization": f"token {token}"}))
        attempts.append(("auth-bearer", base, {"Authorization": f"Bearer {token}"}))
    else:
        attempts.append(("noauth", base, {}))

    probe_url = str(server.get("url") or "")

    for label, url, headers in attempts:
        try:
            req = Request(url, data=b"", method="POST", headers=headers)
            with urlopen(req, timeout=float(timeout)) as resp:
                code = getattr(resp, "status", None) or resp.getcode()
                if 200 <= int(code) < 400:
                    logging.info("Shutdown via HTTP succeeded (%s): %s", label, _redact_token(url))
                    return True
        except HTTPError as e:
            # 403/404/etc means the endpoint is reachable but not allowed.
            logging.debug("HTTP shutdown failed (%s) %s: HTTPError %s", label, _redact_token(url), e.code)
        except URLError as e:
            logging.debug("HTTP shutdown failed (%s) %s: URLError %s", label, _redact_token(url), e)
        except Exception:
            logging.debug("HTTP shutdown exception (%s) %s: %s", label, _redact_token(url), traceback.format_exc())

        # If the server stopped quickly (sometimes it drops the connection), treat as success.
        try:
            time.sleep(0.2)
            if probe_url and not _tcp_alive(probe_url):
                logging.info("Server appears down after HTTP shutdown attempt (%s).", label)
                return True
        except Exception:
            pass

    return False


def _netstat_listening_pids(port: int) -> set[int] | None:
    """Return PIDs listening on TCP *port* (Windows best-effort), or None if unknown."""
    if os.name != "nt":
        return None

    try:
        flags = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
        out = subprocess.check_output(
            ["netstat", "-ano"],
            stderr=subprocess.DEVNULL,
            creationflags=flags,
        )
        text = out.decode(errors="replace")

        pids: set[int] = set()
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if not line.upper().startswith("TCP"):
                continue

            parts = line.split()
            # Proto  Local Address          Foreign Address        State           PID
            if len(parts) < 5:
                continue

            local_addr = parts[1]
            state = parts[3]
            pid_str = parts[4]

            if not local_addr.endswith(f":{port}"):
                continue
            if not state.upper().startswith("LISTEN"):
                continue

            try:
                pids.add(int(pid_str))
            except Exception:
                continue

        return pids

    except Exception:
        logging.debug("netstat failed: %s", traceback.format_exc())
        return None


def _kill_pid(pid: int, host: str | None = None, port: int | None = None) -> bool:
    """Last-resort termination; tries to reduce PID-reuse risk on Windows."""
    try:
        if pid <= 0:
            return False

        if port is not None:
            pids = _netstat_listening_pids(int(port))
            if pids is not None and pid not in pids:
                logging.warning(
                    "Refusing to kill pid=%s for %s:%s (netstat shows %s)",
                    pid,
                    host or "?",
                    port,
                    sorted(pids),
                )
                return False

        os.kill(pid, signal.SIGTERM)
        logging.info("Killed process pid=%s (%s:%s)", pid, host or "?", port or "?")
        return True

    except Exception:
        logging.error("Failed to kill pid=%s: %s", pid, traceback.format_exc())
        return False


def shutdown_server_action(icon, item, app=None, host=None, port=None, pid=None):
    """Attempt graceful shutdown; fallback to PID termination if needed."""
    if host is None or port is None:
        return

    try:
        host_n = _norm_host(str(host))
        port_n = int(port)
    except Exception:
        return

    server = None
    if app is not None:
        server = _best_server_for_host_port(getattr(app, "runtime_dir", None), host_n, port_n)

    # Fallback record if runtime scan can't find it at click time.
    if not server:
        server = {
            "url": f"http://{host_n}:{port_n}/",
            "base_url": "/",
            "token": "",
            "pid": _pid_int(pid),
            "port": port_n,
            "_host": host_n,
        }

    # Try API shutdown first.
    try:
        if _http_shutdown(server):
            return
    except Exception:
        logging.debug("HTTP shutdown wrapper failed: %s", traceback.format_exc())

    # Fallback to kill.
    pid_i = _pid_int(server.get("pid"))
    if pid_i:
        _kill_pid(pid_i, host=host_n, port=port_n)
    else:
        logging.warning("No PID available; could not force shutdown for %s:%s", host_n, port_n)


# ---------------- IPC (Priority 3) ----------------

def _read_ipc_info() -> dict | None:
    try:
        if not IPC_INFO_FILE.exists():
            return None
        data = json.loads(IPC_INFO_FILE.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        if data.get("app") != APP_ID:
            return None
        # Normalize
        if "port" in data:
            data["port"] = int(data["port"])
        return data
    except Exception:
        logging.debug("Failed to read IPC info file: %s", traceback.format_exc())
        return None


def _write_ipc_info(port: int, instance_id: str):
    try:
        data = {
            "app": APP_ID,
            "port": int(port),
            "instance_id": str(instance_id),
            "pid": int(os.getpid()),
            "updated": time.time(),
        }
        tmp = IPC_INFO_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data), encoding="utf-8")
        tmp.replace(IPC_INFO_FILE)
        logging.debug("Wrote IPC info: %s", data)
    except Exception:
        logging.debug("Failed to write IPC info: %s", traceback.format_exc())


def _clear_ipc_info(instance_id: str | None = None):
    try:
        if not IPC_INFO_FILE.exists():
            return
        if instance_id is not None:
            data = _read_ipc_info() or {}
            if data.get("instance_id") != instance_id:
                return
        IPC_INFO_FILE.unlink(missing_ok=True)
    except Exception:
        logging.debug("Failed to clear IPC info: %s", traceback.format_exc())


def _ipc_call(port: int, msg: dict, timeout: float = IPC_CONNECT_TIMEOUT) -> dict | None:
    """Send a single JSON line to the IPC server and read one JSON line response."""
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=float(timeout)) as s:
            s.settimeout(float(IPC_READ_TIMEOUT))
            payload = (json.dumps(msg) + "\n").encode("utf-8")
            s.sendall(payload)

            # Read up to one line.
            data = b""
            while b"\n" not in data and len(data) < 64 * 1024:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk

            if not data:
                return None

            line = data.decode("utf-8", errors="replace").splitlines()[0].strip()
            if not line:
                return None
            return json.loads(line)

    except Exception:
        logging.debug("IPC call failed (port=%s msg=%s): %s", port, msg.get("cmd"), traceback.format_exc())
        return None


def _ipc_ping(port: int, expected_instance_id: str | None = None) -> bool:
    resp = _ipc_call(int(port), {"cmd": "ping"}, timeout=IPC_CONNECT_TIMEOUT)
    if not resp or resp.get("app") != APP_ID or not resp.get("ok"):
        return False
    if expected_instance_id is not None and resp.get("instance_id") != expected_instance_id:
        return False
    return True


class IPCHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            line = self.rfile.readline().decode("utf-8", errors="replace").strip()
            if not line:
                return
            msg = json.loads(line)
            cmd = msg.get("cmd")

            if cmd == "ping":
                resp = {
                    "app": APP_ID,
                    "ok": True,
                    "instance_id": getattr(self.server.app, "instance_id", ""),
                    "pid": int(os.getpid()),
                    "port": int(self.server.server_address[1]),
                }
                self.wfile.write((json.dumps(resp) + "\n").encode("utf-8"))
                self.wfile.flush()
                return

            if cmd == "open":
                path = msg.get("path")
                p = Path(path) if path else None
                threading.Thread(target=self.server.app.handle_open_request, args=(p,), daemon=True).start()

                self.wfile.write((json.dumps({"app": APP_ID, "ok": True}) + "\n").encode("utf-8"))
                self.wfile.flush()
                return

            # Unknown command.
            self.wfile.write((json.dumps({"app": APP_ID, "ok": False, "error": "unknown_cmd"}) + "\n").encode("utf-8"))
            self.wfile.flush()

        except Exception:
            logging.debug("IPC handler error: %s", traceback.format_exc())


class IPCServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, handler, app):
        super().__init__(addr, handler)
        self.app = app


# ---------------- Tray App ----------------
class TrayApp:
    def __init__(self, ipc_port: int, initial_path: Path | None):
        self.ipc_port = int(ipc_port)
        self.initial_path = initial_path
        self.runtime_dir = get_jupyter_runtime_dir()
        self.instance_id = uuid.uuid4().hex
        self.icon = None
        self._ipc: IPCServer | None = None
        self._stop = threading.Event()
        self._cached_servers: list[dict] = []
        logging.info("Runtime Dir: %s", self.runtime_dir)

    def _make_icon(self):
        ico_path = Path(__file__).resolve().parent / "jupyterlab.ico"
        if ico_path.exists():
            try:
                return Image.open(ico_path)
            except Exception:
                logging.debug("Failed to load .ico: %s", traceback.format_exc())

        img = Image.new("RGB", (64, 64), (243, 119, 38))
        d = ImageDraw.Draw(img)
        d.rectangle((16, 16, 48, 48), fill=(255, 255, 255))
        return img

    def _build_menu(self):
        items = []

        items.append(MenuItem("Start New Server", start_server_action))
        items.append(Menu.SEPARATOR)

        try:
            servers = list_live_servers(self.runtime_dir)
            self._cached_servers = servers
        except Exception:
            logging.debug("Menu discovery error: %s", traceback.format_exc())
            servers = []

        if not servers:
            items.append(MenuItem("No active servers", lambda i, it: None, enabled=False))
        else:
            for s in servers:
                pid = s.get("pid")
                port = s.get("port")
                root = s.get("root_dir", "?")
                host = s.get("_host") or "127.0.0.1"
                label = f"{host}:{port}  {os.path.basename(str(root)) or root}"

                open_cb = functools.partial(open_server_action, app=self, host=host, port=port)
                shutdown_cb = functools.partial(shutdown_server_action, app=self, host=host, port=port, pid=pid)

                shutdown_enabled = bool(pid) or bool(s.get("token"))

                submenu = Menu(
                    MenuItem("Open Lab", open_cb),
                    MenuItem("Shutdown", shutdown_cb, enabled=shutdown_enabled),
                )
                items.append(MenuItem(label, submenu))

        items.append(Menu.SEPARATOR)
        items.append(MenuItem("Quit Tray", functools.partial(quit_action, app=self)))

        return Menu(*items)

    def _monitor(self):
        while not self._stop.is_set():
            time.sleep(DISCOVERY_MONITOR_INTERVAL)
            try:
                current = list_live_servers(self.runtime_dir)
                curr_sig = [(s.get("_host"), s.get("port")) for s in current]
                cache_sig = [(s.get("_host"), s.get("port")) for s in self._cached_servers]

                if curr_sig != cache_sig and self.icon:
                    logging.info("Updating menu...")
                    self.icon.menu = self._build_menu()
            except Exception:
                logging.error("Monitor error: %s", traceback.format_exc())

    def handle_open_request(self, path: Path | None):
        logging.info("Open request: %s", path)

        servers = list_live_servers(self.runtime_dir)

        if not servers:
            # Priority 2A: when opening a path and no server exists, root the
            # new server at that path's directory.
            root = _root_for_new_server(path)
            start_server_action(None, None, root=root)

            deadline = time.time() + OPEN_WAIT_FOR_SERVER_SEC
            while time.time() < deadline:
                servers = list_live_servers(self.runtime_dir)
                if servers:
                    break
                time.sleep(0.5)

        if not servers:
            logging.warning("No servers found after waiting; open request ignored.")
            return

        # Priority 2B: prefer a server that can actually see the requested file.
        best = _choose_best_server(servers, path)

        if best and not best.get("token"):
            # Briefly re-scan to allow a token-bearing runtime record to appear.
            deadline2 = time.time() + WAIT_FOR_TOKEN_SEC
            while time.time() < deadline2:
                updated = list_live_servers(self.runtime_dir)
                if not updated:
                    break
                cand = _choose_best_server(updated, path)
                if cand and cand.get("token"):
                    best = cand
                    break
                time.sleep(0.1)

        if best:
            open_browser_action(None, None, url=lab_url(best, path))

    def quit(self):
        self._stop.set()

        try:
            if self._ipc:
                self._ipc.shutdown()
                self._ipc.server_close()
        except Exception:
            logging.debug("IPC shutdown error: %s", traceback.format_exc())

        _clear_ipc_info(self.instance_id)

        if self.icon:
            self.icon.stop()

    def run(self):
        # Start IPC
        try:
            try:
                self._ipc = IPCServer(("127.0.0.1", self.ipc_port), IPCHandler, self)
            except OSError as e:
                logging.warning("IPC port %s busy (%s); binding ephemeral port.", self.ipc_port, e)
                self._ipc = IPCServer(("127.0.0.1", 0), IPCHandler, self)
                self.ipc_port = int(self._ipc.server_address[1])

            threading.Thread(target=self._ipc.serve_forever, daemon=True).start()
            _write_ipc_info(self.ipc_port, self.instance_id)
            logging.info("IPC listening on 127.0.0.1:%s", self.ipc_port)

        except Exception:
            logging.error("Failed to start IPC: %s", traceback.format_exc())

        # Handle initial path
        if self.initial_path:
            threading.Thread(target=self.handle_open_request, args=(self.initial_path,), daemon=True).start()

        # Start Monitor
        threading.Thread(target=self._monitor, daemon=True).start()

        # Run Tray
        self.icon = pystray.Icon("JupyterLab", self._make_icon(), "JupyterLab Tray", menu=self._build_menu())
        self.icon.run()


# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path", nargs="?")
    ap.add_argument("--ipc-port", type=int, default=8765)
    ap.add_argument("--foreground", action="store_true")  # Debug/Internal
    args = ap.parse_args()

    target = Path(args.path).expanduser().resolve() if args.path else None

    # Priority 3: locate existing tray more safely via a handshake.
    ipc_info = _read_ipc_info()

    candidate_ports: list[int] = []
    if ipc_info and isinstance(ipc_info.get("port"), int):
        candidate_ports.append(int(ipc_info["port"]))
    candidate_ports.append(int(args.ipc_port))

    # De-dupe, keep order
    ports: list[int] = []
    for p in candidate_ports:
        if p not in ports:
            ports.append(p)

    tray_port: int | None = None

    for p in ports:
        expected_id = None
        if ipc_info and int(ipc_info.get("port", -1)) == int(p):
            expected_id = ipc_info.get("instance_id")

        if _ipc_ping(p, expected_instance_id=expected_id):
            tray_port = int(p)
            break

    if tray_port is not None:
        resp = _ipc_call(
            tray_port,
            {"cmd": "open", "path": str(target) if target else None},
            timeout=IPC_CONNECT_TIMEOUT,
        )
        if resp and resp.get("ok"):
            print(">> Command sent to existing tray.")
            return 0

    # If IPC info exists but did not ping, clear it (stale / crashed tray).
    if ipc_info and ipc_info.get("port") is not None:
        _clear_ipc_info(None)

    # Launch tray
    is_child = os.environ.get("JLAB_TRAY_CHILD") == "1"

    if args.foreground or is_child:
        try:
            TrayApp(args.ipc_port, target).run()
        except Exception:
            logging.critical(traceback.format_exc())
            traceback.print_exc()
            return 1
        return 0

    # Detach (background)
    print(">> Launching background tray...")
    print(f">> Log: {LOG_FILE}")

    env = os.environ.copy()
    env["JLAB_TRAY_CHILD"] = "1"

    script = str(Path(sys.argv[0]).resolve())
    cmd = [sys.executable, script, "--foreground", "--ipc-port", str(int(args.ipc_port))]
    if args.path:
        cmd.append(str(target))

    flags = 0
    if os.name == "nt":
        flags |= getattr(subprocess, "DETACHED_PROCESS", 0x00000008)
        flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)

    subprocess.Popen(
        cmd,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=flags,
        close_fds=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
