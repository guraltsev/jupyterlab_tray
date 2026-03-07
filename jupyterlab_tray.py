#!/usr/bin/env python
"""JupyterLab Tray Helper (Windows-focused)

Design goals:
- `--help` must always print (even if GUI deps are missing).
- Single-instance via local IPC + handshake + port file in %TEMP%.
- Priority 1-4 improvements:
  1) Safer shutdown: HTTP /api/shutdown first, PID kill as fallback with safety check.
  2) Better open-path: if no server, start rooted at requested path; prefer servers that can see the path.
  3) Robust IPC: handshake + port file + ephemeral port fallback.
  4) Better diagnostics: rotating log + more debug logs.

Log: %TEMP%\\jlab_tray.log
IPC info: %TEMP%\\jlab_tray_ipc.json
"""

from __future__ import annotations

import argparse
import functools
import glob
import json
import logging
import logging.handlers
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
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, urlencode, urljoin, urlsplit, urlunsplit

APP_ID = "jlab_tray"
IPC_INFO_FILE = Path(tempfile.gettempdir()) / "jlab_tray_ipc.json"
LOG_FILE = Path(tempfile.gettempdir()) / "jlab_tray.log"

DEFAULT_IPC_PORT = 8765

MENU_REFRESH_SECONDS = 2.0
TOKEN_WAIT_SECONDS = 2.0
SERVER_START_WAIT_SECONDS = 15.0
TCP_ALIVE_TIMEOUT = 0.2
IPC_CONNECT_TIMEOUT = 0.5
HTTP_SHUTDOWN_TIMEOUT = 2.0


# -------------------- Small UX helpers --------------------

def _is_windows() -> bool:
    return os.name == "nt"


def _stdout_is_tty() -> bool:
    try:
        return bool(sys.stdout) and sys.stdout.isatty()
    except Exception:
        return False



def _has_windows_console() -> bool:
    """True if this process has a console window (Windows only)."""
    if not _is_windows():
        return True
    try:
        import ctypes
        return bool(ctypes.windll.kernel32.GetConsoleWindow())
    except Exception:
        return False


def _print_or_messagebox(title: str, text: str) -> None:
    """Prefer printing if a console exists; otherwise use a message box."""
    if _has_windows_console():
        try:
            sys.stdout.write(text.rstrip() + "\n")
            sys.stdout.flush()
            return
        except Exception:
            pass
    _message_box_error(title, text)

def _message_box_error(title: str, text: str) -> None:
    """Show a Windows message box (best effort)."""
    if not _is_windows():
        return
    try:
        import ctypes

        MB_OK = 0x00000000
        MB_ICONERROR = 0x00000010
        ctypes.windll.user32.MessageBoxW(0, text, title, MB_OK | MB_ICONERROR)
    except Exception:
        pass




def _script_dir() -> Path:
    """Best-effort directory containing this script/executable."""
    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        try:
            return Path(str(meipass)).resolve()
        except Exception:
            pass

    try:
        return Path(__file__).resolve().parent
    except Exception:
        pass

    try:
        return Path(sys.argv[0]).resolve().parent
    except Exception:
        pass

    return Path.cwd()


def _find_tray_icon_path() -> Optional[Path]:
    """Locate tray icon file.

    Prefer a `jupyterlab.ico` placed next to the script.
    Falls back to checking a few plausible runtime directories.
    """
    candidate_dirs: List[Path] = []

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        try:
            candidate_dirs.append(Path(str(meipass)))
        except Exception:
            pass

    try:
        candidate_dirs.append(Path(__file__).resolve().parent)
    except Exception:
        pass

    try:
        candidate_dirs.append(Path(sys.argv[0]).resolve().parent)
    except Exception:
        pass

    try:
        candidate_dirs.append(Path.cwd())
    except Exception:
        pass

    seen: set[str] = set()
    for d in candidate_dirs:
        try:
            d2 = d.resolve()
        except Exception:
            d2 = d

        key = str(d2)
        if key in seen:
            continue
        seen.add(key)

        try:
            p = d2 / "jupyterlab.ico"
            if p.exists() and p.is_file():
                return p
        except Exception:
            pass

    return None

def _configure_logging(level_name: str, also_console: bool) -> None:
    level = getattr(logging, (level_name or "INFO").upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)

    # Reset handlers (avoid duplicates if re-entered)
    for h in list(root.handlers):
        root.removeHandler(h)

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE,
        maxBytes=512 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    root.addHandler(file_handler)

    if also_console:
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        root.addHandler(ch)


# -------------------- Lazy GUI deps --------------------


class MissingDependencies(RuntimeError):
    pass


def _import_tray_deps():
    """Import GUI deps lazily so `--help` always works."""
    try:
        import pystray
        from pystray import Menu, MenuItem
        from PIL import Image, ImageDraw

        # Windows backend: pywin32
        import win32api  # noqa: F401

        return pystray, Menu, MenuItem, Image, ImageDraw
    except Exception as e:
        raise MissingDependencies(
            "Missing GUI dependencies. Install with:\n"
            f"  {sys.executable} -m pip install -U pystray pillow pywin32\n\n"
            f"Import error: {e}"
        )


# -------------------- Networking helpers --------------------


def _tcp_alive(url: str, timeout: float = TCP_ALIVE_TIMEOUT) -> bool:
    try:
        p = urlsplit(url)
        host = p.hostname or "127.0.0.1"
        if host in ("0.0.0.0", "::"):
            host = "127.0.0.1"
        port = p.port or (443 if p.scheme == "https" else 80)
        with socket.create_connection((host, int(port)), timeout=float(timeout)):
            return True
    except OSError:
        return False


def _norm_host(host: Optional[str]) -> str:
    if not host:
        return "127.0.0.1"
    h = str(host).strip().lower()
    if h in ("localhost", "127.0.0.1", "0.0.0.0", "::", "::1"):
        return "127.0.0.1"
    return h


def _extract_host_port(url: str, fallback_port: Optional[int] = None) -> Tuple[str, Optional[int]]:
    try:
        p = urlsplit(url)
        host = _norm_host(p.hostname)
        port = p.port if p.port is not None else fallback_port
        if port is None:
            return host, None
        return host, int(port)
    except Exception:
        return "127.0.0.1", None


def _pid_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


# -------------------- Jupyter runtime discovery --------------------


def get_jupyter_runtime_dir() -> Optional[Path]:
    # Honor env var if set
    env_dir = os.environ.get("JUPYTER_RUNTIME_DIR")
    if env_dir:
        p = Path(env_dir)
        if p.exists():
            return p

    # Ask: python -m jupyter --paths --json
    try:
        out = subprocess.check_output([sys.executable, "-m", "jupyter", "--paths", "--json"], stderr=subprocess.DEVNULL)
        data = json.loads(out)
        runtime_dirs = data.get("runtime", [])
        if runtime_dirs:
            return Path(runtime_dirs[0])
    except Exception:
        logging.debug("Failed to get runtime dir via jupyter --paths", exc_info=True)

    # Fallback
    appdata = os.environ.get("APPDATA")
    if appdata:
        p = Path(appdata) / "jupyter" / "runtime"
        if p.exists():
            return p

    return None


def _server_score(d: Dict[str, Any]) -> int:
    score = 0
    if d.get("token"):
        score += 5
    if d.get("root_dir"):
        score += 3
    if d.get("base_url"):
        score += 2
    if str(d.get("_source_file", "")).lower().startswith("jpserver-"):
        score += 1
    if d.get("pid") is not None:
        score += 1
    return score


def list_live_servers(runtime_dir: Optional[Path]) -> List[Dict[str, Any]]:
    if not runtime_dir or not runtime_dir.exists():
        return []

    patterns = [runtime_dir / "nbserver-*.json", runtime_dir / "jpserver-*.json"]
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(str(pat)))

    unique: Dict[Tuple[str, int], Dict[str, Any]] = {}

    for fpath in files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)

            url = data.get("url")
            if not url:
                continue

            host, port = _extract_host_port(url, _pid_int(data.get("port")))
            if port is None:
                continue

            data["port"] = int(port)
            data["pid"] = _pid_int(data.get("pid"))
            data["_host"] = host
            data["_source_file"] = Path(fpath).name

            if not _tcp_alive(url):
                continue

            key = (host, int(port))
            if key not in unique or _server_score(data) > _server_score(unique[key]):
                unique[key] = data

        except Exception:
            logging.debug("Failed to parse runtime file: %s", fpath, exc_info=True)

    out_list = list(unique.values())
    out_list.sort(key=lambda d: (int(d.get("port", 0)), str(d.get("_host", ""))))
    return out_list


def _server_root_url(server: Dict[str, Any]) -> str:
    base = str(server.get("url") or "")
    if not base:
        return ""
    if not base.endswith("/"):
        base += "/"

    base_url = str(server.get("base_url") or "/")
    if not base_url.startswith("/"):
        base_url = "/" + base_url
    if not base_url.endswith("/"):
        base_url += "/"

    return urljoin(base, base_url)


def _redact_token(url: str) -> str:
    try:
        parts = urlsplit(url)
        q = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True) if k.lower() != "token"]
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))
    except Exception:
        if "token=" in url:
            return url.split("token=")[0] + "token=REDACTED"
        return url


def lab_url(server: Dict[str, Any], target: Optional[Path] = None) -> str:
    root = _server_root_url(server)
    if not root.endswith("/"):
        root += "/"

    token = str(server.get("token") or "")

    if target is None:
        url = urljoin(root, "lab")
    else:
        root_dir = str(server.get("root_dir") or "")
        try:
            if root_dir:
                rel = target.resolve(strict=False).relative_to(Path(root_dir).resolve(strict=False))
                lab_path = rel.as_posix()
            else:
                lab_path = target.resolve(strict=False).as_posix()
        except Exception:
            lab_path = target.resolve(strict=False).as_posix()

        url = urljoin(root, "lab/tree/") + quote(lab_path, safe="/-._~")

    if token and "token=" not in url:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}token={quote(token)}"

    return url


# -------------------- Server selection --------------------


def _best_server_for_host_port(runtime_dir: Optional[Path], host: str, port: int, wait_for_token: float = TOKEN_WAIT_SECONDS) -> Optional[Dict[str, Any]]:
    deadline = time.time() + max(0.0, float(wait_for_token))
    best: Optional[Dict[str, Any]] = None

    while True:
        try:
            servers = list_live_servers(runtime_dir)
            matches = [s for s in servers if s.get("_host") == host and int(s.get("port", -1)) == int(port)]
            if matches:
                best = max(matches, key=_server_score)
                if best.get("token") or time.time() >= deadline:
                    return best
        except Exception:
            logging.debug("Error selecting best server", exc_info=True)

        if time.time() >= deadline:
            return best
        time.sleep(0.1)


def _path_under_root(target: Path, root_dir: str) -> bool:
    if not root_dir:
        return False
    try:
        target.resolve(strict=False).relative_to(Path(root_dir).resolve(strict=False))
        return True
    except Exception:
        return False


def _pick_best_server_for_path(servers: List[Dict[str, Any]], target: Optional[Path]) -> Optional[Dict[str, Any]]:
    if not servers:
        return None
    if target is None:
        return max(servers, key=_server_score)

    visible = [s for s in servers if _path_under_root(target, str(s.get("root_dir") or ""))]
    if visible:
        return max(visible, key=_server_score)

    return max(servers, key=_server_score)


# -------------------- Start / Open --------------------


def _creationflags_no_window() -> int:
    if not _is_windows():
        return 0
    flags = 0
    flags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    flags |= getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
    return flags


def start_server(root_dir: Path) -> None:
    root_dir = root_dir.expanduser().resolve(strict=False)

    cmd = [
        sys.executable,
        "-m",
        "jupyterlab",
        "--no-browser",
        "--ServerApp.open_browser=False",
        f"--ServerApp.root_dir={str(root_dir)}",
    ]

    logging.info("Starting new server (root_dir=%s)", str(root_dir))
    try:
        subprocess.Popen(
            cmd,
            cwd=str(root_dir),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=_creationflags_no_window(),
            close_fds=True,
        )
    except Exception:
        logging.error("Failed to start server", exc_info=True)


def open_in_browser(url: str) -> None:
    logging.info("Opening: %s", _redact_token(url))
    try:
        webbrowser.open(url, new=2)
        return
    except Exception:
        logging.debug("webbrowser.open failed", exc_info=True)

    if _is_windows():
        try:
            os.startfile(url)  # type: ignore[attr-defined]
        except Exception:
            logging.error("os.startfile failed", exc_info=True)


# -------------------- Priority 1: safer shutdown --------------------


def _http_post(url: str, timeout: float) -> Tuple[bool, str]:
    try:
        import urllib.request

        req = urllib.request.Request(url, method="POST")
        with urllib.request.urlopen(req, timeout=float(timeout)) as resp:
            if 200 <= int(resp.status) < 300:
                return True, ""
            return False, f"HTTP {resp.status}"
    except Exception as e:
        return False, str(e)


def _shutdown_via_http(server: Dict[str, Any]) -> bool:
    root = _server_root_url(server)
    if not root:
        return False

    token = str(server.get("token") or "")
    base = urljoin(root, "api/shutdown")

    urls = []
    if token:
        joiner = "&" if "?" in base else "?"
        urls.append(f"{base}{joiner}token={quote(token)}")
    urls.append(base)

    for u in urls:
        ok, err = _http_post(u, timeout=HTTP_SHUTDOWN_TIMEOUT)
        if ok:
            logging.info("Shutdown via HTTP succeeded (%s)", _redact_token(u))
            return True
        logging.debug("Shutdown via HTTP failed (%s): %s", _redact_token(u), err)

    return False


def _windows_pid_listening_on_port(pid: int, port: int) -> Optional[bool]:
    if not _is_windows():
        return None
    try:
        out = subprocess.check_output(["netstat", "-ano"], stderr=subprocess.DEVNULL)
        text = out.decode("utf-8", errors="replace")
        needle = f":{int(port)}"
        pid_str = str(int(pid))

        saw_port = False
        for line in text.splitlines():
            if needle not in line:
                continue
            if "LISTENING" not in line.upper():
                continue
            saw_port = True
            if line.strip().endswith(pid_str):
                return True

        if saw_port:
            return False
        return False
    except Exception:
        logging.debug("netstat check failed", exc_info=True)
        return None


def shutdown_server(server: Dict[str, Any]) -> None:
    # 1) Try graceful-ish HTTP shutdown
    try:
        if _shutdown_via_http(server):
            return
    except Exception:
        logging.debug("HTTP shutdown threw", exc_info=True)

    # 2) PID fallback
    pid = _pid_int(server.get("pid"))
    if not pid:
        logging.warning("No PID available; cannot force shutdown")
        return

    port = int(server.get("port") or 0)
    pid_ok = _windows_pid_listening_on_port(pid, port)
    if pid_ok is False:
        logging.error("Refusing to kill PID %s: not listening on port %s", pid, port)
        return

    try:
        logging.info("Terminating PID %s (fallback)", pid)
        os.kill(pid, signal.SIGTERM)
    except Exception:
        logging.error("Failed to terminate PID %s", pid, exc_info=True)


# -------------------- Priority 3: IPC (handshake + port file) --------------------


def _atomic_write_json(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(str(tmp), str(path))


def _write_ipc_info(port: int, instance_id: str) -> None:
    try:
        _atomic_write_json(
            IPC_INFO_FILE,
            {
                "app": APP_ID,
                "instance_id": instance_id,
                "pid": os.getpid(),
                "port": int(port),
                "updated": time.time(),
            },
        )
    except Exception:
        logging.debug("Failed to write IPC info", exc_info=True)


def _read_ipc_info() -> Optional[Dict[str, Any]]:
    try:
        if not IPC_INFO_FILE.exists():
            return None
        with open(IPC_INFO_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        if data.get("app") != APP_ID:
            return None
        return data
    except Exception:
        logging.debug("Failed to read IPC info", exc_info=True)
        return None


def _ipc_roundtrip(port: int, msg: Dict[str, Any], timeout: float = IPC_CONNECT_TIMEOUT) -> Optional[Dict[str, Any]]:
    try:
        with socket.create_connection(("127.0.0.1", int(port)), timeout=float(timeout)) as s:
            s.settimeout(float(timeout))
            s.sendall((json.dumps(msg) + "\n").encode("utf-8"))
            data = b""
            while not data.endswith(b"\n"):
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            if not data:
                return None
            return json.loads(data.decode("utf-8", errors="replace").strip())
    except Exception:
        return None


def _ipc_ping(port: int) -> Optional[Dict[str, Any]]:
    return _ipc_roundtrip(port, {"cmd": "ping"})


def _ipc_send_open(port: int, path: Optional[str]) -> bool:
    resp = _ipc_roundtrip(port, {"cmd": "open", "path": path})
    return bool(resp and resp.get("ok"))


class IPCHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        try:
            line = self.rfile.readline().decode("utf-8", errors="replace").strip()
            if not line:
                return
            msg = json.loads(line)

            cmd = msg.get("cmd")
            if cmd == "ping":
                self.wfile.write(
                    (json.dumps({"ok": True, "app": APP_ID, "instance_id": getattr(self.server, "instance_id", "")}) + "\n").encode(
                        "utf-8"
                    )
                )
                self.wfile.flush()
                return

            if cmd == "open":
                path_str = msg.get("path")
                self.wfile.write((json.dumps({"ok": True}) + "\n").encode("utf-8"))
                self.wfile.flush()

                try:
                    p = Path(path_str) if path_str else None
                except Exception:
                    p = None

                threading.Thread(target=self.server.app.handle_open_request, args=(p,), daemon=True).start()  # type: ignore[attr-defined]
                return

            self.wfile.write((json.dumps({"ok": False, "error": "unknown cmd"}) + "\n").encode("utf-8"))
            self.wfile.flush()

        except Exception:
            logging.debug("IPC handler error", exc_info=True)


class IPCServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr: Tuple[str, int], handler, app: Any, instance_id: str):
        super().__init__(addr, handler)
        self.app = app
        self.instance_id = instance_id


# -------------------- Tray app --------------------


class TrayApp:
    def __init__(self, requested_ipc_port: int, initial_path: Optional[Path]):
        self.requested_ipc_port = int(requested_ipc_port)
        self.initial_path = initial_path
        self.runtime_dir = get_jupyter_runtime_dir()

        self.instance_id = uuid.uuid4().hex

        self._stop = threading.Event()
        self._cached_sig: List[Tuple[str, int]] = []

        self.pystray = None
        self.Menu = None
        self.MenuItem = None
        self.Image = None
        self.ImageDraw = None

        self.icon = None
        self._ipc: Optional[IPCServer] = None
        self.ipc_port: Optional[int] = None

    def _make_icon_image(self):
        # Prefer an .ico placed next to the script (e.g. jupyterlab.ico).
        icon_path = _find_tray_icon_path()
        if icon_path is not None:
            try:
                img0 = self.Image.open(str(icon_path))
                # ICOs are often palette-based; convert to RGBA for best compatibility.
                img = img0.convert("RGBA")
                logging.info("Loaded tray icon: %s (%sx%s)", str(icon_path), img.size[0], img.size[1])
                return img
            except Exception:
                logging.warning("Failed to load tray icon from %s; using fallback", str(icon_path), exc_info=True)
        else:
            logging.info("Tray icon file jupyterlab.ico not found; using fallback icon")

        # Fallback: simple generated icon.
        img = self.Image.new("RGB", (64, 64), (243, 119, 38))
        d = self.ImageDraw.Draw(img)
        d.rectangle((16, 16, 48, 48), fill=(255, 255, 255))
        return img

    def _menu_start_new_server(self, _icon, _item):
        start_server(Path.home())

    def _menu_open_server(self, _icon, _item, host: str, port: int):
        host = _norm_host(host)
        best = _best_server_for_host_port(self.runtime_dir, host, int(port))
        if best:
            open_in_browser(lab_url(best, None))
        else:
            open_in_browser(f"http://{host}:{int(port)}/lab")

    def _menu_shutdown_server(self, _icon, _item, host: str, port: int):
        host = _norm_host(host)
        best = _best_server_for_host_port(self.runtime_dir, host, int(port), wait_for_token=0.0)
        if not best:
            logging.warning("Shutdown requested but server not found for %s:%s", host, port)
            return
        shutdown_server(best)

    def _menu_quit(self, _icon, _item):
        self.quit()

    def _build_menu(self):
        Menu = self.Menu
        MenuItem = self.MenuItem

        items = []
        items.append(MenuItem("Start New Server", self._menu_start_new_server))
        items.append(Menu.SEPARATOR)

        try:
            servers = list_live_servers(self.runtime_dir)
        except Exception:
            logging.debug("list_live_servers failed", exc_info=True)
            servers = []

        self._cached_sig = [(str(s.get("_host") or ""), int(s.get("port") or 0)) for s in servers]

        if not servers:
            items.append(MenuItem("No active servers", lambda _i, _it: None, enabled=False))
        else:
            for s in servers:
                host = str(s.get("_host") or "127.0.0.1")
                port = int(s.get("port") or 0)
                root = str(s.get("root_dir") or "")
                root_label = os.path.basename(root) if root else "(unknown root)"
                label = f"{host}:{port}  {root_label}"

                open_cb = functools.partial(self._menu_open_server, host=host, port=port)
                shutdown_enabled = bool(_pid_int(s.get("pid")))
                shutdown_cb = functools.partial(self._menu_shutdown_server, host=host, port=port)

                submenu = Menu(
                    MenuItem("Open Lab", open_cb),
                    MenuItem("Shutdown", shutdown_cb, enabled=shutdown_enabled),
                )
                items.append(MenuItem(label, submenu))

        items.append(Menu.SEPARATOR)
        items.append(MenuItem("Quit Tray", self._menu_quit))

        return Menu(*items)

    def _monitor(self):
        while not self._stop.is_set():
            time.sleep(MENU_REFRESH_SECONDS)

            try:
                servers = list_live_servers(self.runtime_dir)
                sig = [(str(s.get("_host") or ""), int(s.get("port") or 0)) for s in servers]
                if sig != self._cached_sig and self.icon is not None:
                    logging.info("Updating menu...")
                    try:
                        self.icon.menu = self._build_menu()
                        if hasattr(self.icon, "update_menu"):
                            try:
                                self.icon.update_menu()  # type: ignore[attr-defined]
                            except Exception:
                                pass
                    except Exception:
                        logging.debug("Menu update failed", exc_info=True)

                if self.ipc_port is not None:
                    _write_ipc_info(self.ipc_port, self.instance_id)

            except Exception:
                logging.debug("Monitor thread error", exc_info=True)

    def _derive_root_dir_for_target(self, target: Optional[Path]) -> Path:
        if target is None:
            return Path.home()

        try:
            if target.exists() and target.is_dir():
                return target
            if target.exists() and target.is_file():
                return target.parent
        except Exception:
            pass

        try:
            if target.suffix:
                return target.parent
        except Exception:
            pass

        return Path.home()

    def handle_open_request(self, path: Optional[Path]) -> None:
        logging.info("Open request: %s", str(path) if path else "(none)")

        servers = list_live_servers(self.runtime_dir)

        if not servers:
            # Priority 2-A
            root_dir = self._derive_root_dir_for_target(path)
            start_server(root_dir)

            deadline = time.time() + SERVER_START_WAIT_SECONDS
            while time.time() < deadline:
                time.sleep(0.5)
                servers = list_live_servers(self.runtime_dir)
                if servers:
                    break

        if not servers:
            logging.error("No servers found")
            return

        # Priority 2-B
        best = _pick_best_server_for_path(servers, path)
        if best is None:
            logging.error("Failed to select a server")
            return

        # Late-bind token
        if not best.get("token"):
            deadline2 = time.time() + TOKEN_WAIT_SECONDS
            while time.time() < deadline2:
                time.sleep(0.1)
                servers2 = list_live_servers(self.runtime_dir)
                cand = _pick_best_server_for_path(servers2, path)
                if cand and cand.get("token"):
                    best = cand
                    break

        open_in_browser(lab_url(best, path))

    def quit(self) -> None:
        self._stop.set()
        try:
            if self._ipc:
                self._ipc.shutdown()
        except Exception:
            pass
        try:
            if self.icon:
                self.icon.stop()
        except Exception:
            pass

    def run(self) -> None:
        self.pystray, self.Menu, self.MenuItem, self.Image, self.ImageDraw = _import_tray_deps()

        logging.info("Runtime Dir: %s", str(self.runtime_dir))

        # Bind IPC: preferred port, else ephemeral.
        self.ipc_port = None
        for p in (int(self.requested_ipc_port), 0):
            try:
                self._ipc = IPCServer(("127.0.0.1", p), IPCHandler, app=self, instance_id=self.instance_id)
                self.ipc_port = int(self._ipc.server_address[1])
                threading.Thread(target=self._ipc.serve_forever, daemon=True).start()
                break
            except OSError:
                continue

        if self.ipc_port is None:
            raise RuntimeError("Failed to bind IPC server")

        _write_ipc_info(self.ipc_port, self.instance_id)
        logging.info("IPC listening on 127.0.0.1:%s", self.ipc_port)

        if self.initial_path is not None:
            threading.Thread(target=self.handle_open_request, args=(self.initial_path,), daemon=True).start()

        threading.Thread(target=self._monitor, daemon=True).start()

        self.icon = self.pystray.Icon(
            "JupyterLab",
            self._make_icon_image(),
            "JupyterLab Tray",
            menu=self._build_menu(),
        )
        self.icon.run()


# -------------------- Detach --------------------


def _spawn_detached_child(args: argparse.Namespace, target: Optional[Path]) -> None:
    env = os.environ.copy()
    env["JLAB_TRAY_CHILD"] = "1"

    script = str(Path(sys.argv[0]).resolve())

    cmd = [sys.executable, script, "--foreground", "--ipc-port", str(int(args.ipc_port)), "--log-level", str(args.log_level)]
    if target is not None:
        cmd.append(str(target))

    flags = 0
    if _is_windows():
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


# -------------------- CLI --------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog=Path(sys.argv[0]).name,
        description="Windows tray helper for JupyterLab (single-instance IPC).",
    )
    ap.add_argument("path", nargs="?", help="Optional file/dir to open in JupyterLab.")
    ap.add_argument("--ipc-port", type=int, default=DEFAULT_IPC_PORT, help="Preferred IPC port (tray may choose another).")
    ap.add_argument("--foreground", action="store_true", help="Run in foreground (do not detach).")
    ap.add_argument(
        "--log-level",
        default=os.environ.get("JLAB_TRAY_LOG_LEVEL", "INFO"),
        help="Logging level (DEBUG/INFO/WARNING/ERROR). Env: JLAB_TRAY_LOG_LEVEL",
    )
    return ap


def main(argv: Optional[List[str]] = None) -> int:
    # Normalize argv so we can intercept -h/--help even when there's no console (e.g., pythonw/double-click).
    argv_list = list(sys.argv[1:] if argv is None else argv)

    ap = _build_arg_parser()

    if any(a in ("-h", "--help") for a in argv_list):
        help_text = ap.format_help()
        _print_or_messagebox("JupyterLab Tray - Help", help_text)
        return 0

    args = ap.parse_args(argv_list)

    _configure_logging(str(args.log_level), also_console=bool(args.foreground and _has_windows_console()))

    target: Optional[Path]
    if args.path:
        try:
            target = Path(args.path).expanduser().resolve(strict=False)
        except Exception:
            target = Path(args.path)
    else:
        target = None

    # Try existing tray first (Priority 3 handshake)
    candidate_ports: List[int] = []

    info = _read_ipc_info()
    if info and isinstance(info.get("port"), int):
        candidate_ports.append(int(info["port"]))

    if int(args.ipc_port) not in candidate_ports:
        candidate_ports.append(int(args.ipc_port))

    for port in candidate_ports:
        ping = _ipc_ping(port)
        if not ping or ping.get("app") != APP_ID:
            continue
        if _ipc_send_open(port, str(target) if target is not None else None):
            _print_or_messagebox(
                "JupyterLab Tray",
                f">> Sent open request to existing tray on 127.0.0.1:{port}",
            )
            return 0

    # No tray found
    is_child = os.environ.get("JLAB_TRAY_CHILD") == "1"

    if args.foreground or is_child:
        try:
            TrayApp(requested_ipc_port=int(args.ipc_port), initial_path=target).run()
            return 0
        except MissingDependencies as e:
            msg = str(e)
            logging.error(msg)
            _print_or_messagebox("JupyterLab Tray - Missing Dependencies", msg)
            return 1
        except Exception:
            logging.critical("Fatal error", exc_info=True)
            if _has_windows_console():
                traceback.print_exc()
            else:
                _message_box_error("JupyterLab Tray - Error", f"Fatal error. See log:\n{LOG_FILE}")
            return 1

    # Detach
    try:
        _print_or_messagebox(
            "JupyterLab Tray",
            "Launching JupyterLab tray in the background...\n"
            f"Log: {LOG_FILE}\n"
            f"IPC info: {IPC_INFO_FILE}",
        )
        _spawn_detached_child(args, target)
        return 0
    except Exception:
        logging.critical("Failed to spawn detached child", exc_info=True)
        if _has_windows_console():
            traceback.print_exc()
        else:
            _message_box_error("JupyterLab Tray - Error", f"Failed to launch. See log:\n{LOG_FILE}")
        return 1


if __name__ == "__main__":

    raise SystemExit(main())
