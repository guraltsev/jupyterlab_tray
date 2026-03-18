import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "jupyterlab_tray", Path(__file__).resolve().parents[1] / "jupyterlab_tray.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class RuntimeDiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime_dir = Path(tempfile.mkdtemp(prefix="jlab_tray_rt_"))
        self._orig_is_windows = MODULE._is_windows
        self._orig_tcp_alive = MODULE._tcp_alive
        self._orig_windows_listeners = MODULE._windows_listening_pids_by_port

        MODULE._is_windows = lambda: True
        MODULE._tcp_alive = lambda url, timeout=MODULE.TCP_ALIVE_TIMEOUT: True

    def tearDown(self) -> None:
        MODULE._is_windows = self._orig_is_windows
        MODULE._tcp_alive = self._orig_tcp_alive
        MODULE._windows_listening_pids_by_port = self._orig_windows_listeners

    def _write_runtime(self, filename: str, *, pid: int, token: str, root_dir: str) -> None:
        path = self.runtime_dir / filename
        path.write_text(
            json.dumps(
                {
                    "url": "http://localhost:8889/",
                    "base_url": "/",
                    "root_dir": root_dir,
                    "token": token,
                    "pid": pid,
                    "port": 8889,
                }
            ),
            encoding="utf-8",
        )

    def test_stale_runtime_file_is_ignored_when_pid_does_not_own_port(self) -> None:
        self._write_runtime("jpserver-1111.json", pid=1111, token="oldtoken", root_dir="/old")
        time.sleep(0.02)
        self._write_runtime("jpserver-2222.json", pid=2222, token="newtoken", root_dir="/current")

        MODULE._windows_listening_pids_by_port = lambda: {8889: {2222}}

        servers = MODULE.list_live_servers(self.runtime_dir)
        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0]["pid"], 2222)
        self.assertEqual(servers[0]["token"], "newtoken")
        self.assertEqual(servers[0]["root_dir"], "/current")

    def test_newest_runtime_file_wins_when_pid_mapping_is_unavailable(self) -> None:
        self._write_runtime("jpserver-1111.json", pid=1111, token="oldtoken", root_dir="/old")
        time.sleep(0.02)
        self._write_runtime("jpserver-2222.json", pid=2222, token="newtoken", root_dir="/current")

        MODULE._windows_listening_pids_by_port = lambda: None

        servers = MODULE.list_live_servers(self.runtime_dir)
        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0]["pid"], 2222)
        self.assertEqual(servers[0]["token"], "newtoken")

    def test_token_redaction_preserves_prefix(self) -> None:
        redacted = MODULE._redact_tokens_in_text(
            "Jupyter URL: http://localhost:8889/lab?token=abc123&next=%2Flab"
        )
        self.assertIn("token=REDACTED", redacted)
        self.assertNotIn("abc123", redacted)


if __name__ == "__main__":
    unittest.main()
