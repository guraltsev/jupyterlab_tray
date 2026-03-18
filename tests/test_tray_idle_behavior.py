import importlib.util
import tempfile
import types
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "jupyterlab_tray", Path(__file__).resolve().parents[1] / "jupyterlab_tray.py"
)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class FakeMenuItem:
    def __init__(self, text, action, **kwargs):
        self.text = text
        self.action = action
        self.kwargs = kwargs


class FakeMenu:
    SEPARATOR = object()

    def __init__(self, *items):
        self._items = tuple(items)

    @property
    def items(self):
        if len(self._items) == 1 and callable(self._items[0]) and not isinstance(self._items[0], FakeMenuItem):
            return tuple(self._items[0]())
        return self._items


class TrayIdleBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_runtime_dir = MODULE.get_jupyter_runtime_dir
        MODULE.get_jupyter_runtime_dir = lambda: Path(tempfile.mkdtemp(prefix="jlab_tray_runtime_"))

    def tearDown(self) -> None:
        MODULE.get_jupyter_runtime_dir = self._orig_runtime_dir

    def test_build_menu_defers_server_scan_until_menu_is_evaluated(self) -> None:
        app = MODULE.TrayApp(requested_ipc_port=MODULE.DEFAULT_IPC_PORT, initial_path=None)
        app.Menu = FakeMenu
        app.MenuItem = FakeMenuItem

        calls = {"count": 0}
        orig_list_live_servers = MODULE.list_live_servers

        def fake_list_live_servers(runtime_dir):
            calls["count"] += 1
            return []

        MODULE.list_live_servers = fake_list_live_servers
        try:
            menu = app._build_menu()
            self.assertEqual(calls["count"], 0)

            first_items = menu.items
            self.assertEqual(calls["count"], 1)
            self.assertEqual(first_items[0].text, "Start New Server")
            self.assertEqual(first_items[-1].text, "Quit Tray")

            second_items = menu.items
            self.assertEqual(calls["count"], 2)
            self.assertEqual(second_items[0].text, "Start New Server")
        finally:
            MODULE.list_live_servers = orig_list_live_servers

    def test_run_does_not_start_background_monitor_thread(self) -> None:
        app = MODULE.TrayApp(requested_ipc_port=MODULE.DEFAULT_IPC_PORT, initial_path=None)

        class FakeIcon:
            def __init__(self, name, icon, title, menu):
                self.name = name
                self.icon = icon
                self.title = title
                self.menu = menu
                self.stopped = False

            def run(self):
                return None

            def stop(self):
                self.stopped = True

        class FakeIPCServer:
            def __init__(self, addr, handler, app, instance_id):
                self.server_address = (addr[0], addr[1] or 9999)
                self.app = app
                self.instance_id = instance_id

            def serve_forever(self):
                return None

            def shutdown(self):
                return None

        class FakeThread:
            started_targets = []

            def __init__(self, target=None, args=(), kwargs=None, daemon=None):
                self.target = target
                self.args = args
                self.kwargs = kwargs or {}
                self.daemon = daemon

            def start(self):
                FakeThread.started_targets.append(self.target)

        orig_import_tray_deps = MODULE._import_tray_deps
        orig_detect_jupyter = MODULE._detect_jupyter_installation
        orig_write_ipc = MODULE._write_ipc_info
        orig_ipc_server = MODULE.IPCServer
        orig_thread = MODULE.threading.Thread
        orig_make_icon_image = MODULE.TrayApp._make_icon_image

        MODULE._import_tray_deps = lambda: (
            types.SimpleNamespace(Icon=FakeIcon),
            FakeMenu,
            FakeMenuItem,
            object(),
            object(),
        )
        MODULE._detect_jupyter_installation = lambda: (True, "")
        MODULE._write_ipc_info = lambda port, instance_id: None
        MODULE.IPCServer = FakeIPCServer
        MODULE.threading.Thread = FakeThread
        MODULE.TrayApp._make_icon_image = lambda self: object()

        try:
            app.run()
        finally:
            MODULE._import_tray_deps = orig_import_tray_deps
            MODULE._detect_jupyter_installation = orig_detect_jupyter
            MODULE._write_ipc_info = orig_write_ipc
            MODULE.IPCServer = orig_ipc_server
            MODULE.threading.Thread = orig_thread
            MODULE.TrayApp._make_icon_image = orig_make_icon_image

        target_names = [getattr(t, "__name__", repr(t)) for t in FakeThread.started_targets]
        self.assertEqual(target_names, ["serve_forever"])


if __name__ == "__main__":
    unittest.main()
