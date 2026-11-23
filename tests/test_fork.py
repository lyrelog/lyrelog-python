import os

import pytest

import lyrelog

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")


def _child(fn):
    read, write = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.write(write, b"1" if fn() else b"0")
        finally:
            os._exit(0)
    os.close(write)
    result = os.read(read, 1)
    os.close(read)
    os.waitpid(pid, 0)
    return result == b"1"


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
def test_default_lock_is_fresh_in_the_child():
    import lyrelog as package

    package._default_lock.acquire()  # another thread holds it at the moment of the fork
    try:
        assert _child(lambda: package._default_lock.acquire(timeout=1))
    finally:
        package._default_lock.release()


PARKED_FORK = r"""
import os, sys, time, warnings
import lyrelog

class Recorder:
    def __init__(self):
        self.bodies = []

    def send(self, url, body, headers, timeout):
        self.bodies.append(body)
        return 200, b'{"accepted":1,"rejected":[]}', None

recorder = Recorder()
client = lyrelog.Client(endpoint="http://127.0.0.1:1", api_key="k", flush_interval=60, transport=recorder)
client.info("master")
assert client._worker is not None and client._worker.is_alive()
time.sleep(0.05)
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    pid = os.fork()
    if pid == 0:
        os._exit(0 if client.stats()["queued"] == 0 else 1)
    _, status = os.waitpid(pid, 0)
assert status == 0, status
assert not [w for w in caught if "multi-threaded" in str(w.message)], [str(w.message) for w in caught]
assert client._worker is not None and client._worker.is_alive()  # started again in the parent
assert client.flush(5) and len(recorder.bodies) == 1
print("ok")
"""


def test_an_idle_worker_is_parked_across_a_fork():
    # gunicorn --preload: the app logged in the master, so its worker thread is alive when the
    # master forks. The SDK stops it for the fork (no multi-threaded-fork warning, no thread
    # caught halfway) and starts it again in the parent. In a fresh interpreter: on macOS the
    # system proxy lookup of earlier tests leaves OS threads behind that would trip the check.
    import subprocess
    import sys

    result = subprocess.run([sys.executable, "-c", PARKED_FORK], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0 and result.stdout.strip() == "ok", result.stderr
