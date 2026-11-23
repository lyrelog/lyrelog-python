"""Against a running Lyrelog: LYRELOG_E2E_URL=http://localhost:8090 LYRELOG_E2E_KEY=lyr_ing_... uv run pytest tests/test_e2e.py"""

import logging
import os
import uuid

import pytest

import lyrelog

URL = os.environ.get("LYRELOG_E2E_URL")
KEY = os.environ.get("LYRELOG_E2E_KEY")

pytestmark = pytest.mark.skipif(not (URL and KEY), reason="set LYRELOG_E2E_URL and LYRELOG_E2E_KEY")


def test_events_are_accepted_by_a_real_ingest():
    run = uuid.uuid4().hex[:8]
    client = lyrelog.Client(endpoint=URL, api_key=KEY, environment="e2e", release="0.9.0", app_id="lyrelog-python-e2e",
                            flush_interval=60)
    client.set_user("e2e-user")
    client.info("e2e_python_info", category="e2e", attrs={"run": run, "n": 1, "ok": True, "tags": ["a"]},
                payload={"detail": "x" * 100})
    try:
        raise RuntimeError("e2e boom")
    except RuntimeError:
        client.capture_error(category="e2e", attrs={"run": run})
    handler = lyrelog.LoggingHandler(client=client)
    log = logging.getLogger("e2e.logging")
    log.propagate = False
    log.addHandler(handler)
    log.warning("e2e_python_logging", extra={"run": run})
    log.removeHandler(handler)
    assert client.flush(10)
    stats = client.stats()
    client.shutdown()
    assert stats["delivered"] == 3, stats
    assert stats["rejected"] == 0 and stats["discarded"] == 0 and not stats["disabled"]
    print(f"run={run}")
