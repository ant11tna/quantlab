from __future__ import annotations

import subprocess

from quantlab.data import update_all as update_all_mod


class _FakeProc:
    def __init__(self) -> None:
        self.pid = 12345
        self.stdout = iter([
            '{"type":"progress","stage":"raw","done":1,"total":1,"symbol":"510300"}\n',
            '{"type":"done","stage":"raw","ok":true,"done":1,"total":1}\n',
        ])

    def poll(self):
        return None

    def wait(self) -> int:
        return 0


def test_update_all_stream_merges_stderr_and_stdout(monkeypatch) -> None:
    popen_kwargs = {}

    def _fake_popen(*args, **kwargs):
        popen_kwargs.update(kwargs)
        return _FakeProc()

    class _FakeBuilder:
        def build_all_iter(self, validate: bool = True):
            yield {"type": "done", "stage": "curated", "ok": True, "done": 0, "total": 0}

    monkeypatch.setattr(update_all_mod.subprocess, "Popen", _fake_popen)

    import quantlab.data.curate as curate_mod

    monkeypatch.setattr(curate_mod, "CuratedDataBuilder", _FakeBuilder)

    events = list(update_all_mod.update_all_stream())

    assert popen_kwargs["stderr"] == subprocess.STDOUT
    assert any(ev.get("type") == "start" and ev.get("stage") == "raw" for ev in events)
    assert any(ev.get("type") == "done" and ev.get("stage") == "raw" for ev in events)
    assert any(ev.get("type") == "done" and ev.get("stage") == "curated" for ev in events)
    assert any(ev.get("type") == "done" and "stage" not in ev for ev in events)
