#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""IngestGate module: validation gate between importers and Mixer.

Subscribes to the pre-gate queue and publishes to ``Importers`` only on
pass; failures go to the DLQ (``ingest:dlq``) with provenance. Wire in
``configs/modules.cfg`` as ``[IngestGate] subscribe=Importers-pre``.
Follows the ``TemplateModule`` pattern; queue wiring degrades to a
logged dry-run when the AIL queue layer is unavailable.
"""
import os
import sys

sys.path.append(os.environ.get("AIL_BIN", os.path.join(os.path.dirname(__file__), "..")))

try:
    from modules.abstract_module import AbstractModule
    _HAS_AIL = True
except Exception:
    AbstractModule = object  # type: ignore
    _HAS_AIL = False

from lib.ingest import IngestGate as _Gate
from lib.ingest.envelope import IngestEnvelope


class IngestGate(AbstractModule):  # type: ignore[misc]
    def __init__(self) -> None:
        if _HAS_AIL:
            try:
                super().__init__()
            except Exception as exc:
                self.logger.warning("queue init failed (dry-run): %s", exc)  # type: ignore[attr-defined]
        self.gate = _Gate()
        self.logger.info("Module IngestGate initialized")  # type: ignore[attr-defined]

    def compute_envelope(self, env: IngestEnvelope) -> bool:
        """Return True when the envelope may proceed downstream."""
        return self.gate.check(env).passed

    def compute(self, message):  # queue entry point (message = envelope JSON)
        import json
        env = IngestEnvelope.from_dict(json.loads(message))
        if self.compute_envelope(env):
            self.add_message_to_queue(message=message)  # type: ignore[attr-defined]
        else:
            self.logger.warning("blocked envelope %s", env.envelope_uuid)  # type: ignore[attr-defined]


if __name__ == "__main__":
    module = IngestGate()
    if _HAS_AIL and hasattr(module, "run"):
        module.run()  # type: ignore[attr-defined]
