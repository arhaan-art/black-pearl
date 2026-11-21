#!/usr/bin/env python3
# -*-coding:UTF-8 -*
"""DocReprocessor module: consumes docproc:reprocess queue (design §9).

Thin AbstractModule subclass following the pending_seconds pattern.
Re-fetches bytes from the PDF/Image object file, re-runs the registry
chain, forks a child envelope (parent preserved), updates the registry
only when the new confidence is >= the old one.
"""
import os
import sys

sys.path.append(os.environ.get('AIL_BIN', ''))

from modules.abstract_module import AbstractModule  # noqa: E402


class DocReprocessor(AbstractModule):
    def __init__(self):
        super(DocReprocessor, self).__init__()
        self.pending_seconds = 5
        self.logger.info(f'Module {self.module_name} initialized')

    def compute(self, message):
        try:
            from lib.ingest.processors.registry_store import ProcessedDocRegistry, RegistryEntry, text_sha256
            from lib.ingest.processors import run_doc_pipeline, default_registry
            from lib.ingest.config import load_pipeline_config
        except Exception as exc:
            self.logger.warning(f'docproc unavailable: {exc}')
            return
        registry = ProcessedDocRegistry()
        item = registry.dequeue_reprocess()
        if not item:
            return
        sha = item.get('sha256', '')
        obj = self.get_obj()
        path = None
        try:
            path = obj.get_filepath()
        except Exception:
            path = None
        if not path or not os.path.exists(path):
            self.logger.warning(f'reprocess: no file for {sha}')
            return
        with open(path, 'rb') as fh:
            content = fh.read()
        cfg = load_pipeline_config().docproc.to_processor_config()
        mime = 'application/pdf' if str(path).lower().endswith('.pdf') else 'application/octet-stream'
        result = run_doc_pipeline(content, mime, os.path.basename(path), config=cfg)
        new_conf = result.confidence.overall
        if registry.should_overwrite(sha, new_conf):
            cap_ids = default_registry(cfg).capability_ids(cfg)
            registry.record(RegistryEntry(
                sha256=sha, capability_ids=cap_ids,
                overall_confidence=new_conf, mimetype=mime,
                text_sha256=text_sha256(result.text)))
            self.logger.info(f'reprocessed {sha} conf={new_conf}')
        else:
            self.logger.info(f'reprocess kept old result for {sha} (new conf {new_conf} lower)')


if __name__ == '__main__':
    module = DocReprocessor()
    module.run()
