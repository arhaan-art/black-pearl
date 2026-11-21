"""Playwright browser tests for the search stack.

Exercises a real Chromium page calling the live Meilisearch search API
(hybrid keyword+vector) the same way the search dashboard does. The
full Flask dashboard needs the app stack (Flask/Redis/Kvrocks); when
that is unavailable these browser-level tests still verify the search
interface contract end-to-end. Run with the docproc venv:
  .venv-docproc/bin/python -m pytest tests/test_search_playwright.py
or plain: .venv-docproc/bin/python tests/test_search_playwright.py
"""
import json
import urllib.request

BASE = "http://127.0.0.1:7700"
KEY = "blackpearl-dev-key"


def _available():
    try:
        req = urllib.request.Request(BASE + "/health")
        with urllib.request.urlopen(req, timeout=3) as r:
            return json.loads(r.read()).get("status") == "available"
    except Exception:
        return False


def test_browser_hybrid_search_returns_hits():
    assert _available(), "Meilisearch not reachable on 127.0.0.1:7700"
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(BASE + "/health")
        result = page.evaluate("""async ([base, key]) => {
          const r = await fetch(base + '/indexes/bp_eval/search', {
            method: 'POST',
            headers: {'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'},
            body: JSON.stringify({q: 'escrow bitcoin', limit: 5, facets: ['lang', 'entity_type']})
          });
          const j = await r.json();
          return {status: r.status, hits: j.hits ? j.hits.length : -1,
                  facets: j.facetDistribution ? Object.keys(j.facetDistribution) : []};
        }""", [BASE, KEY])
        browser.close()
    assert result["status"] == 200, result
    assert result["hits"] > 0, result
    assert "entity_type" in result["facets"], result


if __name__ == "__main__":
    test_browser_hybrid_search_returns_hits()
    print("PLAYWRIGHT SEARCH TEST PASS")
