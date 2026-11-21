"""API-level search_engine tests without Meilisearch/Redis (stubbed deps).

Covers: entity detection/expansion, facets filter construction, explain
payload flags, ranking/semantic-ratio config, and the hybrid kill-switch
regression (sort='hybrid' must not send a vector when hybrid_enabled=False).
Live Meilisearch is blocked on this host (glibc 2.35), so MeiliSearch.search
is exercised against a captured multi_search fake -- documented substitute.
"""
import importlib.util, os, sys, types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items(): setattr(m, k, v)
    sys.modules[name] = m
    return m

class _Cfg:
    def get_config_boolean(self, s, k): return False
    def get_config_str(self, s, k): return ''
    def has_section(self, s): return False
    def has_option(self, s, k): return False
    def get_db_conn(self, name):
        class DB:
            def exists(self, k): return False
            def sadd(self, *a): pass
            def hset(self, *a): pass
        return DB()

def _load():
    if 'search_engine_under_test' in sys.modules:
        return sys.modules['search_engine_under_test']
    os.environ.setdefault('AIL_BIN', os.path.join(ROOT, 'bin'))
    meili = _stub('meilisearch'); meili.Client = object
    err = _stub('meilisearch.errors')
    for c in ('MeilisearchApiError', 'MeilisearchCommunicationError', 'MeilisearchTimeoutError'):
        setattr(err, c, type(c, (Exception,), {'message': 'stub'}))
    meili.errors = err
    _stub('requests')
    lib = _stub('lib'); lib.__path__ = []
    _stub('lib.ail_logger', get_config=lambda name=None: {'version': 1, 'disable_existing_loggers': False, 'handlers': {}, 'root': {}})
    _stub('lib.exceptions', MeilisearchError=type('MeilisearchError', (Exception,), {}))
    _stub('lib.ConfigLoader', ConfigLoader=_Cfg)
    objs = _stub('lib.objects'); objs.__path__ = []
    for n in ('Domains', 'FilesNames', 'Images', 'Items', 'Messages', 'Forums', 'ForumThreads', 'Posts', 'Screenshots', 'Titles', 'UsersAccount'):
        m = _stub(f'lib.objects.{n}'); setattr(objs, n, m)
    sys.modules['lib.objects'].Domains.get_all_domains_types = lambda: []
    _stub('lib.chats_viewer', get_chat_protocols=lambda: [])
    pkg = _stub('packages'); pkg.__path__ = []
    _stub('packages.Date', convert_str_date_to_epoch=lambda s: 0, convert_str_date_to_epoch_end=lambda s: 0)
    sys.path.insert(0, os.path.join(ROOT, 'bin', 'lib'))
    spec = importlib.util.spec_from_file_location('search_engine_under_test', os.path.join(ROOT, 'bin', 'lib', 'search_engine.py'))
    mod = importlib.util.module_from_spec(spec); sys.modules[spec.name] = mod; spec.loader.exec_module(mod)
    return mod

se = _load()

class FakeSearchClient:
    def __init__(self): self.calls = None
    def multi_search(self, queries, params): self.calls = (queries, params); return {'results': []}

def _engine():
    eng = object.__new__(se.MeiliSearch); eng.search_client = FakeSearchClient(); return eng

def test_detect_entities_types():
    q = "contact a@b.com CVE-2024-1234 hash " + "a"*64 + " wallet bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq"
    types_found = {e['type'] for e in se.detect_entities(q)}
    assert {'email', 'cve', 'sha256'} <= types_found

def test_expand_query_synonyms_and_no_dup():
    exp, terms = se.expand_query("btc wallet btc")
    assert 'bitcoin' in terms and exp.startswith("btc wallet btc")
    exp2, terms2 = se.expand_query("unrelated words")
    assert terms2 == [] and exp2 == "unrelated words"

def test_semantic_ratio_kill_switch_function():
    orig = dict(se.SEARCH_CONFIG); se.SEARCH_CONFIG['hybrid_enabled'] = False
    try: assert se.get_semantic_ratio(['pdf'], 'hybrid') == 0.0
    finally: se.SEARCH_CONFIG.update(orig)

def test_hybrid_sort_no_vector_when_disabled_regression():
    orig = dict(se.SEARCH_CONFIG); se.SEARCH_CONFIG['hybrid_enabled'] = False
    try:
        eng = _engine(); eng.search(['pdf'], 'ransomware', sort='hybrid')
        q = eng.search_client.calls[0][0]
        assert 'vector' not in q and 'hybrid' not in q
        eng2 = _engine(); eng2.search(['pdf'], 'x', sort='hybrid', semantic_ratio=0.9)
        assert 'hybrid' not in eng2.search_client.calls[0][0]
    finally: se.SEARCH_CONFIG.update(orig)

def test_facets_filter_and_explain_flags():
    eng = _engine(); eng.search(['pdf'], 'test', sort='best', facets_filter={'lang': ['en'], 'bogus': ['x']}, explain=True)
    q = eng.search_client.calls[0][0]
    assert "lang IN ['en']" in q.get('filter', '') and 'bogus' not in q.get('filter', '')
    assert q['showRankingScore'] is True and q['showMatchesPosition'] is True
    assert q['facets'] == se.FACET_ATTRIBUTES and q['attributesToSearchOn'] == se.SEARCHABLE_ATTRIBUTES

def test_ranking_config_shape():
    assert se.SEARCHABLE_ATTRIBUTES == ['title', 'name', 'username', 'entities.value', 'content']
    assert 'sort' not in str(se.FACET_ATTRIBUTES)  # facets are filterable attrs, not ranking rules
