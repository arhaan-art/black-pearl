#!/usr/bin/env python3
# -*-coding:UTF-8 -*

import os
import logging.config
import sys
import time
import uuid

import requests
import meilisearch
from meilisearch.errors import MeilisearchApiError, MeilisearchCommunicationError, MeilisearchTimeoutError
from hashlib import sha256

sys.path.append(os.environ['AIL_BIN'])
##################################
# Import Project packages
##################################
from lib import ail_logger
from lib.exceptions import MeilisearchError
from lib.ConfigLoader import ConfigLoader
from lib.objects import Domains
from lib.objects import FilesNames
from lib.objects import Images
from lib.objects import Items
from lib.objects import Messages
from lib.objects import Forums
from lib.objects import ForumThreads
from lib.objects import Posts
from lib.objects import Screenshots
from lib.objects import Titles
from lib.objects import UsersAccount
from lib import chats_viewer
from packages import Date
import re as _re
try:
    from lib.search_embedding import get_embedding_service
except Exception:
    get_embedding_service = None

logging.config.dictConfig(ail_logger.get_config(name='ail'))
logger = logging.getLogger()

config_loader = ConfigLoader()
IS_MEILISEARCH_ENABLED = config_loader.get_config_boolean('Indexer', 'meilisearch')
M_URL = config_loader.get_config_str('Indexer', 'meilisearch_url')
M_KEY = config_loader.get_config_str('Indexer', 'meilisearch_key')

def _cfg(section, key, default, cast=str):
    try:
        if not config_loader.has_section(section) or not config_loader.has_option(section, key):
            return default
        raw = config_loader.get_config_str(section, key)
        return cast(raw) if cast is not str else raw
    except Exception:
        return default

SEARCH_CONFIG = {
    'hybrid_enabled': _cfg('Search', 'hybrid_enabled', False, lambda v: str(v).lower() in ('1','true','yes')),
    'embedder_source': _cfg('Search', 'embedder_source', 'userProvided'),
    'embedder_model': _cfg('Search', 'embedder_model', 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'),
    'embedder_dimensions': _cfg('Search', 'embedder_dimensions', 384, int),
    'semantic_ratio_default': _cfg('Search', 'semantic_ratio_default', 0.4, float),
    'semantic_ratio_exact': _cfg('Search', 'semantic_ratio_exact', 0.1, float),
    'semantic_ratio_conceptual': _cfg('Search', 'semantic_ratio_conceptual', 0.7, float),
    'show_ranking_score': _cfg('Search', 'show_ranking_score', False, lambda v: str(v).lower() in ('1','true','yes')),
    'entity_boost': _cfg('Search', 'entity_boost', True, lambda v: str(v).lower() not in ('0','false','no')),
}
try:
    import json as _json
    SEARCH_CONFIG['semantic_ratio_overrides'] = _json.loads(_cfg('Search', 'semantic_ratio_overrides', '{}') or '{}')
except Exception:
    SEARCH_CONFIG['semantic_ratio_overrides'] = {}
EMBEDDER_NAME = 'default'
FACET_ATTRIBUTES = ['last', 'first', 'lang', 'source', 'entity_type', 'tags', 'fid', 'type', 'domain_type']
SEARCHABLE_ATTRIBUTES = ['title', 'name', 'username', 'entities.value', 'content']
r_search = config_loader.get_db_conn("Kvrocks_Searchs")
config_loader = None


def get_obj_uuid5(obj_gid):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, obj_gid))


def is_meilisearch_enabled():
    return IS_MEILISEARCH_ENABLED


_ENTITY_PATTERNS = [
    ('onion', _re.compile(r'\b[a-z2-7]{56}\.onion\b')),
    ('domain', _re.compile(r'\b[a-z0-9.-]+\.(?:onion|i2p|com|net|org)\b')),
    ('btc', _re.compile(r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b|\bbc1[a-z0-9]{39,59}\b')),
    ('eth', _re.compile(r'\b0x[a-fA-F0-9]{40}\b')),
    ('xmr', _re.compile(r'\b4[0-9AB][1-9A-HJ-NP-Za-km-z]{93}\b')),
    ('cve', _re.compile(r'\bCVE-\d{4}-\d{4,7}\b', _re.IGNORECASE)),
    ('sha256', _re.compile(r'\b[a-fA-F0-9]{64}\b')),
    ('email', _re.compile(r'\b[\w.+-]+@[\w-]+\.[\w.-]+\b')),
    ('username', _re.compile(r'(?<!\w)@[\w.-]{2,64}\b')),
]
_SYNONYMS = {'btc': 'bitcoin', 'bitcoin': 'btc', 'xmr': 'monero', 'monero': 'xmr', 'eth': 'ethereum', 'ethereum': 'eth'}

def detect_entities(query):
    found = []
    for etype, pat in _ENTITY_PATTERNS:
        for m in pat.finditer(query or ''):
            found.append({'type': etype, 'value': m.group(0)})
    return found

def expand_query(query):
    expanded, terms = query or '', []
    for token in list((query or '').lower().split()):
        if token in _SYNONYMS and _SYNONYMS[token] not in terms:
            terms.append(_SYNONYMS[token])
    if terms:
        expanded = query + ' ' + ' '.join(terms[:5])
    return expanded, terms

def get_semantic_ratio(indexes, sort='best', query=None):
    if not SEARCH_CONFIG.get('hybrid_enabled') or sort == 'hybrid-off':
        return 0.0
    # Entity/hash/domain queries regress under semantic blending (eval:
    # entity Recall@10 0.75 -> 0.667 at ratio 0.4): keep them keyword-led.
    if query and detect_entities(query):
        return float(SEARCH_CONFIG.get('semantic_ratio_exact', 0.1))
    ratio = SEARCH_CONFIG.get('semantic_ratio_default', 0.4)
    for idx in (indexes or []):
        if idx in SEARCH_CONFIG.get('semantic_ratio_overrides', {}):
            ratio = float(SEARCH_CONFIG['semantic_ratio_overrides'][idx]); break
    return float(ratio)

def add_embeddings(document):
    """Single choke point: attach _vectors.default; failure never blocks indexing."""
    if not document or not SEARCH_CONFIG.get('hybrid_enabled') or get_embedding_service is None:
        return document
    try:
        svc = get_embedding_service(SEARCH_CONFIG['embedder_model'], SEARCH_CONFIG['embedder_dimensions'], SEARCH_CONFIG['embedder_source'])
        vec = svc.embed_document(document.get('content') or document.get('title') or '')
        if vec:
            document['_vectors'] = {EMBEDDER_NAME: vec}
    except Exception as e:
        logger.warning(f'embedding failed, indexing without vector: {e}')
    return document


# TODO One index for all forums ???
# def load_forum_indexes():
#     indexes = set()
#     for protocol in chats_viewer.get_chat_protocols():
#         indexes.add(f'c{protocol}')
#     return indexes


def load_messages_indexes():
    indexes = set()
    for protocol in chats_viewer.get_chat_protocols():
        indexes.add(f'c{protocol}')
    return indexes


# FORUMS_INDEXES = load_forums_indexes()
MESSAGES_INDEXES = load_messages_indexes()
DATERANGE_INDEXES = {'filename', 'title', 'pdf', 'ocr'}


def load_indexes_names():
    names = {'desc-dom', 'desc-img', 'desc-screen', 'filename', 'forum', 'title', 'pdf', 'ocr'}
    for domain_types in Domains.get_all_domains_types():
        names.add(domain_types)
    for chat_name in MESSAGES_INDEXES:
        names.add(chat_name)
    return sorted(names)


INDEX_NAMES = load_indexes_names()
def get_indexes_names():
    return INDEX_NAMES


def index_all():
    # Engine._delete_all()
    # Engine._delete('onion')
    # delete_crawled_content_domains_items()
    Engine.create_indexes()
    # Engine._create_index('onion')
    # Engine.setup_indexes_searchable_filterable_sortable()
    index_crawled()
    index_chats()
    index_user_accounts()
    index_messages()
    index_forum_posts()
    index_images_descriptions()
    index_screenshots_descriptions()
    index_domains_descriptions()
    index_titles()
    index_file_names()
    index_pdfs()
    index_ocrs()


class MeiliSearch:
    def __init__(self):
        self.client = meilisearch.Client(M_URL, M_KEY, timeout=5)
        self.search_client = meilisearch.Client(M_URL, M_KEY, timeout=30)
        try:
            self._known_indexes = set(self.get_indexes())
        except meilisearch.errors.MeilisearchCommunicationError:
            self._known_indexes = set()

    def init(self):
        self.create_missing_indexes()

    def is_up(self):
        try:
            self.client.health()
            return True
        except (MeilisearchApiError, MeilisearchCommunicationError, Exception) as e:
            # TODO LOG
            return False

    def get_nb_tasks(self):
        return self.client.get_tasks().total

    def tasks_enqueued(self):
        tasks = self.client.get_tasks({"statuses": ["enqueued"],"limit": 100})
        print("currently enqueued tasks:", len(tasks.results))
        for task in tasks.results:
            print("=" * 100)
            print("uid:        ", task.uid)
            print("index_uid:  ", task.index_uid)
            print("type:       ", task.type)
            print("status:     ", task.status)
            print("enqueued_at:", task.enqueued_at)
            print("started_at: ", task.started_at)
            print("finished_at:", task.finished_at)
            print("details:    ", task.details)
            print("error:      ", task.error)

    def tasks_processing(self):
        tasks = self.client.get_tasks({"statuses": ["processing"],"limit": 100})
        print("currently processing tasks:", len(tasks.results))
        for task in tasks.results:
            print("=" * 100)
            print("uid:        ", task.uid)
            print("index_uid:  ", task.index_uid)
            print("type:       ", task.type)
            print("status:     ", task.status)
            print("enqueued_at:", task.enqueued_at)
            print("started_at: ", task.started_at)
            print("finished_at:", task.finished_at)
            print("details:    ", task.details)
            print("error:      ", task.error)

    def tasks_failed(self):
        tasks = self.client.get_tasks({"statuses": ["failed"], "limit": 100})
        print("currently failed tasks:", len(tasks.results))
        for task in tasks.results:
            print("=" * 100)
            print("uid:        ", task.uid)
            print("index_uid:  ", task.index_uid)
            print("type:       ", task.type)
            print("status:     ", task.status)
            print("enqueued_at:", task.enqueued_at)
            print("started_at: ", task.started_at)
            print("finished_at:", task.finished_at)
            print("details:    ", task.details)
            print("error:      ", task.error)

    def task_status(self, task_uid):
        status = self.client.get_task(task_uid)
        print(status)
        return status

    def task_payload(self, task_uid):
        url = f"{M_URL}/tasks/{task_uid}/documents"
        r = requests.get(url, headers={"Authorization": f"Bearer {M_KEY}"})
        if r.status_code != 200:
            print(f"Could not dump documents for task {task_uid}: HTTP {r.status_code}")
            print(r.text[:1000])
            return None
        return r.content

    def tasks_cancel_processing(self, task_uids):
        cancel_task = self.client.cancel_tasks({"uids": task_uids, "statuses": ["processing"]})
        print("Cancellation task:", cancel_task.task_uid)

    def _wait_task(self, task, timeout_in_ms=120000):
        task_uid = getattr(task, 'task_uid', None) or task.get('taskUid')
        return self.client.wait_for_task(task_uid, timeout_in_ms=timeout_in_ms)

    def search(self, indexes, query, nb=20, page=1, timestamp_from=None, timestamp_to=None, sort='recent', forum_ids=None, forum_types=None, facets_filter=None, explain=False, semantic_ratio=None):
        # TODO investigate attributesToRetrieve speed
        end_query = []
        for index in indexes:
            filters = []
            q = {'indexUid': index,
                 'q': query,
                 'attributesToSearchOn': SEARCHABLE_ATTRIBUTES,
                 'attributesToCrop': ['content'],
                 'attributesToHighlight': ['content'],
                 'highlightPreTag': '🔎⏩',
                 'highlightPostTag': '⏪🔍',
                 'cropLength': 100,
                 'showMatchesPosition': bool(explain),
                 'facets': FACET_ATTRIBUTES,
                 }
            if explain or SEARCH_CONFIG.get('show_ranking_score'):
                q['showRankingScore'] = True
                q['showRankingScoreDetails'] = True
            # hybrid_enabled=False is the kill-switch/rollback: never send a
            # vector, even for sort='hybrid' or an explicit semantic_ratio.
            if not SEARCH_CONFIG.get('hybrid_enabled'):
                _ratio = 0.0
            elif semantic_ratio is not None:
                _ratio = float(semantic_ratio)
            else:
                _ratio = get_semantic_ratio([index], sort, query=query)
                if sort == 'hybrid' and not _ratio:
                    _ratio = SEARCH_CONFIG.get('semantic_ratio_default', 0.4)
            if _ratio and get_embedding_service is not None:
                try:
                    _svc = get_embedding_service(SEARCH_CONFIG['embedder_model'], SEARCH_CONFIG['embedder_dimensions'], SEARCH_CONFIG['embedder_source'])
                    _vec = _svc.embed_query(query)
                    if _vec:
                        q['vector'] = _vec
                        q['hybrid'] = {'semanticRatio': _ratio, 'embedder': EMBEDDER_NAME}
                except Exception as e:
                    logger.warning(f'query embedding failed, keyword fallback: {e}')
            if facets_filter:
                _ff = []
                for _k, _vals in facets_filter.items():
                    if _k in FACET_ATTRIBUTES and _vals:
                        _esc = [str(v).replace('\\', '\\\\').replace("'", "\\'") for v in _vals]
                        _ff.append(_k + ' IN [' + ', '.join([f"'{v}'" for v in _esc]) + ']')
                if _ff:
                    filters.append(' AND '.join(_ff))
            if sort == 'recent':
                q['sort'] = ['last:desc']
            if index == 'forum' and forum_ids:
                escaped_forum_ids = [forum_id.replace('\\', '\\\\').replace("'", "\\'") for forum_id in forum_ids]
                filters.append('fid IN [' + ', '.join([f"'{forum_id}'" for forum_id in escaped_forum_ids]) + ']')
            if index == 'forum' and forum_types:
                escaped_forum_types = [forum_type.replace('\\', '\\\\').replace("'", "\\'") for forum_type in forum_types]
                filters.append('type IN [' + ', '.join([f"'{forum_type}'" for forum_type in escaped_forum_types]) + ']')
            if timestamp_from and timestamp_to:
                if index in DATERANGE_INDEXES:
                    filters.append(f'first >= {timestamp_from} AND last <= {timestamp_to}')
                else:
                    filters.append(f'last >= {timestamp_from} AND last <= {timestamp_to}')
            elif timestamp_from:
                if index in DATERANGE_INDEXES:
                    filters.append(f'first >= {timestamp_from}')
                else:
                    filters.append(f'last >= {timestamp_from}')
            elif timestamp_to:
                filters.append(f'last <= {timestamp_to}')
            if filters:
                q['filter'] = ' AND '.join(filters)
            end_query.append(q)
        return self.search_client.multi_search(end_query, {'limit': nb, 'offset': (page - 1) * nb})

    def get_indexes(self):
        names = []
        for index in self.client.get_indexes().get('results', []):
            names.append(index.uid)
        return names

    def _create_index(self, index_name):
        self.client.create_index(index_name, {'primaryKey': 'uuid'})
        self.setup_index_searchable_filterable_sortable(index_name)

    def create_indexes(self):
        for index_name in get_indexes_names():
            self.ensure_index(index_name)

    def create_missing_indexes(self):
        for index_name in get_indexes_names():
            self.ensure_index(index_name)

    def ensure_index(self, index_name):
        if index_name not in self._known_indexes:
            self._create_index(index_name)
            self._known_indexes.add(index_name)

    def setup_index_searchable_filterable_sortable(self, index_name):
        self.client.index(index_name).update_searchable_attributes(SEARCHABLE_ATTRIBUTES)
        filterable_attributes = list(FACET_ATTRIBUTES)
        self.client.index(index_name).update_filterable_attributes(filterable_attributes)
        self.client.index(index_name).update_sortable_attributes(['last', 'first'])
        try:
            self.client.index(index_name).update_embedders({EMBEDDER_NAME: {'source': SEARCH_CONFIG['embedder_source'], 'dimensions': SEARCH_CONFIG['embedder_dimensions']}})
        except Exception as e:
            logger.warning(f'embedder setup skipped for {index_name}: {e}')
        try:
            self.client.index(index_name).update_synonyms({'btc': ['bitcoin'], 'bitcoin': ['btc'], 'xmr': ['monero'], 'monero': ['xmr'], 'eth': ['ethereum'], 'ethereum': ['eth']})
        except Exception:
            pass
        # result rank
        # Default:
        #     "words",      -> nb match terms
        #     "typo",       -> nb match typo
        #     "proximity",  -> short distance between terms
        #     "attribute",  -> most important attributes
        #     "sort",
        #     "exactness"
        self.client.index(index_name).update_ranking_rules(
            ['words', 'typo', 'proximity', 'attribute', 'exactness'])
        # fix issue attributesToSearchOn fails on never-populated index
        # https://github.com/meilisearch/meilisearch/issues/5921
        dummy_document = {'uuid': 'dummy', 'content': 'dummy', 'last': 0, 'id': 'dummy'}
        if index_name not in MESSAGES_INDEXES:
            dummy_document['first'] = 0
        self.add(index_name, dummy_document)
        self.remove(index_name, 'dummy')

    def setup_indexes_searchable_filterable_sortable(self):
        for index_name in get_indexes_names():
            self.setup_index_searchable_filterable_sortable(index_name)

    # replacing existing documents
    def add(self, index, document, retry=3):
        document = add_embeddings(document)
        try:
            self.client.index(index).add_documents([document], primary_key='uuid')
        except (MeilisearchCommunicationError, MeilisearchApiError, MeilisearchTimeoutError) as e:
            logger.warning(f'Meilisearch connection failed, retry: {retry}')
            if retry > 0:
                self.add(index, document, retry - 1)
                time.sleep(10)
            else:
                logger.warning(f'Meilisearch connection failed: {e.message}')
                raise MeilisearchError(e.message)

    def update(self, index, document, retry=3):
        self.ensure_index(index)
        document = add_embeddings(document)
        try:
            self.client.index(index).update_documents([document], primary_key='uuid')
        except (MeilisearchCommunicationError, MeilisearchApiError, MeilisearchTimeoutError) as e:
            logger.warning(f'Meilisearch connection failed, retry: {retry}')
            if retry > 0:
                self.update(index, document, retry - 1)
                time.sleep(10)
            else:
                logger.warning(f'Meilisearch connection failed: {e.message}')
                raise MeilisearchError(e.message)

    def remove(self, index, doc_id):
        self.client.index(index).delete_document(doc_id)

    def _delete(self, index):
        # self.client.index(index).delete_all_documents()
        self.client.delete_index(index)
        self._known_indexes.remove(index)

    def _delete_all(self):
        indexes = self.client.get_indexes()
        for index in indexes["results"]:
            index.delete()
        delete_crawled_content_domains_items()
        self._known_indexes = set()

    def get_stats(self):
        return self.client.get_all_stats()

    def index_obj(self, index, obj, timestamp):
        document = obj.get_search_document(timestamp)
        if document:
            self.update(index, document)

    ## INDEX ##
    def index_crawled_item(self, domain, item, timestamp):
        content = item.get_html2text_content()
        cid = sha256(content.encode()).hexdigest()
        index = domain.get_domain_type()
        # update
        if r_search.exists(f'crawled:{index}:{cid}'):
            document = {'uuid': cid, 'last': timestamp}
            self.update(index, document)
        # new content
        else:
            r_search.sadd(f'crawled:{index}:contents', cid)
            document = {'uuid': cid, 'content': content, 'last': timestamp}
            self.add(index, document)
        r_search.hset(f'crawled:{index}:{cid}', domain.id, item.id)

    def index_chat_message(self, message):
        index = f'c{message.get_protocol()}'
        if index not in MESSAGES_INDEXES:
            self._create_index(index)
            MESSAGES_INDEXES.add(index)
        timestamp = message.get_timestamp()
        chat_instance = message.get_chat_instance()
        chat = chats_viewer.get_obj_chat('chat', chat_instance, message.get_chat_id())
        self.index_obj(index, chat, timestamp)
        self.index_obj(index, message, timestamp)
        user_account = message.get_user_account()
        if user_account:
            _, user_account_subtype, user_account_id = user_account.split(':', 2)
            user_account = UsersAccount.UserAccount(user_account_id, user_account_subtype)
            self.index_obj(index, user_account, timestamp)

    def index_pdf(self, obj):
        document = obj.get_search_document()
        if document:
            self.update('pdf', document)

    def index_ocr(self, obj):
        document = obj.get_search_document()
        if document:
            self.update('ocr', document)


if IS_MEILISEARCH_ENABLED:
    Engine = MeiliSearch()
else:
    Engine = None

#### INDEXER ####

## DOMAIN ##

def index_crawled_item(item):
    domain = Domains.Domain(item.get_domain())
    timestamp = domain.get_item_timestamp(item.id)
    if timestamp:
        Engine.index_crawled_item(domain, item, timestamp)
    # else: # TODO LOG


def _index_crawled_domain(dom_id):
    domain = Domains.Domain(dom_id)
    for timestamp in domain.get_timestamps_up():
        for item_id in domain.get_crawled_items_by_epoch(epoch=timestamp):
            item = Items.Item(item_id)
            if not item.exists():
                continue
            Engine.index_crawled_item(domain, item, timestamp)


def index_crawled():
    # multi process
    if not r_search.exists('to_index:crawled'):
        for domain_type in Domains.get_all_domains_types():
            for dom_id in Domains.get_domains_up_by_type(domain_type):
                r_search.sadd('to_index:crawled', dom_id)
    while True:
        dom_id = r_search.spop('to_index:crawled')
        if not dom_id:
            break
        try:
            _index_crawled_domain(dom_id)
        except MeilisearchError:
            # send back for resume
            r_search.sadd('to_index:crawled', dom_id)
            print('send back to indexing queue')
            time.sleep(10)


## MESSAGE ##

def index_chats():
    for obj in chats_viewer.get_chats_iterator():
        index = f'c{obj.get_protocol()}'
        document = obj.get_search_document()
        if document:
            Engine.update(index, document)

def index_messages():
    for message in chats_viewer.get_messages_iterator():
        index = f'c{message.get_protocol()}'
        document = message.get_search_document()
        if document:
            Engine.update(index, document)

def index_user_accounts():
    for obj in UsersAccount.UserAccounts().get_iterator():
        index = f'c{obj.get_protocol()}'
        document = obj.get_search_document()
        if document:
            Engine.update(index, document)

## DESCRIPTION ##

def index_image_description(image):
    index = f'desc-img'
    document = image.get_search_document()
    if document:
        Engine.add(index, document)

def index_images_descriptions():
    for image in Images.get_all_images_objects():
        index_image_description(image)


def index_screenshot_description(screenshot):
    index = f'desc-screen'
    document = screenshot.get_search_document()
    if document:
        Engine.add(index, document)

def index_screenshots_descriptions():
    for screenshot in Screenshots.get_screenshots_obj_iterator():
        index_screenshot_description(screenshot)


def index_domain_description(domain_id):
    index = f'desc-dom'
    domain = Domains.Domain(domain_id)
    document = domain.get_search_description_document()
    if document:
        Engine.add(index, document)

def index_domains_descriptions():
    for dom_id in Domains.get_domains_up_by_type('onion'):
        index_domain_description(dom_id)
    for dom_id in Domains.get_domains_up_by_type('web'):
        index_domain_description(dom_id)


## TITLE ##  # TODO update only

def index_title(obj):
    index = 'title'
    document = obj.get_search_document()
    if document:
        Engine.update(index, document)


def index_titles():
    index = 'title'
    for obj in Titles.Titles().get_iterator():
        document = obj.get_search_document()
        if document:
            Engine.update(index, document)


## FILENAME ##  # TODO update only

def index_forum_thread(obj):
    index = 'forum'
    document = obj.get_search_document()
    if document:
        Engine.update(index, document)

def index_forum_post(obj):
    index = 'forum'
    document = obj.get_search_document()
    if document:
        Engine.update(index, document)

def index_forum_posts():
    for forum_id in Forums.get_forums():
        for obj_id, _score in ForumThreads.ForumThreads().get_id_iterators_by_subtype(forum_id):
            thread = ForumThreads.ForumThread(obj_id, forum_id)
            index_forum_thread(thread)
            posts, _ = thread._get_posts()
            for post_gid, _timestamp in posts:
                _, _, post_id = post_gid.split(':', 2)
                index_forum_post(Posts.Post(post_id))

def index_file_name(obj):
    index = 'filename'
    document = obj.get_search_document()
    if document:
        Engine.update(index, document)


def index_file_names():
    index = 'filename'
    for obj in FilesNames.FilesNames().get_iterator():
        document = obj.get_search_document()
        if document:
            Engine.update(index, document)


def index_pdf(obj):
    index = 'pdf'
    document = obj.get_search_document()
    if document:
        Engine.update(index, document)


def index_pdfs():
    try:
        from lib.objects import PDFs
        for obj in PDFs.PDFs().get_iterator():
            index_pdf(obj)
    except Exception:
        pass


def index_ocr(obj):
    index = 'ocr'
    document = obj.get_search_document()
    if document:
        Engine.update(index, document)


def index_ocrs():
    try:
        from lib.objects import Ocrs
        for obj in Ocrs.Ocrs().get_iterator():
            index_ocr(obj)
    except Exception:
        pass


INDEXING_FUNCTIONS = {
    'all': index_all,
    'crawled': index_crawled,
    'chats': index_chats,
    'messages': index_messages,
    'forum': index_forum_posts,
    'user_accounts': index_user_accounts,
    'images_descriptions': index_images_descriptions,
    'screenshots_descriptions': index_screenshots_descriptions,
    'domains_descriptions': index_domains_descriptions,
    'titles': index_titles,
    'file_names': index_file_names,
    'pdfs': index_pdfs,
    'ocrs': index_ocrs,
}

## --INDEXER-- ##


def remove_document(index_name, obj_gid):
    if Engine is None:
        return
    Engine.remove(index_name, get_obj_uuid5(obj_gid))


def delete_index(index_name):
    if Engine is None:
        return
    Engine._delete(index_name)
    Engine._create_index(index_name)

def log(user_id, index, to_search):
    logger.warning(f'{user_id} search: {index} - {to_search}')


#### PAGINATION ####

def sanityze_page(page):
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1
    if page < 1:
        page = 1
    return page

def _get_pagination_nb_first_last(pagination, nb_per_page):
    first = (pagination['page'] - 1) * nb_per_page
    last = first + nb_per_page
    if last > pagination['total']:
        last = pagination['total']
    if first == 0:
        first = 1
    return first, last

def extract_pagination_from_result(result, nb_per_page, page):
    if 'page' in result:
        pagination = {'page': result['page'], 'total': result['totalHits'], 'nb_pages': result['totalPages']}
    else:
        pagination = create_pagination_multiple_indexes(result['estimatedTotalHits'], nb_per_page, page)
    pagination['nb_first'], pagination['nb_last'] = _get_pagination_nb_first_last(pagination, nb_per_page)
    return pagination

def create_pagination_multiple_indexes(total, nb_per_page, page):
    pagination = {'page': page, 'total': total}
    pages = total / nb_per_page
    if pages.is_integer():
        pagination['nb_pages'] = int(pages)
    else:
        pagination['nb_pages'] = int(pages) + 1
    pagination['nb_first'], pagination['nb_last'] = _get_pagination_nb_first_last(pagination, nb_per_page)
    return pagination


## --PAGINATION-- ##

#### CRAWLED CONTENT ####

def get_crawled_content_domains_items(index, h):
    return r_search.hgetall(f'crawled:{index}:{h}')


def delete_crawled_content_domains_items():
    for index in Domains.get_all_domains_types():
        while True:
            c_id = r_search.spop(f'crawled:{index}:contents')
            if not c_id:
                break
            r_search.delete(f'crawled:{index}:{c_id}')
    r_search.delete('to_index:crawled')


#### API ####

def api_check_indexes(indexes):
    for index in indexes:
        if index not in INDEX_NAMES:
            return {"status": "error", "reason": f"Unknow index: {index}"}, 404
    return None, 200


def api_search(data):
    indexes = data.get("indexes")
    to_search = data.get("search")
    page = sanityze_page(data.get("page"))
    nb_per_page = 20
    user_id = data.get("user_id")
    log(user_id, str(indexes), to_search)

    r = api_check_indexes(indexes)
    if r[1] != 200:
        return r

    if not indexes:
        indexes = get_indexes_names()

    if Engine is None or not is_meilisearch_enabled():
        return {"status": "error", "error_type": "meilisearch_unreachable", "reason": "Meilisearch is disabled or unreachable."}, 503
    sort = data.get("sort", "recent")
    if sort not in ("best", "recent", "hybrid"):
        return {"status": "error", "reason": "Invalid sort"}, 400
    facets_filter = data.get("facets") or {}
    if facets_filter and not isinstance(facets_filter, dict):
        return {"status": "error", "reason": "Invalid facets"}, 400
    for _k in facets_filter:
        if _k not in FACET_ATTRIBUTES:
            return {"status": "error", "reason": f"Unknown facet: {_k}"}, 400
    explain = bool(data.get("explain"))

    forum_ids = data.get("forum_ids", [])
    if isinstance(forum_ids, str):
        forum_ids = [forum_id for forum_id in forum_ids.split(',') if forum_id]

    forum_types = data.get("forum_types", [])
    if isinstance(forum_types, str):
        forum_types = [forum_type for forum_type in forum_types.split(',') if forum_type]
    invalid_forum_types = set(forum_types) - {'post', 'forum-thread'}
    if invalid_forum_types:
        return {"status": "error", "reason": "Invalid forum result type"}, 400

    timestamp_from = data.get("from")
    timestamp_to = data.get("to")

    if timestamp_from:
        try:
            timestamp_from = Date.convert_str_date_to_epoch(timestamp_from)
        except:
            return {"status": "error", "reason": "Invalid date from"}, 400
    if timestamp_to:
        try:
            timestamp_to = Date.convert_str_date_to_epoch_end(timestamp_to)
        except:
            return {"status": "error", "reason": "Invalid date to"}, 400

    entity_matches = detect_entities(to_search or '')
    expanded_query, expanded_terms = expand_query(to_search or '')
    try:
        result = Engine.search(indexes, expanded_query, page=page, nb=nb_per_page, timestamp_from=timestamp_from, sort=sort,
                               timestamp_to=timestamp_to, forum_ids=forum_ids, forum_types=forum_types,
                               facets_filter=facets_filter, explain=explain)
    except MeilisearchTimeoutError:
        return {
            "status": "error",
            "error_type": "meilisearch_timeout",
            "reason": "The connection to Meilisearch timed out. Please try again in a few minutes."
        }, 503
    except MeilisearchCommunicationError:
        return {
            "status": "error",
            "error_type": "meilisearch_unreachable",
            "reason": "Meilisearch is unreachable. Install/start Meilisearch or check the connection settings."
        }, 503

    objs = []
    pagination = {}
    if result.get("hits"):
        pagination = extract_pagination_from_result(result, nb_per_page, page)
        for res in result['hits']:
            # crawled items
            if not res.get('id'):
                index = res['_federation']['indexUid']
                #
                domains_items = get_crawled_content_domains_items(index, res['uuid'])
                obj = Items.Item(domains_items[next(iter(domains_items))])
                meta = obj.get_meta(options={'link', 'crawler'})
                # TODO change default separator
                meta['date'] = meta['date'].replace('/', '-')
                meta['domains_items'] = domains_items
            else:
                obj_type, subtype, obj_id = res['id'].split(':', 2)
                if obj_type == 'item':
                    obj = Items.Item(obj_id)
                    meta = obj.get_meta(options={'url'})
                elif obj_type == 'message':
                    message = Messages.Message(obj_id)
                    meta = message.get_meta(
                        options={'barcodes', 'files', 'files-names', 'forwarded_from', 'full_date', 'icon', 'images',
                                 'language', 'link', 'parent', 'parent_meta', 'protocol', 'qrcodes', 'reactions',
                                 'user-account'})
                    meta['protocol'] = chats_viewer.get_chat_protocol_meta(meta['protocol'])
                    # if meta.get('reply_to'):
                    #     print(meta['reply_to'])
                elif obj_type == 'chat':
                    obj = chats_viewer.get_obj_chat('chat', subtype, obj_id)
                    meta = obj.get_meta(options={'link', 'icon', 'info', 'nb_participants', 'protocol', 'tags_safe', 'username', 'usernames'})
                elif obj_type == 'user-account':
                    obj = UsersAccount.UserAccount(obj_id, subtype)
                    meta = obj.get_meta(options={'link', 'icon', 'info', 'nb_chats', 'protocol', 'tags_safe', 'username', 'usernames'})
                elif obj_type == 'image':
                    obj = Images.Image(obj_id)
                    meta = obj.get_meta(options={'link', 'tags_safe'})
                elif obj_type == 'screenshot':
                    obj = Screenshots.Screenshot(obj_id)
                    meta = obj.get_meta(options={'link', 'tags_safe'})
                elif obj_type == 'domain':
                    obj = Domains.Domain(obj_id)
                    meta = obj.get_meta(options={'img', 'link', 'tags_safe'})
                    # TEMP FIX  TODO
                    meta['domain_type'] = meta['type']
                    meta['type'] = 'domain'
                    meta['first_seen'] = meta['first_seen'].replace('/', '-')
                    meta['last_check'] = meta['last_check'].replace('/', '-')
                elif obj_type == 'file-name':
                    obj = FilesNames.FileName(obj_id)
                    meta = obj.get_meta(options={'link'})
                elif obj_type == 'post':
                    obj = Posts.Post(obj_id)
                    meta = obj.get_meta(options={'content', 'link', 'user-account', 'full_date', 'state', 'svg_icon'})
                elif obj_type == 'forum-thread':
                    obj = ForumThreads.ForumThread(obj_id, subtype)
                    meta = obj.get_meta(options={'link', 'name', 'info', 'nb_posts', 'icon'})
                elif obj_type == 'title':
                    obj = Titles.Title(obj_id)
                    meta = obj.get_meta(options={'link'})
                else:
                    print('ERROR UNKNOWN OBJ RESULT', res)
                    continue  # TODO ERROR

            meta['result'] = (res.get('_formatted') or {}).get('content', '')
            if explain:
                meta['explanation'] = {'rankingScore': res.get('_rankingScore'), 'rankingScoreDetails': res.get('_rankingScoreDetails'),
                    'matchedTerms': list((res.get('_matchesPosition') or {}).keys()), 'highlightedSnippet': meta['result'],
                    'semanticRatio': get_semantic_ratio(indexes, sort, query=to_search), 'expandedTerms': expanded_terms, 'entityMatches': entity_matches}
            objs.append(meta)
    pagination['facets'] = result.get('facetDistribution') or {}
    pagination['expanded_terms'] = expanded_terms
    pagination['entity_matches'] = entity_matches
    return (objs, pagination), 200

## --API-- ##

### SEARCH
# Limitation: The maximum number of terms taken into account for each search query is 10.
# If a search query includes more than 10 words, all words after the 10th will be ignored.

### INDEX
# .index(index_name).update_documents([doc])  # partial update -> keep old filed
# .index(index_name).add_documents() -> delete old fields
# .swap_indexes -> update index without interruption

### delete
# client.index('movies').delete_all_documents()
# .index('movies').delete_document(25684)


if __name__ == '__main__':
    index_all()
    # delete_crawled_content_domains_items()
    # Engine.setup_indexes_searchable_filterable_sortable()
    # import json
    # print(json.dumps(Engine.get_stats(), indent=2))
    # data = {'indexes': ['filename', 'title'], 'user_id': 'admin@admin.test', 'search': 'stick'}
    # r = api_search(data)
    # print(r)
