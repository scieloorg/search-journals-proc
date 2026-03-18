import functools
import os

import requests


_DEFAULT_COLLECTION_CLASSIFICATIONS = ["scielonetwork"]


def _load_collection_config_from_endpoint(url):
    """
    Remote source of truth.
    Endpoint returns a list of objects with "code"/"acron" and "network_classification".
    """
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    mapping = {}

    if not isinstance(payload, list):
        return mapping

    for item in payload:
        if not isinstance(item, dict):
            continue
        key = item.get("code") or item.get("acron")
        if not key:
            continue
        classifications = item.get("network_classification") or []
        if not isinstance(classifications, list):
            classifications = []
        mapping[str(key).lower()] = [str(c) for c in classifications]

    return mapping


@functools.lru_cache(maxsize=1)
def get_collection_config():
    """
    Returns a dict {collection_acronym: [classifications]}.
    Fetched from ArticleMeta; failures propagate.
    """
    url = os.environ.get(
        "COLLECTION_IDENTIFIERS_URL",
        "https://articlemeta.scielo.org/api/v1/collection/identifiers/",
    )
    return _load_collection_config_from_endpoint(url)


def get_collection_classifications(collection_acronym):
    if not collection_acronym:
        return list(_DEFAULT_COLLECTION_CLASSIFICATIONS)
    return get_collection_config().get(
        str(collection_acronym).lower(),
        list(_DEFAULT_COLLECTION_CLASSIFICATIONS),
    )
