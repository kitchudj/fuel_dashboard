import os
import warnings
from abc import ABC, abstractmethod
from typing import Optional, Callable

import requests
from joblib import Memory
import pydash

default_cache_path = os.environ.get("DEFAULT_CACHE_PATH")


# -----------------------------
# Helpers
# -----------------------------
def safe_json(resp):
    try:
        return resp.json()
    except ValueError:
        preview = resp.text[:500]
        raise ValueError(
            f"Invalid JSON response | "
            f"status={resp.status_code} | "
            f"content-type={resp.headers.get('Content-Type')} | "
            f"preview={preview}"
        )


def fetch_with_retries(method, url, retries=3, **kwargs):
    last_err = None
    for attempt in range(retries):
        try:
            resp = method(url, **kwargs)
            resp.raise_for_status()
            return safe_json(resp)
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            if attempt < retries - 1:
                print(f"Retrying {url} (attempt {attempt + 1}) due to {e}")
    raise last_err


# -----------------------------
# Base classes
# -----------------------------
class ResourceLoader(ABC):
    @abstractmethod
    def load(self):
        pass


def _inner_load_from(loader: ResourceLoader):
    return loader.load()


class LoaderCache:
    def __init__(self, cache_path: Optional[str] = None, compress: bool = True):
        self._cache_path = cache_path or default_cache_path
        if self._cache_path and os.path.exists(self._cache_path):
            self._disable_caching = False
            self._memory = Memory(self._cache_path, compress=compress)
            self._inner_load_from = self._memory.cache(_inner_load_from)
        else:
            warnings.warn(
                f"{self._cache_path} does not exist. caching will be disabled"
            )
            self._disable_caching = True
            self._inner_load_from = None

    def load_from(self, loader: ResourceLoader):
        if not isinstance(loader, ResourceLoader):
            raise ValueError("loader must be an instance of ResourceLoader")
        if self._disable_caching:
            return loader.load()
        return self._inner_load_from(loader)


# -----------------------------
# Loaders
# -----------------------------
class PagedHTTPListLoader(ResourceLoader):
    filter_list: Optional[Callable]

    def __init__(
        self,
        list_url: str,
        resp_data_field: str = "results",
        resp_page_field: str = "paging",
        query_pnum_name: str = "pnum",
        query_psize_name: str = "psize",
        query_extras: Optional[dict] = None,
        psize: int = 100,
    ):
        self.list_url = list_url
        self.resp_data_field = resp_data_field
        self.resp_page_field = resp_page_field
        self.query_pnum_name = query_pnum_name
        self.query_psize_name = query_psize_name
        self.query_extras = query_extras or {}
        self.psize = psize

    @staticmethod
    def check_if_last_page(paging_block: Optional[dict] = None):
        return paging_block.get("isLastPage", False)

    def load(self):
        pnum = 1
        accumulator = []

        while True:
            params = {
                self.query_psize_name: self.psize,
                self.query_pnum_name: pnum,
                **self.query_extras,
            }

            response = fetch_with_retries(
                requests.get,
                self.list_url,
                params=params,
                timeout=60,
            )
            pnum += 1

            if isinstance(response, list):
                return response

            if not isinstance(response, dict):
                raise ValueError(f"Invalid paged response: {response}")

            paging_block = pydash.get(response, self.resp_page_field, {})
            list_block = pydash.get(response, self.resp_data_field, [])

            if hasattr(self, "filter_list") and callable(self.filter_list):
                list_block = self.filter_list(list_block)

            accumulator.extend(list_block)

            if self.check_if_last_page(paging_block) or not list_block:
                break

        return accumulator


class DashboardQueryLoader(ResourceLoader):
    def __init__(self, url, query, headers=None, user_token=None):
        self.url = url
        self.query = query
        self.user_token = user_token
        self.headers = (
            {"Content-Type": "application/json"} if headers is None else headers
        )

    def load(self):
        try:
            return fetch_with_retries(
                requests.post,
                self.url,
                data=self.query,
                headers=self.headers,
                timeout=60,
            )
        except Exception as e:
            print(f"Dashboard query failed: {e}")
            return None


class HTTPLoader(ResourceLoader):
    filter_list: Optional[Callable]

    def __init__(
        self,
        list_url: str,
        resp_data_field: str = "results",
        query_extras: Optional[dict] = None,
        psize: int = 100,
    ):
        self.list_url = list_url
        self.resp_data_field = resp_data_field
        self.query_extras = query_extras or {}
        self.psize = psize

    def load(self):
        params = {
            **self.query_extras,
            "pnum": 1,
            "psize": self.psize,
        }

        response = fetch_with_retries(
            requests.get,
            self.list_url,
            params=params,
            timeout=60,
        )

        if isinstance(response, list):
            return response

        if not isinstance(response, dict):
            raise ValueError(f"Invalid response: {response}")

        return pydash.get(response, self.resp_data_field)