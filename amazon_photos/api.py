import asyncio
import logging.config
import math
import os
import platform
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from functools import partial
from hashlib import md5
from logging import getLogger, Logger
from pathlib import Path
from typing import Generator

import aiofiles
import numpy as np
import orjson
import pandas as pd
import psutil
from httpx import AsyncClient, Client, Response, Limits
from tqdm.asyncio import tqdm, tqdm_asyncio

from .constants import *
from .helpers import format_nodes, folder_relmap

try:
    get_ipython()
    import nest_asyncio

    nest_asyncio.apply()
except:
    ...

if platform.system() != 'Windows':
    try:
        import uvloop

        uvloop.install()
    except:
        ...

logging.config.dictConfig(LOG_CONFIG)
logger = getLogger(list(Logger.manager.loggerDict)[-1])


class AmazonPhotos:
    def __init__(self, *, tld: str = None, cookies: dict = None, db_path: str | Path = 'ap.parquet', tmp: str = '', **kwargs):
        self.n_threads = len(os.sched_getaffinity(0))
        self.tld = tld or self.determine_tld(cookies)
        self.drive_url = f'https://www.amazon.{self.tld}/drive/v1'
        self.cdn_url = 'https://content-na.drive.amazonaws.com/cdproxy/nodes'  # variant? 'https://content-na.drive.amazonaws.com/cdproxy/v1/nodes'
        self.base_params = {
            'asset': 'ALL',
            'tempLink': 'false',
            'resourceVersion': 'V2',
            'ContentType': 'JSON',
        }
        self.limits = kwargs.pop('limits', Limits(
            max_connections=2000,
            max_keepalive_connections=None,
            keepalive_expiry=5.0,
        ))
        self.client = Client(
            http2=False,  # todo: "Max outbound streams is 128, 128 open" errors with http2?
            follow_redirects=True,
            timeout=60,
            headers={
                'user-agent': random.choice(USER_AGENTS),
                'x-amzn-sessionid': cookies.get('session-id') if cookies else os.getenv('session_id'),
            },
            cookies=cookies or {
                f'ubid-acb{tld}': os.getenv(f'ubid_acb{tld}'),
                f'at-acb{tld}': os.getenv(f'at_acb{tld}'),
                'session-id': os.getenv('session_id'),
            }
        )
        self.tmp = Path(tmp)
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(db_path).expanduser()
        self.root = self.get_root()
        self.folders = self.get_folders()
        self.db = self.load_db(**kwargs)
        self.tree = self.build_tree()

    def determine_tld(self, cookies: dict) -> str:
        """
        Determine top-level domain based on cookies

        @param cookies: cookies dict
        @return: top-level domain
        """
        for k, v in cookies.items():
            if k.startswith(x := 'at-acb'):
                return k.split(x)[-1]

    async def process(self, fns: Generator, max_connections: int = None, **kwargs):
        """
        Efficiently process a generator of async partials

        @param fns: generator of async partials
        @param max_connections: max number of connections to use (controlled by semaphore)
        @param kwargs: optional kwargs to pass to AsyncClient and tqdm
        @return: list of results
        """
        desc = kwargs.pop('desc', None)  # tqdm
        defaults = {
            'http2': kwargs.pop('http2', True),
            'follow_redirects': kwargs.pop('follow_redirects', True),
            'timeout': kwargs.pop('timeout', 30.0),
            'verify': kwargs.pop('verify', False),
        }
        headers, cookies = (
            kwargs.pop('headers', {}) | dict(self.client.headers),
            kwargs.pop('cookies', {}) | dict(self.client.cookies)
        )
        sem = asyncio.Semaphore(max_connections or self.limits.max_connections)
        async with AsyncClient(limits=self.limits, headers=headers, cookies=cookies, **defaults, **kwargs) as client:
            tasks = (fn(client=client, sem=sem) for fn in fns)
            if desc:
                return await tqdm_asyncio.gather(*tasks, desc=desc)
            return await asyncio.gather(*tasks)

    async def async_backoff(self, fn, sem: asyncio.Semaphore, *args, m: int = 20, b: int = 2, max_retries: int = 12, **kwargs) -> any:
        """Async truncated exponential backoff"""
        for i in range(max_retries + 1):
            try:
                async with sem:

                    r = await fn(*args, **kwargs)

                    if r.status_code == 409:  # conflict
                        logger.debug(f'{r.status_code} {r.text}')
                        return r

                    if r.status_code == 401:  # BadAuthenticationData
                        logger.error(f'{r.status_code} {r.text}')
                        logger.error(f'Cookies expired. Log in to Amazon Photos and copy fresh cookies.')
                        sys.exit(1)

                    r.raise_for_status()

                    if self.tmp.name:
                        async with aiofiles.open(f'{self.tmp}/{time.time_ns()}', 'wb') as fp:
                            await fp.write(r.content)
                    return r
            except Exception as e:
                if i == max_retries:
                    logger.warning(f'Max retries exceeded\n{e}')
                    return
                t = min(random.random() * (b ** i), m)
                logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                await asyncio.sleep(t)

    def backoff(self, fn, *args, m: int = 20, b: int = 2, max_retries: int = 12, **kwargs) -> any:
        """Exponential truncated exponential backoff"""
        for i in range(max_retries + 1):
            try:
                r = fn(*args, **kwargs)

                if r.status_code == 409:  # conflict
                    logger.debug(f'{r.status_code} {r.text}')
                    return r

                if r.status_code == 400:  # malformed query
                    logger.error(f'{r.status_code} {r.text}')
                    logger.error(f'Incorrect query syntax. See readme for query language syntax.')
                    sys.exit(1)

                if r.status_code == 401:  # "BadAuthenticationData"
                    logger.error(f'{r.status_code} {r.text}')
                    logger.error(f'Cookies expired. Log in to Amazon Photos and copy fresh cookies.')
                    sys.exit(1)

                r.raise_for_status()

                if self.tmp.name:
                    with open(f'{self.tmp}/{time.time_ns()}', 'wb') as fp:
                        fp.write(r.content)
                return r
            except Exception as e:
                if i == max_retries:
                    logger.warning(f'Max retries exceeded\n{e}')
                    return
                t = min(random.random() * (b ** i), m)
                logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                time.sleep(t)

    def usage(self) -> dict | pd.DataFrame:
        """
        Get Amazon Photos current memory usage stats

        @param as_df: return as pandas DataFrame
        @return: dict or pd.DataFrame
        """
        r = self.backoff(
            self.client.get,
            f'{self.drive_url}/account/usage',
            params=self.base_params,
        )
        data = r.json()
        data.pop('lastCalculated')
        return pd.DataFrame(
            {
                'type': k,
                'billable_bytes': int(v['billable']['bytes']),
                'billable_count': int(v['billable']['count']),
                'total_bytes': int(v['total']['bytes']),
                'total_count': int(v['total']['count']),
            } for k, v in data.items()
        )

    async def q(self, client: AsyncClient, sem: asyncio.Semaphore, filters: str, offset: int, limit: int = MAX_LIMIT) -> dict:
        """
        Lower level access to Amazon Photos search.
        The `query` method using this to search all media in Amazon Photos.

        @param client: an async client instance
        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @return: media data as a dict
        """
        r = await self.async_backoff(
            client.get,
            sem,
            f'{self.drive_url}/search',
            params=self.base_params | {
                "limit": limit,
                "offset": offset,
                "filters": filters,
                "lowResThumbnail": "true",
                "searchContext": "customer",
                "sort": "['createdDate DESC']",
            },
        )
        return r.json()

    def query(self, filters: str = "type:(PHOTOS OR VIDEOS)", offset: int = 0, limit: int = math.inf, **kwargs) -> list[dict] | pd.DataFrame:
        """
        Search all media in Amazon Photos

        @param filters: query Amazon Photos database. See query language syntax in readme.
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @return: media data as a dict or DataFrame
        """
        initial = self.backoff(
            self.client.get,
            f'{self.drive_url}/search',
            params=self.base_params | {
                "limit": MAX_LIMIT,
                "offset": offset,
                "filters": filters,
                "lowResThumbnail": "true",
                "searchContext": "customer",
                "sort": "['createdDate DESC']",
            },
        ).json()
        res = [initial]
        # small number of results, no need to paginate
        if initial['count'] <= MAX_LIMIT:
            # return dump(as_df, res, out)
            return format_nodes(pd.DataFrame(initial.get('data', [])))

        offsets = range(offset, min(initial['count'], limit), MAX_LIMIT)
        fns = (partial(self.q, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='Getting media', **kwargs)))
        # return dump(as_df, res, out)
        return format_nodes(pd.DataFrame(y for x in res for y in x.get('data', [])))

    def photos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all photos"""
        return self.query('type:(PHOTOS)', **kwargs)

    def videos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all videos"""
        return self.query('type:(VIDEOS)', **kwargs)

    @staticmethod
    def _md5(p):
        return p, md5(p.read_bytes()).hexdigest()

    def dedup_files(self, path: str | Path, md5s: set[str], max_workers=psutil.cpu_count(logical=False)) -> list[Path]:
        """
        Deduplicate all files in folder by comparing md5 against database md5

        @param db: database
        @param path: path to folder to dedup
        @param max_workers: max number of workers to use
        @return: deduped files
        """
        dups = []
        unique = []
        files = [x for x in Path(path).rglob('*') if x.is_file()]

        with ProcessPoolExecutor(max_workers=max_workers) as e:
            fut = {e.submit(self._md5, file): file for file in files}
            with tqdm(total=len(files), desc='Checking for duplicate files') as pbar:
                for future in as_completed(fut):
                    file, file_md5 = future.result()
                    if file_md5 not in md5s:
                        unique.append(file)
                    else:
                        dups.append(file)
                    pbar.update()
        logger.info(f'{len(dups)} Duplicate files found')
        logger.info(f'{len(unique)} Unique files found')
        return unique

    def upload(self, path: str | Path, chunk_size=64 * 1024, refresh: bool = True, md5s: set[str] = None, **kwargs) -> list[dict]:
        """
        Upload files to Amazon Photos

        @param path: path to folder to upload
        @param md5s: set of file hashes to deduplicate against
        @param chunk_size: optional chunk size
        @param refresh: refresh database after upload
        @param kwargs: optional kwargs to pass to AsyncClient
        @return: upload response
        """

        async def stream_bytes(file: Path) -> bytes:
            async with aiofiles.open(file, 'rb') as f:
                while chunk := await f.read(chunk_size):
                    yield chunk

        async def post(client: AsyncClient, sem: asyncio.Semaphore, pid: str, file: Path, max_retries: int = 12, m: int = 20, b: int = 2):
            for i in range(max_retries + 1):
                try:
                    async with sem:
                        r = await client.post(
                            f'https://content-na.drive.amazonaws.com/cdproxy/nodes',
                            data=stream_bytes(file),
                            params={
                                'name': file.name,
                                'kind': 'FILE',
                                # 'parents': [pid], # careful, official docs are wrong again
                                'parentNodeId': pid,
                            }
                        )

                        if r.status_code == 409:  # conflict
                            logger.debug(f'{r.status_code} {r.text}')
                            return r

                        if r.status_code == 400:
                            logger.error(f'{r.status_code} {r.text}')
                            sys.exit(1)

                        if r.status_code == 401:  # BadAuthenticationData
                            logger.error(f'{r.status_code} {r.text}')
                            logger.error(f'Cookies expired. Log in to Amazon Photos and copy fresh cookies.')
                            sys.exit(1)
                        r.raise_for_status()
                        return r
                except Exception as e:
                    if i == max_retries:
                        logger.warning(f'Max retries exceeded\n{e}')
                        return
                    t = min(random.random() * (b ** i), m)
                    logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                    await asyncio.sleep(t)

        path = Path(path)
        folder_map, folders = self.create_folders(path)

        if md5s:
            files = self.dedup_files(path, md5s)
        else:
            if 'md5' in self.db.columns:
                files = self.dedup_files(path, set(self.db.md5))
            else:
                logger.warning('`md5` column missing from database, skipping deduplication checks.')
                files = (x for x in path.rglob('*') if x.is_file())

        relmap = folder_relmap(path.name, files, folder_map)
        fns = (partial(post, pid=pid, file=file) for pid, file in relmap)
        res = asyncio.run(self.process(fns, desc='Uploading files', **kwargs))
        if refresh:
            self.refresh_db()
        return res

    def download(self, node_ids: list[str] | pd.Series, out: str = 'media', chunk_size: int = None, **kwargs) -> dict:
        """
        Download files from Amazon Photos

        @param node_ids: list of media node ids to download
        @param out: path to save files
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        params = {
            'querySuffix': '?download=true',
            'ownerId': self.root['ownerId'],
        }

        async def get(client: AsyncClient, sem: asyncio.Semaphore, node: str) -> None:
            logger.debug(f'Downloading {node}')
            try:
                async with sem:
                    url = f'{self.drive_url}/nodes/{node}/contentRedirection'
                    async with client.stream('GET', url, params=params) as r:
                        content_disposition = dict([y for x in r.headers['content-disposition'].split('; ') if len((y := x.split('='))) > 1])
                        fname = content_disposition['filename'].strip('"')
                        async with aiofiles.open(out / f"{node}_{fname}", 'wb') as fp:
                            async for chunk in r.aiter_bytes(chunk_size):
                                await fp.write(chunk)
            except Exception as e:
                logger.debug(f'Download FAILED for {node}\t{e}')

        fns = (partial(get, node=node) for node in node_ids)
        asyncio.run(self.process(fns, desc='Downloading media', **kwargs))
        return {'timestamp': time.time_ns(), 'nodes': node_ids}

    def trashed(self, filters: str = '', offset: int = 0, limit: int = MAX_LIMIT, as_df: bool = True, out: str = 'trashed.json', **kwargs) -> list[dict]:
        """
        Get trashed nodes. Essentially a view your trash bin in Amazon Photos.

        **Note**: Amazon restricts API access to first 9999 nodes

        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @param as_df: return as pandas DataFrame
        @param out: path to save results
        @return: trashed nodes as a dict or DataFrame
        """
        initial = self.backoff(
            self.client.get,
            f'{self.drive_url}/trash',
            params={
                'sort': "['modifiedDate DESC']",
                'limit': MAX_LIMIT,
                'offset': offset,
                'filters': filters or 'kind:(FILE* OR FOLDER*) AND status:(TRASH*)',
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
                '_': int(time.time_ns() // 1e6)
            },
        ).json()
        res = [initial]
        # small number of results, no need to paginate
        if initial['count'] <= MAX_LIMIT:
            # return dump(as_df, res, out)
            return format_nodes(pd.DataFrame(initial.get('data', [])))

        # see AWS error: E.g. "Offset + limit cannot be greater than 9999"
        # offset must be 9799 + limit of 200
        if initial['count'] > MAX_NODES:
            offsets = MAX_NODE_OFFSETS
        else:
            offsets = range(offset, min(initial['count'], limit), MAX_LIMIT)
        fns = (partial(self._get_nodes, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='Getting trashed nodes', **kwargs)))
        # return dump(as_df, res, out)
        return format_nodes(pd.DataFrame(y for x in res for y in x.get('data', [])))

    def trash(self, node_ids: list[str] | pd.Series, filters: str = '', **kwargs) -> list[dict]:
        """
        Move nodes or entire folders to trash bin

        @param node_ids: list of node ids to trash
        @return: the trash response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, sem: asyncio.Semaphore, ids: list[str]) -> Response:
            async with sem:
                return await client.patch(
                    f'{self.drive_url}/trash',
                    json={
                        'recurse': 'true',
                        'op': 'add',
                        'filters': filters,
                        'conflictResolution': 'RENAME',
                        'value': ids,
                        'resourceVersion': 'V2',
                        'ContentType': 'JSON',
                    }
                )

        id_batches = [node_ids[i:i + MAX_TRASH_BATCH] for i in range(0, len(node_ids), MAX_TRASH_BATCH)]
        fns = (partial(patch, ids=ids) for ids in id_batches)
        return asyncio.run(self.process(fns, desc='Trashing files', **kwargs))

    def restore(self, node_ids: list[str] | pd.Series) -> Response:
        """
        Restore nodes from trash bin

        @param node_ids: list of node ids to restore
        @return: the restore response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.patch(
            f'{self.drive_url}/trash',
            json={
                'recurse': 'true',
                'op': 'remove',
                'conflictResolution': 'RENAME',
                'value': node_ids,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        )

    def delete(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Permanently delete nodes from Amazon Photos

        @param node_ids: list of node ids to delete
        @return: the delete response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def post(client: AsyncClient, sem: asyncio.Semaphore, ids: list[str]) -> Response:
            async with sem:
                return await client.post(
                    f'{self.drive_url}/bulk/nodes/purge',
                    json={
                        'recurse': 'false',
                        'nodeIds': ids,
                        'resourceVersion': 'V2',
                        'ContentType': 'JSON',
                    }
                )

        id_batches = [node_ids[i:i + MAX_PURGE_BATCH] for i in range(0, len(node_ids), MAX_PURGE_BATCH)]
        fns = (partial(post, ids=ids) for ids in id_batches)
        return asyncio.run(self.process(fns, desc='Permanently deleting files', **kwargs))

    def aggregations(self, category: str, out: str = 'aggregations') -> dict:
        """
        Get Amazon Photos aggregations. E.g. see all identified people, locations, things, etc.

        @param category: category to get aggregations for. See readme for list of categories.
        @param out: path to save results
        @return: aggregations as a dict
        """
        if category == 'all':
            r = self.backoff(
                self.client.get,
                f'{self.drive_url}/search',
                params=self.base_params | {
                    'limit': 1,  # don't care about node info, just want aggregations
                    'lowResThumbnail': 'true',
                    'searchContext': 'all',
                    'groupByForTime': 'year',
                })
            data = r.json()['aggregations']
            if out:
                _out = Path(out)
                _out.mkdir(parents=True, exist_ok=True)
                [(_out / f'{k}.json').write_bytes(
                    orjson.dumps(data[k], option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)) for k in data]
            return data

        categories = {'allPeople', 'clusterId', 'familyMembers', 'favorite', 'location', 'people', 'things', 'time',
                      'type'}
        if category not in categories:
            raise ValueError(f'category must be one of {categories}')

        r = self.backoff(
            self.client.get,
            f'{self.drive_url}/search/aggregation',
            params={
                'aggregationContext': 'all',
                'category': category,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        )
        data = r.json()['aggregations']
        path = f'{category}.json' if category else out
        if out:
            # save to disk
            _out = Path(path)
            _out.mkdir(parents=True, exist_ok=True)
            _out.write_bytes(orjson.dumps(data))
        return data

    def favorite(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Add media to favorites

        @param node_ids: media node ids to add to favorites
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, sem: asyncio.Semaphore, node_id: str) -> dict:
            async with sem:
                r = await client.patch(f'{self.drive_url}/nodes/{node_id}', json={
                    'settings': {
                        'favorite': True,
                    },
                    'resourceVersion': 'V2',
                    'ContentType': 'JSON',
                })
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(self.process(fns, desc='adding media to favorites', **kwargs))

    def unfavorite(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Remove media from favorites

        @param node_ids: media node ids to remove from favorites
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, sem: asyncio.Semaphore, node_id: str) -> dict:
            async with sem:
                r = await client.patch(f'{self.drive_url}/nodes/{node_id}', json={
                    'settings': {
                        'favorite': False,
                    },
                    'resourceVersion': 'V2',
                    'ContentType': 'JSON',
                })
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(self.process(fns, desc='removing media from favorites', **kwargs))

    def create_album(self, album_name: str, node_ids: list[str] | pd.Series):
        """
        Create album

        @param album_name: name of album to create
        @param node_ids: media node ids to add to album
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        r = self.client.post(f'{self.drive_url}/nodes', json={
            'kind': 'VISUAL_COLLECTION',
            'name': album_name,
            'resourceVersion': 'V2',
            'ContentType': 'JSON',
        })
        created_album = r.json()
        album_id = created_album['id']
        self.client.patch(
            f'{self.drive_url}/nodes/{album_id}/children',
            json={
                'op': 'add',
                'value': node_ids,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        )
        return created_album

    def rename_album(self, album_id: str, name: str) -> dict:
        """
        Rename album

        @param album_id: album node id
        @param name: new name
        @return: operation response
        """
        return self.client.patch(
            f'{self.drive_url}/nodes/{album_id}',
            json={
                'name': name,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        ).json()

    def add_to_album(self, album_id: str, node_ids: list[str] | pd.Series) -> dict:
        """
        Add media to album

        @param album_id: album node id
        @param node_ids: media node ids to add to album
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.patch(
            f'{self.drive_url}/nodes/{album_id}/children',
            json={
                'op': 'add',
                'value': node_ids,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        ).json()

    def remove_from_album(self, album_id: str, node_ids: list[str] | pd.Series):
        """
        Remove media from album

        @param album_id: album node id
        @param node_ids: media node ids to remove from album
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.patch(
            f'{self.drive_url}/nodes/{album_id}/children',
            json={
                'op': 'remove',
                'value': node_ids,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        ).json()

    def hide(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Hide media

        @param node_ids: node ids to hide
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, sem: asyncio.Semaphore, node_id: str) -> dict:
            async with sem:
                r = await client.patch(
                    f'{self.drive_url}/nodes/{node_id}',
                    json={
                        'settings': {
                            'hidden': True,
                        },
                        'resourceVersion': 'V2',
                        'ContentType': 'JSON',
                    }
                )
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(self.process(fns, desc='hiding media', **kwargs))

    def unhide(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Unhide media

        @param node_ids: node ids to unhide
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, sem: asyncio.Semaphore, node_id: str) -> dict:
            async with sem:
                r = await client.patch(
                    f'{self.drive_url}/nodes/{node_id}',
                    json={
                        'settings': {
                            'hidden': False,
                        },
                        'resourceVersion': 'V2',
                        'ContentType': 'JSON',
                    }
                )
                return r.json()

        fns = (partial(patch, ids=ids) for ids in node_ids)
        return asyncio.run(self.process(fns, desc='unhiding media', **kwargs))

    def rename_cluster(self, cluster_id: str, name: str) -> dict:
        """
        Rename a cluster

        E.g. rename a person cluster from a default hash to a readable name

        @param cluster_id: target cluster id
        @param name: new name
        @return: operation response
        """
        return self.client.put(f'{self.drive_url}/cluster/name', json={
            'sourceCluster': cluster_id,
            'newName': name,
            'context': 'customer',
            'resourceVersion': 'V2',
            'ContentType': 'JSON',
        }).json()

    def combine_clusters(self, cluster_ids: list[str] | pd.Series, name: str) -> dict:
        """
        Combine clusters

        E.g. combine multiple person clusters into a single cluster

        @param cluster_ids: target cluster ids
        @param name: name of new combined cluster
        @return: operation response
        """

        if isinstance(cluster_ids, pd.Series):
            cluster_ids = cluster_ids.tolist()

        return self.client.post(f'{self.drive_url}/cluster', json={
            'clusterIds': cluster_ids,
            'newName': name,
            'context': 'customer',
            'resourceVersion': 'V2',
            'ContentType': 'JSON',
        }).json()

    def update_cluster_thumbnail(self, cluster_id: str, node_id: str) -> dict:
        """
        Update cluster thumbnail

        @param cluster_id: target cluster id
        @param node_id: node id to set as thumbnail
        @return: operation response
        """
        return self.client.put(
            f'{self.drive_url}/cluster/hero/{cluster_id}',
            json={
                'clusterId': cluster_id,
                'nodeId': node_id,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }).json()

    def add_to_family_vault(self, family_id: str, node_ids: list[str] | pd.Series) -> dict:
        """
        Add media to family vault

        @param family_id: family vault id
        @param node_ids: node ids to add to family vault
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.post(
            f'{self.drive_url}/familyArchive/', json={
                'nodesToAdd': node_ids,
                'nodesToRemove': [],
                'familyId': family_id,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }).json()

    def remove_from_family_vault(self, family_id: str, node_ids: list[str] | pd.Series) -> dict:
        """
        Remove media from family vault

        @param family_id: family vault id
        @param node_ids: node ids to remove from family vault
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        return self.client.post(
            f'{self.drive_url}/familyArchive/', json={
                'nodesToAdd': [],
                'nodesToRemove': node_ids,
                'familyId': family_id,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }).json()

    def get_root(self) -> dict:
        """
        isRoot:true filter is sketchy, can add extra data and request is still valid.
        seems like only check is something like: `if(data.contains("true"))`
        """
        r = self.backoff(self.client.get, f'{self.drive_url}/nodes', params={'filters': 'isRoot:true'} | self.base_params)
        root = r.json()['data'][0]
        logger.debug(f'Got root node: {root}')
        return root

    def get_folders(self) -> list[dict]:
        """
        Get all folders in Amazon Photos

        @return: list of folders
        """
        aclient = AsyncClient(
            http2=False,
            limits=self.limits,
            headers=self.client.headers,
            cookies=self.client.cookies,
            verify=False,
        )

        async def helper(node):
            url = f'{self.drive_url}/nodes/{node["id"]}/children'
            params = {'filters': 'kind:FOLDER'} | self.base_params
            r = await aclient.get(url, params=params)
            return r.json()['data']

        async def process_node(queue):
            result = []
            while True:
                node = await queue.get()
                if node.get('id'):
                    subfolders = await helper(node)
                    for folder in subfolders:
                        await queue.put(folder)
                    result.extend(subfolders)
                queue.task_done()
                if queue.empty():
                    return result

        async def main(data, batch_size=self.n_threads):
            Q = asyncio.Queue()
            for node in data:
                await Q.put(node)
            tasks = [asyncio.create_task(process_node(Q)) for _ in range(batch_size)]
            await Q.join()
            for task in tasks:
                task.cancel()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [y for x in results if x for y in x]

        folders = asyncio.run(main([{'id': self.root['id']}]))
        return folders

    def find_path(self, target: str, root: dict = None):
        """
        Find path to node in tree

        @param target: path to node
        @param root: optional root node to search from
        @return: node
        """
        if target == '':
            return self.tree
        root = root or self.tree
        if root['name'] == '':
            current_path = root['name']
        else:
            current_path = '/'.join(filter(len, root['path'].keys()))
        if current_path == target:
            return root
        for child in root['children']:
            result = self.find_path(target, child)
            if result is not None:
                return result
        return None

    def build_tree(self, folders: list[dict] = None) -> dict:
        """
        Build tree from folders

        @param folders: list of folders
        @return: tree
        """
        folders = folders or self.folders
        folders.extend([{'id': self.root['id'], 'name': '', 'parents': [None]}])
        nodes = {item['id']: {'name': item['name'], 'id': item['id'], 'children': [], 'path': {}} for item in folders}
        root = None
        for item in folders:
            node = nodes[item['id']]
            if parent_id := item.get('parents')[0]:
                parent = nodes[parent_id]
                parent['children'].append(node)
                node['path'] = parent['path'] | {parent['name']: parent_id} | {node['name']: node['id']}
            else:
                root = node
                root['path'] = {root['name']: root['id']}
        return root

    def print_tree(self, node: dict = None, show_id: bool = True, color: bool = True, indent: int = 4, prefix='', last=True):
        """
        Visualize your Amazon Photos folder structure

        @param node: optional root node to start from (default is root)
        @param show_id: optionally show node id in output
        @param color: optionally colorize output
        @param indent: optional indentation level
        @param prefix: optional prefix to add to output
        """
        node = node or self.tree
        conn = ('└── ' if last else '├── ') if node['name'] else ''
        name = node['name'] or '~'
        print(f"{prefix}{conn}{Red if color else ''}{name}{Reset if color else ''} {node['id'] if show_id else ''}")
        if node['name']:
            prefix += (indent * ' ') if last else '│' + (' ' * (indent - 1))
        else:
            prefix = ''
        for i, child in enumerate(node['children']):
            last_child = i == len(node['children']) - 1
            self.print_tree(child, show_id, color, indent, prefix, last_child)

    def create_folders(self, path: str | Path) -> tuple[dict, list[dict]]:
        """
        Recursively create folders in Amazon Photos

        @param path: path to root folder to create
        @return: a {folder: parent ID} map, and list of created folders
        """
        folder_map = {}
        aclient = AsyncClient(
            http2=False,
            limits=self.limits,
            headers=self.client.headers,
            cookies=self.client.cookies,
            verify=False,
        )

        def get_id(data: dict) -> str:
            return data.get('id') or data.get('info', {}).get('nodeId')

        async def mkdir(sem: asyncio.Semaphore, parent_id: str, path: Path):
            while True:
                r = await self.async_backoff(
                    aclient.post,
                    sem,
                    f'{self.drive_url}/nodes',
                    json=self.base_params | {
                        "kind": "FOLDER",
                        "name": path.name,
                        "parents": [parent_id],
                    },
                )
                if r.status_code < 300:
                    logger.debug(f'Folder created: {path.name}\t{r.status_code} {r.text}')
                else:
                    logger.error(f'Folder creation failed: {path.name}\t{r.status_code} {r.text}')
                folder_data = r.json()
                if get_id(folder_data):
                    return folder_data

        async def process_folder(sem: asyncio.Semaphore, Q: asyncio.Queue, parent_id: str, path_: Path) -> dict:
            folder = await mkdir(sem, parent_id, path_)
            fid = get_id(folder)
            idx = path_.parts.index(path.name)
            rel = Path(*path_.parts[idx:])
            folder_map[str(rel)] = fid  # track parent folder id relative to root
            for p in path_.iterdir():
                if p.is_dir():
                    await Q.put([fid, p])  # map to newly created folder(s) ID
            return folder

        async def folder_worker(sem: asyncio.Semaphore, Q: asyncio.Queue) -> list[dict]:
            res = []
            while True:
                parent_id, path = await Q.get()
                folder = await process_folder(sem, Q, parent_id, path)
                res.append(folder)
                Q.task_done()
                if Q.empty():
                    return res

        async def main(root, n_workers=self.n_threads, max_connections: int = self.limits.max_connections):
            sem = asyncio.Semaphore(max_connections)
            # check if local folder name exists in Amazon Photos root
            root_folder = self.find_path(root.name)
            if not root_folder:
                logger.debug(f'Root folder not found, creating root folder: {root.name}')
                root_folder = await mkdir(sem, self.root['id'], root)
                logger.debug(f'Created root folder: {root_folder = }')

            parent_id = get_id(root_folder)
            folder_map[root_folder['name']] = parent_id
            # init queue with root folder + sub-folders
            Q = asyncio.Queue()
            for p in root.iterdir():
                if p.is_dir():
                    await Q.put([parent_id, p])  # Add folder and parent ID to queue
            workers = [asyncio.create_task(folder_worker(sem, Q)) for _ in range(n_workers)]
            await Q.join()
            [w.cancel() for w in workers]
            res = await asyncio.gather(*workers, return_exceptions=True)
            return [y for x in filter(lambda x: isinstance(x, list), res) for y in x]

        res = asyncio.run(main(Path(path)))
        return folder_map, res

    def refresh_db(self) -> pd.DataFrame:
        """
        Refresh database
        """
        # todo: may be able to use finegrained node Range Query instead? although results may be limited to 9999 most recent nodes?
        logger.info(f'Refreshing db `{self.db_path}`')
        now = datetime.now()
        y, m, d = f'timeYear:({now.year})', f'timeMonth:({now.month})', f'timeDay:({now.day})'
        ap_today = self.query(f'type:(PHOTOS OR VIDEOS) AND {y} AND {m} AND {d}')
        y, m, d = f'timeYear:({now.year})', f'timeMonth:({now.month})', f'timeDay:({now.day + 1})'
        ap_tomorrow = self.query(f'type:(PHOTOS OR VIDEOS) AND {y} AND {m} AND {d}')

        cols = set(ap_today.columns) | set(ap_tomorrow.columns) | set(self.db.columns)

        # disgusting
        obj_dtype = np.dtypes.ObjectDType()
        df = pd.concat([
            ap_today.reindex(columns=cols).astype(obj_dtype),
            ap_tomorrow.reindex(columns=cols).astype(obj_dtype),
            self.db.reindex(columns=cols).astype(obj_dtype),
        ]).drop_duplicates('id').reset_index(drop=True)

        df = format_nodes(df)

        df.to_parquet(self.db_path)
        self.db = df
        return df

    def load_db(self, **kwargs):
        """
        Load database

        @param kwargs: optional kwargs to pass to pd.read_parquet
        @return: DataFrame
        """
        df = None
        if self.db_path.name and self.db_path.exists():
            try:
                df = format_nodes(pd.read_parquet(self.db_path, **kwargs))
            except Exception as e:
                logger.warning(f'Failed to load db `{self.db_path}`\t{e}')
        else:
            logger.warning(f'Database `{self.db_path}` not found, initializing new database')
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            df = format_nodes(self.query())
            df.to_parquet(self.db_path)
        return df

    def nodes(self, filters: list[str] = None, sort: list[str] = None, limit: int = MAX_LIMIT, offset: int = 0, **kwargs) -> pd.DataFrame | None:
        """
        Get first 9999 Amazon Drive nodes

        **Note**: Amazon restricts API access to first 9999 nodes. error: "Offset + limit cannot be greater than 9999"
        startToken/endToken are no longer returned, so we can't use them to paginate.
        """

        filters = ' AND '.join([f"({x})" for x in filters]) if filters else ''
        sort = str(sort) if sort else ''
        r = self.backoff(
            self.client.get,
            f'{self.drive_url}/nodes',
            params={
                'sort': sort,
                'limit': limit,
                'offset': offset,
                'filters': filters,
            }
        )
        initial = r.json()
        # small number of results, no need to paginate
        if initial['count'] <= MAX_LIMIT:
            if not (df := pd.json_normalize(initial['data'])).empty:
                return format_nodes(df)
            logger.info(f'No results found for {filters = }')
            return

        res = [initial]
        # see AWS error: E.g. "Offset + limit cannot be greater than 9999"
        # offset must be 9799 + limit of 200
        if initial['count'] > MAX_NODES:
            offsets = MAX_NODE_OFFSETS
        else:
            offsets = range(offset, min(initial['count'], limit), MAX_LIMIT)
        fns = (partial(self._get_nodes, offset=o, filters=filters, sort=sort, limit=limit) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='Node Query', **kwargs)))
        return format_nodes(
            pd.json_normalize(y for x in res for y in x.get('data', []))
            .drop_duplicates('id')
            .reset_index(drop=True)
        )

    async def _get_nodes(self, client: AsyncClient, sem: asyncio.Semaphore, filters: list[str], sort: list[str], limit: int, offset: int) -> dict:
        """
        Get Amazon Photos nodes

        @param client: an async client instance
        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @return: nodes as a dict
        """

        r = await self.async_backoff(
            client.get,
            sem,
            f'{self.drive_url}/nodes',
            params=self.base_params | {
                'sort': sort,
                'limit': limit,
                'offset': offset,
                'filters': filters,
            },
        )
        return r.json()

    def __at(self):
        r = self.client.get(
            'https://www.amazon.ca/photos/auth/token',
            params={'_': int(time.time_ns() // 1e6)},
        )
        at = r.json()['access_token']
        # todo: does not work, investigate later
        # self.client.cookies.update({'at-acb': at})
        return at
