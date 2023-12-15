import asyncio
import logging.config
import math
import os
import platform
import random
import sys
import time
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
from httpx import AsyncClient, Client, Response, Limits
from tqdm.asyncio import tqdm

from .constants import *
from .helpers import format_nodes

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
    def __init__(self, *, tld: str = None, cookies: dict = None, tmp: str = '', **kwargs):
        self.tld = tld or self.determine_tld(cookies)
        self.drive_url = f'https://www.amazon.{self.tld}/drive/v1'
        self.cdn_url = 'https://content-na.drive.amazonaws.com/cdproxy/nodes'  # variant? 'https://content-na.drive.amazonaws.com/cdproxy/v1/nodes'
        self.base_params = {
            'asset': 'ALL',
            'tempLink': 'false',
            'resourceVersion': 'V2',
            'ContentType': 'JSON',
        }
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
        self.use_cache = kwargs.pop('use_cache', False)
        self.db_path = Path(kwargs.pop('db_path', 'ap.parquet'))
        self.cache_path = Path(kwargs.pop('cache_path', ''))
        self.cache = self.load_cache()
        self.root = self.get_root()
        self.folders = self.get_folders()
        self.db = self.load_db(**kwargs)

    def determine_tld(self, cookies: dict) -> str:
        """
        Determine top-level domain based on cookies

        @param cookies: cookies dict
        @return: top-level domain
        """
        for k, v in cookies.items():
            if k.startswith(x := 'at-acb'):
                return k.split(x)[-1]

    async def process(self, partials: Generator, batch_size: int = 2000, **kwargs):
        """
        Process async partials in batches

        @param batch_size: number of partials to process per batch
        @param kwargs: optional kwargs to pass to AsyncClient
        @return: results from partials
        """
        desc = kwargs.pop('desc', None)  # tqdm
        limits = {
            'max_connections': kwargs.pop('max_connections', batch_size),
            'max_keepalive_connections': kwargs.pop('max_keepalive_connections', None),
            'keepalive_expiry': kwargs.pop('keepalive_expiry', 5.0),
        }
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
        async with AsyncClient(limits=Limits(**limits), headers=headers, cookies=cookies, **defaults, **kwargs) as client:
            queue = asyncio.Queue()
            results = []
            total = 0

            async def worker(pbar):
                while True:
                    task_fn, done = await queue.get()
                    try:
                        res = await task_fn(client=client)
                        done.append(res)
                    except Exception as e:
                        logger.error(f'Task failed: {e}')
                    queue.task_done()
                    pbar.update(1)

            with tqdm(total=total, desc=desc) as pbar:
                workers = [asyncio.create_task(worker(pbar)) for _ in range(batch_size)]
                for p in partials:
                    done = []
                    await queue.put((p, done))
                    results.append(done)
                    total += 1
                    pbar.total = total
                await queue.join()
                for w in workers:
                    w.cancel()
            return [y for x in results for y in x]

    async def async_backoff(self, fn, *args, m: int = 20, b: int = 2, max_retries: int = 12, **kwargs) -> any:
        """Async truncated exponential backoff"""
        for i in range(max_retries + 1):
            try:
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

    async def q(self, client: AsyncClient, filters: str, offset: int, limit: int = MAX_LIMIT) -> dict:
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

    def query(self, filters: str = "type:(PHOTOS OR VIDEOS)", offset: int = 0, limit: int = math.inf, out: str = 'ap.parquet', **kwargs) -> list[dict] | pd.DataFrame:
        """
        Search all media in Amazon Photos

        @param filters: query Amazon Photos database. See query language syntax in readme.
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @param out: path to save results
        @param as_df: return as DataFrame
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
        res.extend(asyncio.run(self.process(fns, desc='getting media', **kwargs)))
        # return dump(as_df, res, out)
        return format_nodes(pd.DataFrame(y for x in res for y in x.get('data', [])))

    def photos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all photos"""
        return self.query('type:(PHOTOS)', **kwargs)

    def videos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all videos"""
        return self.query('type:(VIDEOS)', **kwargs)

    def copy_tree(self, path):
        """
        todo: room for improvement, can find some parallel graph traversal/batching solutions.
        """
        dmap = {}

        def helper(d: str | Path):
            logger.debug(f'Creating Folder: {d}')
            d = str(d)
            if not self.find_folder(d):
                folder_id, folder = self.create_folder(d)
                dmap[d] = folder_id
            else:
                folder = self.find_folder(d)
                dmap[d] = folder['id']

        def copy_dir(src: str | Path):
            helper(src)
            for p in Path(src).iterdir():
                if p.is_dir():
                    copy_dir(p)

        copy_dir(path)
        return dmap

    # def dedup_files(self, path: str | Path) -> list[Path]:
    #     db_md5s = set(self.db.md5)
    #     async def _hash(path):
    #         async with aiofiles.open(path, 'rb') as file:
    #             data = await file.read()
    #         return md5(data).hexdigest()
    #
    #     async def dedup(path):
    #         tasks = []
    #         for p in Path(path).rglob('*'):
    #             if p.is_file():
    #                 tasks.append((p, asyncio.create_task(_hash(p))))
    #         unq, dups = [], []
    #         for p, task in tasks:
    #             res = await task
    #             if res not in db_md5s:
    #                 unq.append(p)
    #             else:
    #                 dups.append(p)
    #         logger.debug(f'{len(unq)} Unique files found')
    #         logger.warning(f'{len(dups)} Duplicate files skipped')
    #         return unq
    #
    #     return asyncio.run(dedup(path))

    def dedup_files(self, folder_path: str):
        files = []
        dups = []
        if self.db is not None:
            md5s = set(self.db.md5)
            for file in Path(folder_path).rglob('*'):
                if file.is_file():
                    if md5(file.read_bytes()).hexdigest() not in md5s:
                        files.append(file)
                    else:
                        dups.append(file)
        else:
            logger.warning('No database found. Checks for duplicate files will not be performed.')
            files = list(Path(folder_path).rglob('*'))
        logger.debug(f'{len(dups)} Duplicate files skipped')
        return files

    def upload(self, folder_path: str, chunk_size=64 * 1024, refresh: bool = True, **kwargs) -> list[dict]:

        async def stream_bytes(file: Path) -> bytes:
            async with aiofiles.open(file, 'rb') as f:
                while chunk := await f.read(chunk_size):
                    yield chunk

        async def post(client: AsyncClient, pid: str, file: Path, max_retries: int = 12, m: int = 20, b: int = 2):
            print('pid =', pid)
            for i in range(max_retries + 1):
                try:
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

        files = self.dedup_files(folder_path)
        dmap = self.copy_tree(folder_path)
        node_map = [(file, dmap[str(file.parent)]) for file in files]

        fns = (partial(post, pid=pid, file=file) for file, pid in node_map)
        res = asyncio.run(self.process(fns, desc='Uploading Files', **kwargs))
        if refresh:
            self.refresh_db()
        return res

    def download(self, node_ids: list[str] | pd.Series, out: str = 'media', chunk_size: int = None, **kwargs) -> dict:
        """
        Download files from Amazon Photos

        Alternatives URLs from legacy documentation (2015), not much difference in speed.
        https://developer.amazon.com/docs/amazon-drive/ad-restful-api-nodes.html#upload-file

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

        async def get(client: AsyncClient, node: str) -> None:
            logger.debug(f'Downloading {node}')
            try:
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
        Get trashed media. Essentially a view your trash bin in Amazon Photos.

        **Note**: Amazon restricts API access to first 9999 nodes

        @param filters: filters to apply to query
        @param offset: offset to begin query
        @param limit: max number of results to return per query
        @param as_df: return as pandas DataFrame
        @param out: path to save results
        @return: trashed media as a dict or DataFrame
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
        fns = (partial(self._nodes, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='getting trashed media', **kwargs)))
        # return dump(as_df, res, out)
        return format_nodes(pd.DataFrame(y for x in res for y in x.get('data', [])))

    def trash(self, node_ids: list[str] | pd.Series, filters: str = '', **kwargs) -> list[dict]:
        """
        Move media or entire folders to trash bin

        @param node_ids: list of media ids to trash
        @return: the trash response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, ids: list[str]) -> Response:
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
        return asyncio.run(self.process(fns, desc='trashing files', **kwargs))

    def restore(self, node_ids: list[str] | pd.Series) -> Response:
        """
        Restore media from trash bin

        @param node_ids: list of media ids to restore
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
        Permanently delete media from Amazon Photos

        @param node_ids: list of media ids to delete
        @return: the delete response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def post(client: AsyncClient, ids: list[str]) -> Response:
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
        return asyncio.run(self.process(fns, desc='trashing files', **kwargs))

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
                    'limit': 1,  # don't care about media info, just want aggregations
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

        async def patch(client: AsyncClient, node_id: str) -> dict:
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

        async def patch(client: AsyncClient, node_id: str) -> dict:
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

        async def patch(client: AsyncClient, node_id: str) -> dict:
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

        async def patch(client: AsyncClient, node_id: str) -> dict:
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

    def update_cache(self, **kwargs):
        if self.cache_path.name:
            try:
                self.cache |= kwargs
                self.cache_path.write_bytes(orjson.dumps(self.cache))  # save to cache
            except Exception as e:
                logger.warning(f'Failed to update cache\t{kwargs = }\t{e}')
        # cache not set, skip

    def get_cache(self, key: str):
        if self.use_cache:
            if self.cache and (x := self.cache.get(key)):
                logger.info(f'Using cached {key} from `{self.cache_path}`')
                return x

    def get_root(self) -> dict:
        """
        isRoot:true filter is sketchy, can add extra data and request is still valid.
        seems like only check is something like: `if(data.contains("true"))`
        """
        if x := self.get_cache('root'):
            return x

        r = self.backoff(self.client.get, f'{self.drive_url}/nodes', params={'filters': 'isRoot:true'} | self.base_params)
        root = r.json()['data'][0]
        logger.debug(f'Got root node: {root}')

        self.update_cache(root=root)
        return root

    def get_folders(self, folder_id: str = None) -> list[dict]:

        def _get(id: str) -> list[dict]:
            url = f'{self.drive_url}/nodes/{id}/children'
            params = {'filters': 'kind:FOLDER'} | self.base_params
            r = self.backoff(self.client.get, url, params=params)
            return r.json()['data']

        def helper(folder: dict) -> dict:
            logger.debug(f'Scanning folder `{folder["name"]}` ({folder["id"]})')
            data = folder | {'children': []}
            folders = _get(folder['id'])
            for sub in folders:
                data['children'].append(helper(sub))
            return data

        if x := self.get_cache('folders'):
            return x
        folders = [helper(node) for node in _get(folder_id or self.root['id'])]
        self.update_cache(folders=folders)
        return folders

    def find_folder(self, path: str):
        def helper(curr: list[dict], remaining: list[str]) -> dict | list[dict]:
            for folder in curr:
                if folder['name'] == remaining[0]:
                    if len(remaining) == 1:
                        return folder['children'] if path.endswith('/') else folder
                    return helper(folder['children'], remaining[1:])

        return helper(self.folders, path.strip('/').split('/'))

    def create_folder(self, path: str, debug: int = 0):
        def _mkdir(parent_id: str, name: str):
            r = self.backoff(
                self.client.post,
                f'{self.drive_url}/nodes',
                json={"kind": "FOLDER", "name": name, "parents": [parent_id], "resourceVersion": "V2", "ContentType": "JSON"},
            )
            if debug:
                logger.debug(f"created new folder {name = } under {parent_id = }")
            # takes a moment for amazon to process folder creation,
            # we keep going, if server returns 409
            # we get the node id in the error message anyways, so we use that
            return r.json().get('id') or r.json().get('info', {}).get('nodeId')

        tmp = ''
        parent = None
        parts = []
        # look for existing folder structure
        for p in filter(len, path.split('/')):
            tmp = os.path.join(tmp, p)
            if folder := self.find_folder(tmp):
                parent = folder.get('id')
            else:
                parts.append(p)

        parent = parent or self.root['id']
        for part in parts:
            try:
                parent = _mkdir(parent, part)
            except Exception as e:
                logger.error(f'Folder creation failed: {part}\t{e}')
                # only retry once, no fancy retry logic
                parent = _mkdir(parent, part)

        # todo: doesn't immediately register folder? time needed for server processing?
        self.folders = self.get_folders()
        folder = self.find_folder(path)
        return parent, folder

    def __at(self):
        r = self.client.get(
            'https://www.amazon.ca/photos/auth/token',
            params={'_': int(time.time_ns() // 1e6)},
        )
        at = r.json()['access_token']
        # todo: does not work, investigate later
        # self.client.cookies.update({'at-acb': at})
        return at

    def refresh_db(self) -> pd.DataFrame:
        """
        todo: may be able to use finegrained node Range Query instead? although results may be limited to 9999 most recent nodes?
        """
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

    def load_cache(self) -> dict:
        if self.cache_path.name:  # ''
            if self.cache_path.exists():
                logger.info(f'Loading cache: {self.cache_path}')
                return orjson.loads(self.cache_path.read_bytes())
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text('{}')
        return {}

    def load_db(self, **kwargs):
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

    async def _nodes(self, client: AsyncClient, filters: list[str], sort: list[str], limit: int, offset: int) -> dict:
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
            f'{self.drive_url}/nodes',
            params=self.base_params | {
                'sort': sort,
                'limit': limit,
                'offset': offset,
                'filters': filters,
            },
        )
        return r.json()

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
        fns = (partial(self._nodes, offset=o, filters=filters, sort=sort, limit=limit) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='Node Query', **kwargs)))
        return format_nodes(
            pd.json_normalize(y for x in res for y in x.get('data', []))
            .drop_duplicates('id')
            .reset_index(drop=True)
        )
