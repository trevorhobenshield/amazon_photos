import asyncio
import hashlib
import logging.config
import math
import os
import platform
import random
import time
from functools import partial
from logging import getLogger, Logger
from pathlib import Path

import aiofiles
import orjson
import pandas as pd
from dotenv import load_dotenv
from httpx import AsyncClient, Client, Response
from tqdm.asyncio import tqdm_asyncio

from .helpers import dump
from .constants import *

load_dotenv()

try:
    if get_ipython().__class__.__name__ == 'ZMQInteractiveShell':
        import nest_asyncio

        nest_asyncio.apply()
except:
    ...

if platform.system() in {'Darwin', 'Linux'}:
    try:
        import uvloop

        uvloop.install()
    except ImportError as e:
        ...

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {'standard': {'format': '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s', 'datefmt': '%Y-%m-%d %H:%M:%S'}},
    'handlers': {
        'file': {'class': 'logging.FileHandler', 'level': 'DEBUG', 'formatter': 'standard', 'filename': 'log.log', 'mode': 'a'},
    },
    'loggers': {'my_logger': {'handlers': ['file'], 'level': 'DEBUG'}}
})
logger_name = list(Logger.manager.loggerDict)[-1]
logger = getLogger(logger_name)


class Photos:
    def __init__(self):
        self.client = Client(
            http2=True,
            follow_redirects=True,
            timeout=60,
            headers={
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                'x-amzn-sessionid': os.getenv('session-id'),
            },
            cookies={
                'session-id': os.getenv('session-id'),
                'ubid-acbca': os.getenv('ubid-acbca'),
                'at-acbca': os.getenv('at-acbca'),  # access_token
            }
        )
        self.root, self.owner_id = self._get_root()

    async def process(self, fns: list, **kwargs) -> list:
        """
        Generic method to process collections of async partials

        @param fns: list or generator containing async partials
        @return: list of results
        """
        client = AsyncClient(
            http2=True,
            timeout=60,
            follow_redirects=True,
            headers=self.client.headers,
            cookies=self.client.cookies
        )
        async with client as c:
            return await tqdm_asyncio.gather(*(fn(client=c) for fn in fns), desc=kwargs.get('desc'))

    @staticmethod
    async def async_backoff(fn, *args, m: int = 20, b: int = 2, max_retries: int = 8, **kwargs) -> any:
        """Async exponential backoff"""
        for i in range(max_retries + 1):
            try:
                r = await fn(*args, **kwargs)
                if r.status_code == 409:
                    logger.debug(f'{r.status_code} {r.text}')
                    return r
                r.raise_for_status()
                return r
            except Exception as e:
                if i == max_retries:
                    logger.warning(f'Max retries exceeded\n{e}')
                    return
                t = min(random.random() * (b ** i), m)
                logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                await asyncio.sleep(t)

    @staticmethod
    def backoff(fn, *args, m: int = 20, b: int = 2, max_retries: int = 8, **kwargs) -> any:
        """Exponential backoff"""
        for i in range(max_retries + 1):
            try:
                r = fn(*args, **kwargs)
                if r.status_code == 409:
                    logger.debug(f'{r.status_code} {r.text}')
                    return r
                r.raise_for_status()
                return r
            except Exception as e:
                if i == max_retries:
                    logger.warning(f'Max retries exceeded\n{e}')
                    return
                t = min(random.random() * (b ** i), m)
                logger.debug(f'Retrying in {f"{t:.2f}"} seconds\t\t{e}')
                time.sleep(t)

    def usage(self, as_df: bool = True) -> dict | pd.DataFrame:
        """
        Get Amazon Photos current memory usage stats

        @param as_df: return as pandas DataFrame
        @return: dict or pd.DataFrame
        """
        r = self.backoff(
            self.client.get,
            "https://www.amazon.ca/drive/v1/account/usage",
            params={"resourceVersion": "V2", "ContentType": "JSON", "_": int(time.time_ns() // 1e6)},
        )
        data = r.json()
        if as_df:
            data.pop('lastCalculated')
            return pd.DataFrame(
                {
                    'type': k,
                    'billable_bytes': v['billable']['bytes'],
                    'billable_count': v['billable']['count'],
                    'total_bytes': v['total']['bytes'],
                    'total_count': v['total']['count'],
                } for k, v in data.items()
            )
        return data

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
            "https://www.amazon.ca/drive/v1/search",
            params={
                "limit": limit,
                "offset": offset,
                "asset": "ALL",
                "filters": filters,
                "lowResThumbnail": "true",
                "searchContext": "customer",
                "sort": "['createdDate DESC']",
                "tempLink": "false",
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        )
        return r.json()

    def query(self, filters: str, offset: int = 0, limit: int = math.inf, out: str = 'media.json', as_df: bool = True) -> list[dict] | pd.DataFrame:
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
            "https://www.amazon.ca/drive/v1/search",
            params={
                "limit": MAX_LIMIT,
                "offset": offset,
                "asset": "ALL",
                "filters": filters,
                "lowResThumbnail": "true",
                "searchContext": "customer",
                "sort": "['createdDate DESC']",
                "tempLink": "false",
                "resourceVersion": "V2",
                "ContentType": "JSON",
            },
        ).json()
        res = [initial]
        # small number of results, no need to paginate
        if initial['count'] <= MAX_LIMIT:
            return dump(as_df, res, out)
        offsets = range(offset, min(initial['count'], limit), MAX_LIMIT)
        fns = (partial(self.q, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='getting media')))
        return dump(as_df, res, out)

    def photos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all photos"""
        return self.query('type:(PHOTOS)', **kwargs)

    def videos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all videos"""
        return self.query('type:(VIDEOS)', **kwargs)

    def upload(self, files: list[Path | str], chunk_size=64 * 1024) -> list[dict]:
        """
        Upload files to Amazon Photos

        @param files: list of files to upload
        @param chunk_size: optional size to chunk files into during stream upload
        @return: the upload response
        """

        async def stream_bytes(file: Path) -> bytes:
            async with aiofiles.open(file, 'rb') as f:
                while chunk := await f.read(chunk_size):
                    yield chunk

        async def upload_file(client: AsyncClient, file: Path):
            logger.debug(f'Upload start: {file.name}')
            file = Path(file) if isinstance(file, str) else file
            file_content = file.read_bytes()
            r = await client.post(
                'https://content-na.drive.amazonaws.com/v2/upload',
                data=stream_bytes(file),
                params={
                    'conflictResolution': 'RENAME',
                    'fileSize': str(len(file_content)),
                    'name': file.name,
                    'parentNodeId': self.root,
                },
                headers={
                    'content-length': str(len(file_content)),
                    'x-amzn-file-md5': hashlib.md5(file_content).hexdigest(),
                }
            )
            data = r.json()
            logger.debug(f'Upload complete: {file.name}')
            logger.debug(f'{data = }')
            return data

        fns = (partial(upload_file, file=file) for file in files)
        return asyncio.run(self.process(fns, desc='Uploading files'))

    def download(self, image_ids: list[str], out: str = 'media') -> None:
        """
        Download files from Amazon Photos

        @param image_ids: list of image ids to download
        @param out: path to save files
        """
        out = Path(out)
        out.mkdir(parents=True, exist_ok=True)
        params = {
            'querySuffix': '?download=true',
            'ownerId': self.owner_id,
        }

        async def get(client: AsyncClient, image_id: str) -> None:
            logger.debug(f'Downloading {image_id}')
            try:
                url = f'https://www.amazon.ca/drive/v1/nodes/{image_id}/contentRedirection'
                r = await self.async_backoff(client.get, url, params=params)
                content_disposition = dict([y for x in r.headers['content-disposition'].split('; ')
                                            if len((y := x.split('='))) > 1])
                fname = content_disposition['filename'].strip('"')
                async with aiofiles.open(out / f"{image_id}_{fname}", 'wb') as fp:
                    await fp.write(r.content)
            except Exception as e:
                logger.debug(f'Download FAILED for {image_id}\t{e}')

        fns = (partial(get, image_id=image_id) for image_id in image_ids)
        asyncio.run(self.process(fns, desc='downloading files'))

    def trashed(self, filters: str = '', offset: int = 0, limit: int = MAX_LIMIT, as_df: bool = True, out: str = 'trashed.json') -> list[dict]:
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
            'https://www.amazon.ca/drive/v1/nodes',
            params={
                'asset': 'ALL',
                'tempLink': 'false',
                'limit': MAX_LIMIT,
                'sort': "['modifiedDate DESC']",
                'filters': filters or 'kind:(FILE* OR FOLDER*) AND contentProperties.contentType:(image* OR video*) AND status:(TRASH*)',
                'lowResThumbnail': 'true',
                'offset': offset,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
                '_': int(time.time_ns() // 1e6)
            },
        ).json()
        res = [initial]
        # small number of results, no need to paginate
        if initial['count'] <= MAX_LIMIT:
            return dump(as_df, res, out)
        # see AWS error: E.g. "Offset + limit cannot be greater than 9999"
        # offset must be 9799 + limit of 200
        if initial['count'] > MAX_NODES:
            offsets = MAX_NODE_OFFSETS
        else:
            offsets = range(offset, min(initial['count'], limit), MAX_LIMIT)
        fns = (partial(self._nodes, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='getting trashed media')))
        return dump(as_df, res, out)

    def trash(self, media_ids: list[str]) -> list[dict]:
        """
        Move media to trash bin

        @param media_ids: list of media ids to trash
        @return: the trash response
        """

        async def patch(client: AsyncClient, ids: list[str]) -> Response:
            return await client.patch(
                'https://www.amazon.ca/drive/v1/trash',
                json={
                    'recurse': 'true',
                    'op': 'add',
                    'conflictResolution': 'RENAME',
                    'value': ids,
                    'resourceVersion': 'V2',
                    'ContentType': 'JSON',
                }
            )

        id_batches = [media_ids[i:i + MAX_TRASH_BATCH] for i in range(0, len(media_ids), MAX_TRASH_BATCH)]
        fns = (partial(patch, ids=ids) for ids in id_batches)
        return asyncio.run(self.process(fns, desc='trashing files'))

    def restore(self, media_ids: list[str]) -> Response:
        """
        Restore media from trash bin

        @param media_ids: list of media ids to restore
        @return: the restore response
        """
        return self.client.patch(
            'https://www.amazon.ca/drive/v1/trash',
            json={
                'recurse': 'true',
                'op': 'remove',
                'conflictResolution': 'RENAME',
                'value': media_ids,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        )

    def delete(self, ids: list[str]) -> Response:
        """
        Permanently delete media from Amazon Photos

        @param ids: list of media ids to delete
        @return: the delete response
        """
        return self.backoff(
            self.client.post,
            'https://www.amazon.ca/drive/v1/bulk/nodes/purge',
            json={
                'recurse': 'false',
                'nodeIds': ids,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }
        )

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
                'https://www.amazon.ca/drive/v1/search',
                params={
                    'asset': 'ALL',
                    'limit': 1,  # don't care about media info, just want aggregations
                    'lowResThumbnail': 'true',
                    'searchContext': 'all',
                    'tempLink': 'false',
                    'groupByForTime': 'year',
                    'resourceVersion': 'V2',
                })
            data = r.json()['aggregations']
            if out:
                _out = Path(out)
                _out.mkdir(parents=True, exist_ok=True)
                [(_out / f'{k}.json').write_bytes(orjson.dumps(data[k], option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)) for k in data]
            return data

        categories = {'allPeople', 'clusterId', 'familyMembers', 'favorite', 'location', 'people', 'things', 'time', 'type'}
        if category not in categories:
            raise ValueError(f'category must be one of {categories}')

        r = self.backoff(
            self.client.get,
            'https://www.amazon.ca/drive/v1/search/aggregation',
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

    def _get_root(self) -> tuple[str, str]:
        """
        Get Amazon Photos root node and owner id

        @return: tuple of root node and owner id
        """
        r = self.backoff(
            self.client.get,
            'https://cdws.us-east-1.amazonaws.com/drive/v2/memories/v1/collections/this_day_filtered',
            params={'day': 1, 'month': 1, 'includeContents': 'true', 'contentsSize': 1},
        )
        data = r.json()
        node = data['collections'][0]['nodes'][0]
        return node['parents'][0], node['ownerId']

    async def _nodes(self, client: AsyncClient, filters: str, offset: int, limit: int = MAX_LIMIT) -> dict:
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
            'https://www.amazon.ca/drive/v1/nodes',
            params={
                'asset': 'ALL',
                'tempLink': 'false',
                'limit': limit,
                'sort': "['modifiedDate DESC']",
                'filters': filters,
                'lowResThumbnail': 'true',
                'offset': offset,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
                '_': int(time.time_ns() // 1e6)
            },
        )
        return r.json()

    def __all_nodes(self, filters: str = '', offset: int = 0, limit: int = MAX_LIMIT, out: str = 'nodes.json') -> list[dict]:
        """
        Get first 9999 Amazon Photos nodes

        **Note**: Amazon restricts API access to first 9999 nodes

        @param filters: filters to apply to node queries
        @param offset: offset to begin querying nodes
        @param limit: max number of results to return per query
        @param out: path to save results
        @return: list of first 9999 nodes as dicts
        """
        initial = self.backoff(
            self.client.get,
            'https://www.amazon.ca/drive/v1/nodes',
            params={
                'limit': MAX_LIMIT,
                'offset': offset,
                'asset': 'ALL',
                'tempLink': 'false',
                'sort': "['modifiedDate DESC']",
                'filters': filters,
                'lowResThumbnail': 'true',
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
                '_': int(time.time_ns() // 1e6)
            },
        ).json()

        res = [initial]

        # small number of results, no need to paginate
        if initial['count'] <= MAX_LIMIT:
            return [initial]

        # see AWS error: E.g. "Offset + limit cannot be greater than 9999"
        # offset must be 9799 + limit of 200
        if initial['count'] > MAX_NODES:
            offsets = MAX_NODE_OFFSETS
        else:
            # offsets = [i for i in range(0, initial['count'], limit)]
            offsets = range(offset, min(initial['count'], limit), MAX_LIMIT)
        fns = (partial(self._nodes, offset=o, filters=filters, limit=MAX_LIMIT) for o in offsets)
        res.extend(asyncio.run(self.process(fns, desc='getting media')))
        # save to disk
        _out = Path(out)
        _out.parent.mkdir(parents=True, exist_ok=True)
        _out.write_bytes(orjson.dumps(res))
        return res
