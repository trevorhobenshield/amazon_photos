import asyncio
import hashlib
import logging.config
import math
import os
import platform
import random
import sys
import time
from functools import partial
from logging import getLogger, Logger
from pathlib import Path
from typing import Generator

import aiofiles
import orjson
import pandas as pd
from httpx import AsyncClient, Client, Response, Limits
from tqdm.asyncio import tqdm_asyncio

from .constants import *
from .helpers import dump

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

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s.%(msecs)03d [%(levelname)s] :: %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'standard',
            'filename': 'log.log',
            'mode': 'a'
        },
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'WARNING',
            'formatter': 'standard'
        }
    },
    'loggers': {
        'my_logger': {
            'handlers': ['file', 'console'],
            'level': 'DEBUG'
        }
    }
})
logger = getLogger(list(Logger.manager.loggerDict)[-1])


class AmazonPhotos:
    def __init__(self, *, tld: str = None, cookies: dict = None):
        self.tld = tld or self.determine_tld(cookies)  # top-level domain
        self.base = f'https://www.amazon.{self.tld}'
        self.client = Client(
            http2=True,
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
        self.root, self.owner_id = self._get_root()

    def determine_tld(self, cookies: dict) -> str:
        """
        Determine top-level domain based on cookies

        @param cookies: cookies dict
        @return: top-level domain
        """
        for k, v in cookies.items():
            if k.startswith(x := 'at-acb'):
                return k.split(x)[-1]

    async def process(self, fns: list | Generator, **kwargs) -> list:
        """
        Process collections of async partials

        @param fns: list or generator containing async partials
        @return: list of results
        """
        client = AsyncClient(
            http2=kwargs.pop('http2', True),
            timeout=kwargs.pop('timeout', 15),
            follow_redirects=kwargs.pop('follow_redirects', True),
            headers=self.client.headers,
            cookies=self.client.cookies,
            limits=Limits(
                max_connections=kwargs.pop('max_connections', 1000),
                max_keepalive_connections=kwargs.pop('max_keepalive_connections', None),
                keepalive_expiry=kwargs.pop('keepalive_expiry', 5.0),
            ),
        )
        async with client as c:
            return await tqdm_asyncio.gather(*(fn(client=c) for fn in fns), desc=kwargs.get('desc'))

    @staticmethod
    async def async_backoff(fn, *args, m: int = 20, b: int = 2, max_retries: int = 12, **kwargs) -> any:
        """Async exponential backoff"""
        for i in range(max_retries + 1):
            try:
                r = await fn(*args, **kwargs)
                if r.status_code == 409:  # conflict
                    logger.debug(f'{r.status_code} {r.text}')
                    return r

                if r.status_code == 401:  # BadAuthenticationData
                    logger.error(f'{r.status_code} {r.text}')
                    logger.error(f'Cookies expired. Log-in to Amazon Photos and copy fresh cookies.')
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

    @staticmethod
    def backoff(fn, *args, m: int = 20, b: int = 2, max_retries: int = 12, **kwargs) -> any:
        """Exponential backoff"""
        for i in range(max_retries + 1):
            try:
                r = fn(*args, **kwargs)

                if r.status_code == 409:  # conflict
                    logger.debug(f'{r.status_code} {r.text}')
                    return r

                if r.status_code == 401:  # BadAuthenticationData
                    logger.error(f'{r.status_code} {r.text}')
                    logger.error(f'Cookies expired. Log-in to Amazon Photos and copy fresh cookies.')
                    sys.exit(1)

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
            f'{self.base}/drive/v1/account/usage',
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
            f'{self.base}/drive/v1/search',
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

    def query(self, filters: str = "type:(PHOTOS OR VIDEOS)", offset: int = 0, limit: int = math.inf, out: str = 'ap.parquet', as_df: bool = True, **kwargs) -> list[dict] | pd.DataFrame:
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
            f'{self.base}/drive/v1/search',
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
        res.extend(asyncio.run(self.process(fns, desc='getting media', **kwargs)))
        return dump(as_df, res, out)

    def photos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all photos"""
        return self.query('type:(PHOTOS)', **kwargs)

    def videos(self, **kwargs) -> list[dict] | pd.DataFrame:
        """Convenience method to get all videos"""
        return self.query('type:(VIDEOS)', **kwargs)

    def upload(self, files: list[Path | str] | Generator, chunk_size=64 * 1024, **kwargs) -> list[dict]:
        async def stream_bytes(file: Path) -> bytes:
            async with aiofiles.open(file, 'rb') as f:
                while chunk := await f.read(chunk_size):
                    yield chunk

        async def upload_file(client: AsyncClient, file: Path):
            logger.debug(f'Upload start: {file.name}')
            file = Path(file) if isinstance(file, str) else file
            file_content = file.read_bytes()  # todo: not ideal, will refactor eventually
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
                    # 'x-amz-access-token':self.client.cookies['at-acbca'],
                    'content-length': str(len(file_content)),
                    'x-amzn-file-md5': hashlib.md5(file_content).hexdigest(),
                }
            )
            data = r.json()
            logger.debug(f'Upload complete: {file.name}')
            logger.debug(f'{data = }')
            return data

        fns = (partial(upload_file, file=file) for file in files)
        return asyncio.run(self.process(fns, desc='Uploading Files', **kwargs))

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
            'ownerId': self.owner_id,
        }

        async def get(client: AsyncClient, node: str) -> None:
            logger.debug(f'Downloading {node}')
            try:
                url = f'{self.base}/drive/v1/nodes/{node}/contentRedirection'
                async with client.stream('GET', url, params=params) as r:
                    content_disposition = dict(
                        [y for x in r.headers['content-disposition'].split('; ') if len((y := x.split('='))) > 1])
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
            f'{self.base}/drive/v1/trash',
            params={
                'sort': "['modifiedDate DESC']",
                'limit': MAX_LIMIT,
                'offset': offset,
                'filters': filters or 'kind:(FILE* OR FOLDER*) AND contentProperties.contentType:(image* OR video*) AND status:(TRASH*)',
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
        res.extend(asyncio.run(self.process(fns, desc='getting trashed media', **kwargs)))
        return dump(as_df, res, out)

    def trash(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Move media to trash bin

        @param node_ids: list of media ids to trash
        @return: the trash response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, ids: list[str]) -> Response:
            return await client.patch(
                f'{self.base}/drive/v1/trash',
                json={
                    'recurse': 'true',
                    'op': 'add',
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
            f'{self.base}/drive/v1/trash',
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
                f'{self.base}/drive/v1/bulk/nodes/purge',
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
                f'{self.base}/drive/v1/search',
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
                [(_out / f'{k}.json').write_bytes(
                    orjson.dumps(data[k], option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)) for k in data]
            return data

        categories = {'allPeople', 'clusterId', 'familyMembers', 'favorite', 'location', 'people', 'things', 'time',
                      'type'}
        if category not in categories:
            raise ValueError(f'category must be one of {categories}')

        r = self.backoff(
            self.client.get,
            f'{self.base}/drive/v1/search/aggregation',
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
            params={'day': 13, 'month': 11, 'includeContents': 'true', 'contentsSize': 1,
                    '_': int(time.time_ns() // 1e6)},
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
            f'{self.base}/drive/v1/nodes',
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

    def favorite(self, node_ids: list[str] | pd.Series, **kwargs) -> list[dict]:
        """
        Add media to favorites

        @param node_ids: media node ids to add to favorites
        @return: operation response
        """

        if isinstance(node_ids, pd.Series):
            node_ids = node_ids.tolist()

        async def patch(client: AsyncClient, node_id: str) -> dict:
            r = await client.patch(f'{self.base}/drive/v1/nodes/{node_id}', json={
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
            r = await client.patch(f'{self.base}/drive/v1/nodes/{node_id}', json={
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

        r = self.client.post(f'{self.base}/drive/v1/nodes', json={
            'kind': 'VISUAL_COLLECTION',
            'name': album_name,
            'resourceVersion': 'V2',
            'ContentType': 'JSON',
        })
        created_album = r.json()
        album_id = created_album['id']
        self.client.patch(
            f'{self.base}/drive/v1/nodes/{album_id}/children',
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
            f'{self.base}/drive/v1/nodes/{album_id}',
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
            f'{self.base}/drive/v1/nodes/{album_id}/children',
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
            f'{self.base}/drive/v1/nodes/{album_id}/children',
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
                f'{self.base}/drive/v1/nodes/{node_id}',
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
                f'{self.base}/drive/v1/nodes/{node_id}',
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
        return self.client.put(f'{self.base}/drive/v1/cluster/name', json={
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

        return self.client.post(f'{self.base}/drive/v1/cluster', json={
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
            f'{self.base}/drive/v1/cluster/hero/{cluster_id}',
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
            f'{self.base}/drive/v1/familyArchive/', json={
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
            f'{self.base}/drive/v1/familyArchive/', json={
                'nodesToAdd': [],
                'nodesToRemove': node_ids,
                'familyId': family_id,
                'resourceVersion': 'V2',
                'ContentType': 'JSON',
            }).json()

    def __nodes_head(self, filters: str = '', offset: int = 0, limit: int = MAX_LIMIT, out: str = 'nodes.json', **kwargs) -> list[dict]:
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
            f'{self.base}/drive/v1/nodes',
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
        res.extend(asyncio.run(self.process(fns, desc='getting media', **kwargs)))
        # save to disk
        _out = Path(out)
        _out.parent.mkdir(parents=True, exist_ok=True)
        _out.write_bytes(orjson.dumps(res))
        return res
