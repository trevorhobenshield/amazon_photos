from pathlib import Path
from typing import Generator

import pandas as pd


def folder_relmap(root: str, files: Generator | list, folder_map: dict) -> list[tuple[str, str]]:
    res = []
    for p in files:
        if root in p.parts:
            idx = p.parts.index(root)
            rel = Path(*p.parts[idx:])
        else:
            rel = p
        fid = folder_map[str(rel.parent)]
        res.append((fid, p))
    return res


def format_nodes(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    # .parquet format does not support cols with empty dicts, drop non-essential cols
    df.drop(columns=['xAccntParentMap', 'assets', 'contentProperties.version'], inplace=True, errors='ignore')
    # clean up col names and order important cols for readability
    df.columns = df.columns.str.replace('contentProperties.', '')
    cols = [
        'createdDate',
        'modifiedDate',
        'id',
        'parents',
        'image.dateTime',
        'name',
        'image.width',
        'image.height',
        'video.width',
        'video.height',
        'size',
        'md5',
        'ownerId',
        'contentType',
        'extension',
    ]
    date_cols = {
        'contentDate',
        'createdDate',
        'creationDate',
        'dateTime',
        'dateTimeDigitized',
        'dateTimeOriginal',
        'modifiedDate',
        'ProcessingTimestamp',
        'VideoMetadataTimestamps',
        'VideoThumbnailTimestamps',
        'VideoTranscodeTimestamps',
    }
    date_cols |= {f'{y}.{x}' for x in date_cols for y in ('img', 'video')}
    valid_date_cols = list(date_cols & set(df.columns))
    df[valid_date_cols] = df[valid_date_cols].apply(pd.to_datetime, format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')
    valid_cols = []  # maintain cols order for readability
    [valid_cols.append(c) for c in cols if c in df.columns]
    df = df[valid_cols + list(set(df.columns) - set(cols))]
    try:
        return (
            df
            .sort_values('modifiedDate', ascending=False)
            .reset_index(drop=True)
        )
    except:
        ...
    return df
