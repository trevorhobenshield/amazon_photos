from pathlib import Path

import orjson
import pandas as pd


def dump(cond: bool, res: list[dict], out: str):
    if out:
        if cond:
            return parse_media(res, out=out)
        _out = Path(out)
        _out.parent.mkdir(parents=True, exist_ok=True)
        _out.write_bytes(orjson.dumps(res))
    return res


def format_nodes(df: pd.DataFrame) -> pd.DataFrame:
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
    return (
        df
        .sort_values('modifiedDate', ascending=False)
        .reset_index(drop=True)
    )


def parse_media(media: any, out: str) -> pd.DataFrame:
    df = pd.json_normalize(y for x in media for y in x['data']).rename(
        {'contentProperties.version': 'contentPropertiesVersion'}, axis=1
    )
    if 'assets' in df.columns:
        df.drop(columns=['assets'], inplace=True)  # todo: parquet issue
    df.columns = df.columns.str.replace('contentProperties.', '')  # clean up col names
    df = format_nodes(df)
    suffix = Path(out).suffix
    if suffix == '.parquet':
        df.to_parquet(out)
    elif suffix == '.json':
        df.to_json(out, orient='records')
    elif suffix == '.csv':
        df.to_csv(out, index=False)
    elif suffix == '.xlsx':
        df.to_excel(out, index=False)
    return df
