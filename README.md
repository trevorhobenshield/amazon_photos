# Amazon Photos API

## Table of Contents

- [Installation](#installation)
- [Setup](#setup)
- [Query Syntax](#query-syntax)
- [Examples](#examples)

## Installation

```bash
pip install amazon-photos
```

## Setup

These environment variables must be set. Log in to Amazon Photos and copy the cookies:
- *`ubid-acbxx`
- *`at-acbxx`
- `session-id`

*replace `xx` with your country code

E.g. for amazon.ca, you would add to your `~/.bashrc`:
```bash
export session_id="..."
export ubid_acbca="..."
export at_acbca="..."
```

## Query Syntax

> For valid **location** and **people** IDs, see the results from the `aggregations()` method.

Example query:

#### `drive/v1/search`

```text
type:(PHOTOS OR VIDEOS)
AND things:(plant AND beach OR moon)
AND timeYear:(2019)
AND timeMonth:(7)
AND timeDay:(1)
AND location:(CAN#BC#Vancouver)
AND people:(CyChdySYdfj7DHsjdSHdy)
```

#### `/drive/v1/nodes`

```
kind:(FILE* OR FOLDER*)
AND contentProperties.contentType:(image* OR video*)
AND status:(AVAILABLE*)
AND settings.hidden:false
AND favorite:(true)
```

## Examples

```python
from pathlib import Path
from amazon_photos import AmazonPhotos

# e.g. using amazon.ca
ap = AmazonPhotos(tld="ca")

# get entire Amazon Photos library. (default save to `ap.parquet`)
nodes = ap.query("type:(PHOTOS OR VIDEOS)")

# query Amazon Photos library with more filters applied. (default save to `ap.parquet`)
nodes = ap.query("type:(PHOTOS OR VIDEOS) AND things:(plant AND beach OR moon) AND timeYear:(2023) AND timeMonth:(8) AND timeDay:(14) AND location:(CAN#BC#Vancouver)")

# sample first 10 nodes
node_ids = nodes.id[:10]

# move a batch of images/videos to the trash bin
ap.trash(node_ids)

# restore a batch of images/videos from the trash bin
ap.restore(node_ids)

# upload a batch of images/videos
files = Path('path/to/files').iterdir()
ap.upload(files)

# download a batch of images/videos
ap.download(node_ids)

# permanently delete a batch of images/videos.
ap.delete(node_ids)

# convenience method to get all photos
ap.photos()

# convenience method to get all videos
ap.videos()

# get current usage stats
ap.usage()

# get all identifiers calculated by Amazon.
ap.aggregations(category="all")

# get specific identifiers calculated by Amazon.
ap.aggregations(category="location")

# get trash bin contents
ap.trashed()
```

## Common Paramters

| name            | type | description                                                                                                                                                                                                                                               |
|:----------------|:-----|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ContentType     | str  | `"JSON"`                                                                                                                                                                                                                                                  |
| _               | int  | `1690059771064`                                                                                                                                                                                                                                           |
| asset           | str  | `"ALL"`<br/>`"MOBILE"`<br/>`"NONE`<br/>`"DESKTOP"`<br/><br/>default: `"ALL"`                                                                                                                                                                              |
| filters         | str  | `"type:(PHOTOS OR VIDEOS) AND things:(plant AND beach OR moon) AND timeYear:(2019) AND timeMonth:(7) AND location:(CAN#BC#Vancouver) AND people:(CyChdySYdfj7DHsjdSHdy)"`<br/><br/>default: `"type:(PHOTOS OR VIDEOS)"`                                   |
| groupByForTime  | str  | `"day"`<br/>`"month"`<br/>`"year"`                                                                                                                                                                                                                        |
| limit           | int  | `200`                                                                                                                                                                                                                                                     |
| lowResThumbnail | str  | `"true"`<br/>`"false"`<br/><br/>default: `"true"`                                                                                                                                                                                                         |
| resourceVersion | str  | `"V2"`                                                                                                                                                                                                                                                    |
| searchContext   | str  | `"customer"`<br/>`"all"`<br/>`"unknown"`<br/>`"family"`<br/>`"groups"`<br/><br/>default: `"customer"`                                                                                                                                                     |
| sort            | str  | `"['contentProperties.contentDate DESC']"`<br/>`"['contentProperties.contentDate ASC']"`<br/>`"['createdDate DESC']"`<br/>`"['createdDate ASC']"`<br/>`"['name DESC']"`<br/>`"['name ASC']"`<br/><br/>default: `"['contentProperties.contentDate DESC']"` |
| tempLink        | str  | `"false"`<br/>`"true"`<br/><br/>default: `"false"`                                                                                                                                                                                                        |             |

## Notes

#### `https://www.amazon.ca/drive/v1/batchLink`

- This endpoint is called when downloading a batch of photos/videos in the web interface. It then returns a URL to
  download a zip file, then makes a request to that url to download the content.
  When making a request to download data for 1200 nodes (max batch size), it turns out to be much slower (~2.5 minutes)
  than asynchronously downloading 1200 photos/videos individually (~1 minute).
