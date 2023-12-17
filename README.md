# Amazon Photos API

## Table of Contents

<!-- TOC -->

* [Installation](#installation)
* [Setup](#setup)
* [Examples](#examples)
* [Search](#search)
* [Nodes](#nodes)
    * [Restrictions](#restrictions)
    * [Range Queries](#range-queries)
* [Notes](#notes)
    * [Known File Types](#known-file-types)

<!-- TOC -->

> It is recommended to use this API in a [Jupyter Notebook](https://jupyter.org/install), as the results from most
> endpoints
> are a [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)
> which can be neatly displayed and efficiently manipulated with vectorized ops. This becomes
> increasingly important if you have "large" amounts of data (e.g. >1 million photos/videos).

### Output Examples

`ap.db`

|      | createdDate                | modifiedDate               | image.dateTime      | name      | image.width | image.height |
|------|----------------------------|----------------------------|---------------------|-----------|-------------|--------------|
| 6360 | 2023-12-13 21:16:16.879000 | 2023-12-13 21:16:20.553000 | 2020-08-08 20:49:37 | test1.jpg | 3024.0      | 4032.0       |
| 6361 | 2023-12-13 21:16:13.481000 | 2023-12-13 21:16:20.543000 | 2020-06-10 13:34:11 | test2.jpg | 2730.0      | 4096.0       |
| 6430 | 2023-12-13 21:16:12.090000 | 2023-12-13 21:16:17.280000 | \<NA>               | test3.jpg | 3024.0      | 4032.0       |
| 6484 | 2023-12-13 21:16:08.306000 | 2023-12-13 21:16:14.276000 | \<NA>               | test4.jpg | 2304.0      | 3072.0       |
| 6530 | 2023-12-13 21:16:07.487000 | 2023-12-13 21:16:12.030000 | 2020-07-15 13:35:54 | test5.jpg | 3024.0      | 4032.0       |

`ap.print_tree()`

```text
~ 
├── Documents 
├── Pictures 
│   ├── iPhone 
│   └── Web 
│       ├── foo 
│       └── bar
├── Videos 
└── Backup 
    ├── LAPTOP-XYZ 
    │   └── Desktop 
    └── DESKTOP-IJK 
        └── Desktop
```

## Installation

```bash
pip install amazon-photos
```

## Setup

There are two ways access protected endpoints. The first is to pass cookies explicitly to the `AmazonPhotos`
constructor, the second is to add cookies as environment variables.

Log in to Amazon Photos and copy the cookies:

- *`ubid-acbxx`
- *`at-acbxx`
- `session-id`

*where `xx` is your country code

### Option 1: Cookies Dict

```python
from amazon_photos import AmazonPhotos

ap = AmazonPhotos(
    cookies={
        'ubid-acbca': ...,
        'at-acbca': ...,
        'session-id': ...,
    }
)
```

### Option 2: Environment Variables

E.g. for amazon.**ca** (Canada), you would add to your `~/.bashrc`:

```bash
export session_id="..."
export ubid_acbca="..."
export at_acbca="..."
```

## Examples

> A database named `ap.parquet` will be created during the initial setup. This is mainly used to reduce upload conflicts
> by checking your local file(s) md5 against the database before sending the request.

```python
from amazon_photos import AmazonPhotos

## e.g. using env variables and specifying tld. E.g. amazon.ca (Canada)
# ap = AmazonPhotos(tld="ca")

## e.g. using cookies dict
ap = AmazonPhotos(
    cookies={
        'ubid-acbca': ...,
        'at-acbca': ...,
        'session-id': ...,
    },
    # pandas options
    dtype_backend='pyarrow',
    engine='pyarrow',
)

# get current usage stats
ap.usage()

# get entire Amazon Photos library. (default save to `ap.parquet`)
nodes = ap.query("type:(PHOTOS OR VIDEOS)")

# query Amazon Photos library with more filters applied. (default save to `ap.parquet`)
nodes = ap.query("type:(PHOTOS OR VIDEOS) AND things:(plant AND beach OR moon) AND timeYear:(2023) AND timeMonth:(8) AND timeDay:(14) AND location:(CAN#BC#Vancouver)")

# sample first 10 nodes
node_ids = nodes.id[:10]

# move a batch of images/videos to the trash bin
ap.trash(node_ids)

# get trash bin contents
ap.trashed()

# permanently delete a batch of images/videos.
ap.delete(node_ids)

# restore a batch of images/videos from the trash bin
ap.restore(node_ids)

# upload media (preserves local directory structure and copies to Amazon Photos root directory)
ap.upload('path/to/files')

# download a batch of images/videos
ap.download(node_ids)

# convenience method to get photos only
ap.photos()

# convenience method to get videos only
ap.videos()

# get all identifiers calculated by Amazon.
ap.aggregations(category="all")

# get specific identifiers calculated by Amazon.
ap.aggregations(category="location")
```

## Search

*Undocumented API, current endpoints valid Dec 2023.*

For valid **location** and **people** IDs, see the results from the `aggregations()` method.

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

## Nodes

*Docs last updated in 2015*

| FieldName                     | FieldType                | Sort Allowed | Notes                                                                                                                                                                                                                                       |
|-------------------------------|--------------------------|--------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| isRoot                        | Boolean                  |              | Only lower case `"true"` is supported.                                                                                                                                                                                                      |
| name                          | String                   | Yes          | This field does an exact match on the name and prefix query. Consider `node1{ "name" : "sample" }` `node2 { "name" : "sample1" }` Query filter<br>`name:sample` will return node1<br>`name:sample*` will return node1 and node2             |
| kind                          | String                   | Yes          | To search for all the nodes which contains kind as FILE `kind:FILE`                                                                                                                                                                         |
| modifiedDate                  | Date (in ISO8601 Format) | Yes          | To Search for all the nodes which has modified from time `modifiedDate:{"2014-12-31T23:59:59.000Z" TO *]`                                                                                                                                   |
| createdDate                   | Date (in ISO8601 Format) | Yes          | To Search for all the nodes created on  `createdDate:2014-12-31T23:59:59.000Z`                                                                                                                                                              |
| labels                        | String Array             |              | Only Equality can be tested with arrays.<br>if labels contains `["name", "test", "sample"]`.<br>Label can be searched for name or combination of values.<br>To get all the labels which contain name and test<br>`labels: (name AND test)`  |
| description                   | String                   |              | To Search all the nodes for description with value 'test'<br>`description:test`                                                                                                                                                             |
| parents                       | String Array             |              | Only Equality can be tested with arrays.<br>if parents contains `["id1", "id2", "id3"]`.<br>Parent can be searched for name or combination of values.<br>To get all the parents which contains id1 and id2<br>`parents:id1 AND parents:id2` |
| status                        | String                   | Yes          | For searching nodes with AVAILABLE status.<br>`status:AVAILABLE`                                                                                                                                                                            |
| contentProperties.size        | Long                     | Yes          |                                                                                                                                                                                                                                             |
| contentProperties.contentType | String                   | Yes          | If prefix query, only the major content-type (e.g. `image*`, `video*`, etc.) is supported as a prefix.                                                                                                                                      |
| contentProperties.md5         | String                   |              |                                                                                                                                                                                                                                             |
| contentProperties.contentDate | Date (in ISO8601 Format) | Yes          | RangeQueries and equals queries can be used with this field                                                                                                                                                                                 |
| contentProperties.extension   | String                   | Yes          |                                                                                                                                                                                                                                             |

### Restrictions

> Max # of Filter Parameters Allowed is 8

| Filter Type | Filters                                                                               |
|:------------|:--------------------------------------------------------------------------------------|
| Equality    | createdDate, description, isRoot, kind, labels, modifiedDate, name, parentIds, status |
| Range       | contentProperties.contentDate, createdDate, modifiedDate                              |
| Prefix      | contentProperties.contentType, name                                                   |

### Range Queries

| Operation            | Syntax                                                           |
|----------------------|------------------------------------------------------------------|
| GreaterThan          | `{"valueToBeTested" TO *}`                                       |
| GreaterThan or Equal | `["ValueToBeTested" TO *]`                                       |
| LessThan             | `{* TO "ValueToBeTested"}`                                       |
| LessThan or Equal    | `{* TO "ValueToBeTested"]`                                       |
| Between              | `["ValueToBeTested_LowerBound" TO "ValueToBeTested_UpperBound"]` |

## Notes

#### `https://www.amazon.ca/drive/v1/batchLink`

- This endpoint is called when downloading a batch of photos/videos in the web interface. It then returns a URL to
  download a zip file, then makes a request to that url to download the content.
  When making a request to download data for 1200 nodes (max batch size), it turns out to be much slower (~2.5 minutes)
  than asynchronously downloading 1200 photos/videos individually (~1 minute).

### Known File Types

| Extension | Category |
|-----------|----------|
| \.pdf     | pdf      |
| \.doc     | doc      |
| \.docx    | doc      |
| \.docm    | doc      |
| \.dot     | doc      |
| \.dotx    | doc      |
| \.dotm    | doc      |
| \.asd     | doc      |
| \.cnv     | doc      |
| \.mp3     | mp3      |
| \.m4a     | mp3      |
| \.m4b     | mp3      |
| \.m4p     | mp3      |
| \.wav     | mp3      |
| \.aac     | mp3      |
| \.aif     | mp3      |
| \.mpa     | mp3      |
| \.wma     | mp3      |
| \.flac    | mp3      |
| \.mid     | mp3      |
| \.ogg     | mp3      |
| \.xls     | xls      |
| \.xlm     | xls      |
| \.xll     | xls      |
| \.xlc     | xls      |
| \.xar     | xls      |
| \.xla     | xls      |
| \.xlb     | xls      |
| \.xlsb    | xls      |
| \.xlsm    | xls      |
| \.xlsx    | xls      |
| \.xlt     | xls      |
| \.xltm    | xls      |
| \.xltx    | xls      |
| \.xlw     | xls      |
| \.ppt     | ppt      |
| \.pptx    | ppt      |
| \.ppa     | ppt      |
| \.ppam    | ppt      |
| \.pptm    | ppt      |
| \.pps     | ppt      |
| \.ppsm    | ppt      |
| \.ppsx    | ppt      |
| \.pot     | ppt      |
| \.potm    | ppt      |
| \.potx    | ppt      |
| \.sldm    | ppt      |
| \.sldx    | ppt      |
| \.txt     | txt      |
| \.text    | txt      |
| \.rtf     | txt      |
| \.xml     | markup   |
| \.htm     | markup   |
| \.html    | markup   |
| \.zip     | zip      |
| \.rar     | zip      |
| \.7z      | zip      |
| \.jpg     | img      |
| \.jpeg    | img      |
| \.png     | img      |
| \.bmp     | img      |
| \.gif     | img      |
| \.tif     | img      |
| \.svg     | img      |
| \.mp4     | vid      |
| \.m4v     | vid      |
| \.qt      | vid      |
| \.mov     | vid      |
| \.mpg     | vid      |
| \.mpeg    | vid      |
| \.3g2     | vid      |
| \.3gp     | vid      |
| \.flv     | vid      |
| \.f4v     | vid      |
| \.asf     | vid      |
| \.avi     | vid      |
| \.wmv     | vid      |
| \.swf     | exe      |
| \.exe     | exe      |
| \.dll     | exe      |
| \.ax      | exe      |
| \.ocx     | exe      |
| \.rpm     | exe      |
