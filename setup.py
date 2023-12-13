from textwrap import dedent

from setuptools import find_packages, setup
from pathlib import Path

install_requires = [
    'nest-asyncio',
    'tqdm',
    'uvloop; platform_system != "Windows"',
    'aiofiles',
    'orjson',
    'httpx[http2]',
    'pandas',
]

about = {}
exec((Path().cwd() / 'amazon_photos' / '__version__.py').read_text(), about)

setup(
    name=about['__title__'],
    version=about['__version__'],
    author=about['__author__'],
    description=about['__description__'],
    license=about['__license__'],
    long_description=dedent('''
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
    
    > It is recommended to use this API in a [Jupyter Notebook](https://jupyter.org/install), as the results from most endpoints
    > are a [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)
    > which can be neatly displayed in a notebook, and efficiently manipulated with vectorized operations. This becomes
    > increasingly important when dealing with large quantities of data.
    
    ## Setup
    
    There are two ways to set up authentication. The first is to pass the cookies explicitly to the `AmazonPhotos`
    constructor, the second is to add your cookies as environment variables.
    
    Log in to Amazon Photos and copy the cookies:
    
    - *`ubid-acbxx`
    - *`at-acbxx`
    - `session-id`
    
    *Replace `xx` with your country code
    
    ### Option 1: Cookies Dict
    
    ```python
    from amazon_photos import AmazonPhotos
    
    ap = AmazonPhotos(
        cookies={
            "session-id": ...,
            "ubid-acbca": ...,
            "at-acbca": ...,
        },
        db_path="ap.parquet",  # initialize a simple database to store results
    )
    
    # sanity check, verify authenticated endpoint can be reached
    ap.usage()
    ```
    
    ### Option 2: Environment Variables
    
    E.g. for amazon.**ca** (Canada), you would add to your `~/.bashrc`:
    
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
    from amazon_photos import AmazonPhotos
    
    ## e.g. using cookies dict
    ap = AmazonPhotos(cookies={
        "at-acbca": ...,
        "ubid-acbca": ...,
        "session-id": ...,
    })
    
    ## e.g. using env variables and specifying tld. E.g. amazon.ca (Canada)
    # ap = AmazonPhotos(tld="ca")
    
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
    '''),
    python_requires=">=3.10.10",
    long_description_content_type='text/markdown',
    author_email='trevorhobenshield@gmail.com',
    url='https://github.com/trevorhobenshield/amazon_photos',
    install_requires=install_requires,
    keywords='amazon photos api async search automation',
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
