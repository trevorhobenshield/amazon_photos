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
* [Custom Image Labeling (Optional)](#custom-image-labeling-optional)

<!-- TOC -->

> It is recommended to use this API in a [Jupyter Notebook](https://jupyter.org/install), as the results from most
> endpoints
> are a [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html#pandas.DataFrame)
> which can be neatly displayed and efficiently manipulated with vectorized ops. This becomes
> increasingly important if you have "large" amounts of data (e.g. >1 million photos/videos).

## Installation

```bash
pip install amazon-photos -U
```

## Web Application for Photo Management

This project also includes a web-based interface for managing your Amazon Photos, allowing you to download your photos and view them in a gallery.

### Setup for Web App

1.  **Clone the Repository:**
    If you haven't already, clone this repository to your local machine.
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Install Dependencies:**
    Ensure you have Python installed. Then, install the necessary dependencies using the `requirements.txt` file:
    ```bash
    pip install -r requirements.txt
    ```
    This will install `fastapi`, `uvicorn`, the `amazon-photos` library itself, and other necessary packages.

### Running the Web App

1.  **Start the Server:**
    Navigate to the directory containing `main.py` (the root of this project) and run the following command:
    ```bash
    uvicorn main:app --reload
    ```
2.  **Access the Web Interface:**
    Open your web browser and go to:
    ```
    http://127.0.0.1:8000/
    ```
    (Note: `127.0.0.1` is the standard IP address for localhost.)

### Obtaining Cookies for the Web App

The web application requires your Amazon Photos session cookies to interact with your account. You'll need to provide these in a JSON format.

1.  **Log in to Amazon Photos:** Open your web browser and log in to your Amazon Photos account (e.g., `https://www.amazon.com/photos/`, `https://www.amazon.co.uk/photos/`, etc.).
2.  **Open Developer Tools:**
    *   Right-click anywhere on the page and select "Inspect" or "Inspect Element".
    *   Alternatively, use keyboard shortcuts (e.g., `Ctrl+Shift+I` or `Cmd+Option+I`).
3.  **Find Your Cookies:**
    *   Go to the "Application" tab (in Chrome/Edge) or "Storage" tab (in Firefox).
    *   Under the "Cookies" section on the left, find the domain for Amazon Photos (e.g., `www.amazon.com`).
    *   You are looking for specific cookie names like `session-id`, and region-specific `ubid-*` and `at-*` cookies.
    *   Alternatively, you can use the "Network" tab. Refresh the Amazon Photos page, click on one of the requests made to the Amazon Photos domain, and look for the `Cookie` header in the "Headers" section.
4.  **Format as JSON:**
    Copy the required cookie names and their corresponding values into a JSON structure like the example below. Paste this JSON into the "Set Cookies" text area in the web application.

    **JSON Cookie Example:**
    ```json
    {
      "session-id": "your-session-id-value-here",
      "ubid-main": "your-ubid-main-value-here",
      "at-main": "your-at-main-value-here"
    }
    ```
    **Important:**
    *   The example above (`ubid-main`, `at-main`) is for users of `amazon.com`.
    *   If you use a different Amazon domain (e.g., `.ca`, `.co.uk`, `.de`), you will need to use the region-specific cookie names. For example, for `amazon.ca`, you would use `ubid-acbca` and `at-acbca`.
    *   Please refer to the **"Setup"** section below for a more comprehensive list of regional cookie names. The same cookie names apply; you just need to format them into the JSON structure shown.

### Using the Interface

1.  **Set Cookies:** Paste the JSON formatted cookies into the text area and click "Save Cookies".
2.  **Start Download:** Click "Start Download" to begin downloading your photo and video metadata and then the files themselves.
3.  **Monitor Progress:** Download progress will be displayed.
4.  **View Gallery:** Once downloads are complete (or partially complete), click "Refresh Gallery" to view your media. Use the "Previous" and "Next" buttons to navigate through your gallery.

### Running with Docker

Alternatively, you can run the web application using Docker. This encapsulates the application and its dependencies in a container.

**1. Prerequisites:**
*   Ensure you have Docker installed on your system.

**2. Building the Docker Image:**
*   Navigate to the root directory of the project (where the `Dockerfile` is located).
*   Run the following command to build the Docker image:
    ```bash
    docker build -t amazon-photos-web .
    ```

**3. Running the Docker Container:**
*   To run the container, you'll want to mount volumes from your host machine into the container. This is crucial for:
    *   Persisting the downloaded media: The `media_downloads` directory where photos and videos are saved.
    *   Persisting the application's database: The `ap.parquet` file which stores metadata.
*   Use the following command to run the container:
    ```bash
    docker run -d \
      -p 8000:8000 \
      -v ./media_downloads:/app/media_downloads \
      -v ./ap.parquet:/app/ap.parquet \
      --name amazon-photos-app \
      amazon-photos-web
    ```
    *   **Explanation of the command:**
        *   `-d`: Runs the container in detached mode (in the background).
        *   `-p 8000:8000`: Maps port 8000 on your host machine to port 8000 in the container.
        *   `-v ./media_downloads:/app/media_downloads`: Mounts the local `./media_downloads` directory (create this directory if it doesn't exist) into the `/app/media_downloads` directory inside the container. The application saves downloaded files here.
        *   `-v ./ap.parquet:/app/ap.parquet`: Mounts a local file named `ap.parquet` into `/app/ap.parquet` inside the container.
            *   **Note on `ap.parquet`:** If `ap.parquet` does not exist locally before running the `docker run` command, Docker might create it as a directory, which can cause issues. It's best to ensure an empty `ap.parquet` file exists locally first (e.g., by running `touch ap.parquet` on Linux/macOS or creating an empty file on Windows) or after the first run of the application (without Docker) which would generate it. The application expects this path for its database.
        *   `--name amazon-photos-app`: Assigns a memorable name to your container.
        *   `amazon-photos-web`: Specifies the Docker image to use (the one you built earlier).

*   **Accessing the Application:**
    Once the container is running, you can access the web application in your browser at `http://localhost:8000`.

**4. Stopping and Removing the Container (Optional):**
*   To stop the container:
    ```bash
    docker stop amazon-photos-app
    ```
*   To remove the container (after it's stopped):
    ```bash
    docker rm amazon-photos-app
    ```

### Output Examples

`ap.db`

|   | dateTimeDigitized        | id                     | name              | ... | model             | apertureValue | focalLength | width | height |   size |
|--:|:-------------------------|:-----------------------|:------------------|:----|:------------------|:--------------|:------------|------:|-------:|-------:|
| 0 | 2019-07-06T18:22:00.000Z | HeMReF-vvJiTTkdPIeWuoP | 1694252973839.png | ... | iPhone XS         | 54823/32325   | 17/4        |  3024 |   4032 | 432777 |
| 1 | 2023-01-18T09:36:22.000Z | z_HiIvASAKqWmdrkjWiqMZ | 1692626817154.jpg | ... | iPhone XS         | 54823/32325   | 17/4        |  3024 |   4032 | 234257 |
| 2 | 2022-08-14T14:13:21.000Z | LKXEZbqoVrhrOYBezisGEQ | 1798219686789.jpg | ... | iPhone 11 Pro Max | 54823/32325   | 17/4        |  3024 |   4032 | 423987 |
| 3 | 2020-06-28T19:32:30.000Z | EPUeciHtfKkGiYkfUyEuMa | 1593482220567.jpg | ... | iPhone XS         | 54823/32325   | 17/4        |  3024 |   4032 | 898957 |
| 4 | 2021-07-07T17:12:55.000Z | fdfKzRJbEyoVeGcfCoJgE- | 1592299282720.png | ... | iPhone XR         | 54823/32325   | 17/4        |  3024 |   4032 | 432556 |
| 5 | 2021-08-18T18:32:41.000Z | crskJSmKPFRhxbpfkivyLm | 1592902159105.png | ... | iPhone XR         | 54823/32325   | 17/4        |  3024 |   4032 | 123123 |
| 6 | 2023-08-23T19:12:21.000Z | qkBFUlyIdkUwVVSaVWWKEF | 1598138358650.png | ... | iPhone 11         | 54823/32325   | 17/4        |  3024 |   4032 | 437887 |
| 7 | 2021-06-19T17:14:13.000Z | TXKMKC-mHvSUrtRfwmtyDe | 1622199863606.jpg | ... | iPhone 12 Pro     | 14447/10653   | 21/5        |  1536 |   2048 | 758432 |
| 8 | 2023-02-15T22:45:40.000Z | FRDvvjcZdpFWiwrIZfTNHO | 1581874518054.jpg | ... | iPhone 8 Plus     | 54823/32325   | 399/100     |  1348 |   2049 | 862883 |

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

## Setup

> [Update] Jan 04 2024: To avoid confusion, setting env vars is no longer supported. One must pass cookies directly as
> shown below for direct library use, or formatted as JSON for the Web Application (see details in the "Web Application for Photo Management" section above).

The following cookie names are required. Log in to Amazon Photos and copy these cookies from your browser's developer tools:

- `session-id`
- `ubid`*
- `at`*

### Canada/Europe

where `xx` is the TLD (top-level domain)

- `ubid-acbxx`
- `at-acbxx`

### United States

- `ubid_main`
- `at_main`

E.g.

```python
from amazon_photos import AmazonPhotos

ap = AmazonPhotos(
    ## US
    # cookies={
    #     'ubid_main': ...,
    #     'at_main': ...,
    #     'session-id': ...,
    # },

    ## Canada
    # cookies={
    #     'ubid-acbca': ...,
    #     'at-acbca': ...,
    #     'session-id': ...,
    # }

    ## Italy
    # cookies={
    #     'ubid-acbit': ...,
    #     'at-acbit': ...,
    #     'session-id': ...,
    # }
)
```

## Examples

> A database named `ap.parquet` will be created during the initial setup. This is mainly used to reduce upload conflicts
> by checking your local file(s) md5 against the database before sending the request.

```python
from amazon_photos import AmazonPhotos

ap = AmazonPhotos(
    # see cookie examples above
    cookies={...},
    # optionally cache all intermediate JSON responses
    tmp='tmp',
    # pandas options
    dtype_backend='pyarrow',
    engine='pyarrow',
)

# get current usage stats
ap.usage()

# get entire Amazon Photos library
nodes = ap.query("type:(PHOTOS OR VIDEOS)")

# query Amazon Photos library with more filters applied
nodes = ap.query("type:(PHOTOS OR VIDEOS) AND things:(plant AND beach OR moon) AND timeYear:(2023) AND timeMonth:(8) AND timeDay:(14) AND location:(CAN#BC#Vancouver)")

# sample first 10 nodes
node_ids = nodes.id[:10]

# move a batch of images/videos to the trash bin
ap.trash(node_ids)

# get trash bin contents
ap.trashed()

# permanently delete a batch of images/videos
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

## Custom Image Labeling (Optional)

Categorize your images into folders using computer vision models.

```bash
pip install amazon-photos[extras] -U
```

See the [Model List](https://www.hobenshield.com/stats/bench/index.html) for a list of all available models.

### Sample Models

**Very Large**

```
eva02_base_patch14_448.mim_in22k_ft_in22k_in1k
```

**Large**

```
eva02_large_patch14_448.mim_m38m_ft_in22k_in1k
```

**Medium**

```
eva02_small_patch14_336.mim_in22k_ft_in1k
vit_base_patch16_clip_384.laion2b_ft_in12k_in1k
vit_base_patch16_clip_384.openai_ft_in12k_in1k
caformer_m36.sail_in22k_ft_in1k_384
```

**Small**

```
eva02_tiny_patch14_336.mim_in22k_ft_in1k
tiny_vit_5m_224.dist_in22k_ft_in1k
edgenext_small.usi_in1k
xcit_tiny_12_p8_384.fb_dist_in1k
```

```python
run(
    'eva02_base_patch14_448.mim_in22k_ft_in22k_in1k',
    path_in='images',
    path_out='labeled',
    thresh=0.0,  # threshold for predictions, 0.9 means you want very confident predictions only
    topk=5,
    # window of predictions to check if using exclude or restrict, if set to 1, only the top prediction will be checked
    exclude=lambda x: re.search('boat|ocean', x, flags=re.I),
    # function to exclude classification of these predicted labels
    restrict=lambda x: re.search('sand|beach|sunset', x, flags=re.I),
    # function to restrict classification to only these predicted labels
    dataloader_options={
        'batch_size': 4,  # *** adjust ***
        'shuffle': False,
        'num_workers': psutil.cpu_count(logical=False),  # *** adjust ***
        'pin_memory': True,
    },
    accumulate=False,
    # accumulate results in path_out, if False, everything in path_out will be deleted before running again
    device='cuda',
    naming_style='name',  # use human-readable label names, optionally use the label index or synset
    debug=0,
)
```
