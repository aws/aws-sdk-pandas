---
id: 031-opensearch
title: "OpenSearch"
sidebar_position: 31
sidebar_label: "31 - OpenSearch"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/031%20-%20OpenSearch.ipynb
---

# OpenSearch

> This page is generated from [`tutorials/031 - OpenSearch.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/031%20-%20OpenSearch.ipynb). Open it in Jupyter to run it yourself.
## Table of Contents
* [1. Initialize](#1.-Initialize)
    * [Connect to your Amazon OpenSearch domain](#Connect-to-your-Amazon-OpenSearch-domain)
    * [Enter your bucket name](#enter-your-bucket-name)
    * [Initialize sample data](#initialize-sample-data)
* [2. Indexing (load)](#2.-Indexing-(load))
	* [Index documents (no Pandas)](#index-documents-(no-pandas))
	* [Index json file](#index-json-file)
    * [Index CSV](#index-csv)
* [3. Search](#3.-Search)
	* [Search by DSL](#search-by-dsl)
	* [Search by SQL](#search-by-sql)
* [4. Delete Indices](#4.-Delete-Indices)
* [5. Bonus - Prepare data and index from DataFrame](#5.-Bonus---Prepare-data-and-index-from-DataFrame)
	* [Prepare the data for indexing](#prepare-the-data-for-indexing)
    * [Create index with mapping](#create-index-with-mapping)
    * [Index dataframe](#index-dataframe)
    * [Execute geo query](#execute-geo-query)

## 1. Initialize

```python
# Install the optional modules first
!pip install 'awswrangler[opensearch]'
```

```python
import awswrangler as wr
```

### Connect to your Amazon OpenSearch domain

```python
client = wr.opensearch.connect(
    host="OPENSEARCH-ENDPOINT",
    #     username='FGAC-USERNAME(OPTIONAL)',
    #     password='FGAC-PASSWORD(OPTIONAL)'
)
client.info()
```

### Enter your bucket name

```python
bucket = "BUCKET"
```

### Initialize sample data

```python
sf_restaurants_inspections = [
    {
        "inspection_id": "24936_20160609",
        "business_address": "315 California St",
        "business_city": "San Francisco",
        "business_id": "24936",
        "business_location": {"lon": -122.400152, "lat": 37.793199},
        "business_name": "San Francisco Soup Company",
        "business_postal_code": "94104",
        "business_state": "CA",
        "inspection_date": "2016-06-09T00:00:00.000",
        "inspection_score": 77,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Improper food labeling or menu misrepresentation",
        "violation_id": "24936_20160609_103141",
    },
    {
        "inspection_id": "60354_20161123",
        "business_address": "10 Mason St",
        "business_city": "San Francisco",
        "business_id": "60354",
        "business_location": {"lon": -122.409061, "lat": 37.783527},
        "business_name": "Soup Unlimited",
        "business_postal_code": "94102",
        "business_state": "CA",
        "inspection_date": "2016-11-23T00:00:00.000",
        "inspection_type": "Routine",
        "inspection_score": 95,
    },
    {
        "inspection_id": "1797_20160705",
        "business_address": "2872 24th St",
        "business_city": "San Francisco",
        "business_id": "1797",
        "business_location": {"lon": -122.409752, "lat": 37.752807},
        "business_name": "TIO CHILOS GRILL",
        "business_postal_code": "94110",
        "business_state": "CA",
        "inspection_date": "2016-07-05T00:00:00.000",
        "inspection_score": 90,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Unclean nonfood contact surfaces",
        "violation_id": "1797_20160705_103142",
    },
    {
        "inspection_id": "66198_20160527",
        "business_address": "1661 Tennessee St Suite 3B",
        "business_city": "San Francisco Whard Restaurant",
        "business_id": "66198",
        "business_location": {"lon": -122.388478, "lat": 37.75072},
        "business_name": "San Francisco Restaurant",
        "business_postal_code": "94107",
        "business_state": "CA",
        "inspection_date": "2016-05-27T00:00:00.000",
        "inspection_type": "Routine",
        "inspection_score": 56,
    },
    {
        "inspection_id": "5794_20160907",
        "business_address": "2162 24th Ave",
        "business_city": "San Francisco",
        "business_id": "5794",
        "business_location": {"lon": -122.481299, "lat": 37.747228},
        "business_name": "Soup House",
        "business_phone_number": "+14155752700",
        "business_postal_code": "94116",
        "business_state": "CA",
        "inspection_date": "2016-09-07T00:00:00.000",
        "inspection_score": 96,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Unapproved or unmaintained equipment or utensils",
        "violation_id": "5794_20160907_103144",
    },
    # duplicate record
    {
        "inspection_id": "5794_20160907",
        "business_address": "2162 24th Ave",
        "business_city": "San Francisco",
        "business_id": "5794",
        "business_location": {"lon": -122.481299, "lat": 37.747228},
        "business_name": "Soup-or-Salad",
        "business_phone_number": "+14155752700",
        "business_postal_code": "94116",
        "business_state": "CA",
        "inspection_date": "2016-09-07T00:00:00.000",
        "inspection_score": 96,
        "inspection_type": "Routine - Unscheduled",
        "risk_category": "Low Risk",
        "violation_description": "Unapproved or unmaintained equipment or utensils",
        "violation_id": "5794_20160907_103144",
    },
]
```

## 2. Indexing (load)

### Index documents (no Pandas)

```python
# index documents w/o providing keys (_id is auto-generated)
wr.opensearch.index_documents(client, documents=sf_restaurants_inspections, index="sf_restaurants_inspections")
```

```text
Indexing: 100% (6/6)|####################################|Elapsed Time: 0:00:01
```

```text
{'success': 6, 'errors': []}
```

```python
# read all documents. There are total 6 documents
wr.opensearch.search(
    client, index="sf_restaurants_inspections", _source=["inspection_id", "business_name", "business_location"]
)
```

```text
                                    _id               business_name  \
0  663dd72d-0da4-495b-b0ae-ed000105ae73            TIO CHILOS GRILL   
1  ff2f50f6-5415-4706-9bcb-af7c5eb0afa3                  Soup House   
2  b9e8f6a2-8fd1-4660-b041-2997a1a80984  San Francisco Soup Company   
3  56b352e6-102b-4eff-8296-7e1fb2459bab              Soup Unlimited   
4  6fec5411-f79a-48e4-be7b-e0e44d5ebbab    San Francisco Restaurant   
5  7ba4fb17-f9a9-49da-b90e-8b3553d6d97c               Soup-or-Salad   

    inspection_id  business_location.lon  business_location.lat  
0   1797_20160705            -122.409752              37.752807  
1   5794_20160907            -122.481299              37.747228  
2  24936_20160609            -122.400152              37.793199  
3  60354_20161123            -122.409061              37.783527  
4  66198_20160527            -122.388478              37.750720  
5   5794_20160907            -122.481299              37.747228
```

### Index json file<a class="anchor" id="index-json"></a>

```python
import pandas as pd

df = pd.DataFrame(sf_restaurants_inspections)
path = f"s3://{bucket}/json/sf_restaurants_inspections.json"
wr.s3.to_json(df, path, orient="records", lines=True)
```

```python
# index json w/ providing keys
wr.opensearch.index_json(
    client,
    path=path,  # path can be s3 or local
    index="sf_restaurants_inspections_dedup",
    id_keys=["inspection_id"],  # can be multiple fields. arg applicable to all index_* functions
)
```

```text
Indexing: 100% (6/6)|####################################|Elapsed Time: 0:00:00
```

```text
{'success': 6, 'errors': []}
```

```python
# now there are no duplicates. There are total 5 documents
wr.opensearch.search(
    client, index="sf_restaurants_inspections_dedup", _source=["inspection_id", "business_name", "business_location"]
)
```

```text
              _id               business_name   inspection_id  \
0  24936_20160609  San Francisco Soup Company  24936_20160609   
1  66198_20160527    San Francisco Restaurant  66198_20160527   
2   5794_20160907               Soup-or-Salad   5794_20160907   
3  60354_20161123              Soup Unlimited  60354_20161123   
4   1797_20160705            TIO CHILOS GRILL   1797_20160705   

   business_location.lon  business_location.lat  
0            -122.400152              37.793199  
1            -122.388478              37.750720  
2            -122.481299              37.747228  
3            -122.409061              37.783527  
4            -122.409752              37.752807
```

### Index CSV

```python
wr.opensearch.index_csv(
    client,
    index="nyc_restaurants_inspections_sample",
    path="https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD",  # index_csv supports local, s3 and url path
    id_keys=["CAMIS"],
    pandas_kwargs={
        "na_filter": True,
        "nrows": 1000,
    },  # pandas.read_csv() args - https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
    bulk_size=500,  # modify based on your cluster size
)
```

```text
Indexing: 100% (1000/1000)|##############################|Elapsed Time: 0:00:00
```

```text
{'success': 1000, 'errors': []}
```

```python
wr.opensearch.search(client, index="nyc_restaurants_inspections_sample", size=5)
```

```text
        _id     CAMIS                         DBA       BORO BUILDING  \
0  41610426  41610426        GLOW THAI RESTAURANT   Brooklyn     7107   
1  40811162  40811162                   CARMINE'S  Manhattan     2450   
2  50012113  50012113                        TANG     Queens   196-50   
3  50014618  50014618                 TOTTO RAMEN  Manhattan      248   
4  50045782  50045782  OLLIE'S CHINESE RESTAURANT  Manhattan     2705   

               STREET  ZIPCODE       PHONE CUISINE DESCRIPTION  \
0            3 AVENUE  11209.0  7187481920                Thai   
1            BROADWAY  10024.0  2123622200             Italian   
2  NORTHERN BOULEVARD  11358.0  7182797080              Korean   
3    EAST   52 STREET  10022.0  2124210052            Japanese   
4            BROADWAY  10025.0  2129323300             Chinese   

  INSPECTION DATE  ... RECORD DATE                        INSPECTION TYPE  \
0      02/26/2020  ...  10/04/2021       Cycle Inspection / Re-inspection   
1      05/28/2019  ...  10/04/2021  Cycle Inspection / Initial Inspection   
2      08/16/2018  ...  10/04/2021  Cycle Inspection / Initial Inspection   
3      08/20/2018  ...  10/04/2021       Cycle Inspection / Re-inspection   
4      10/21/2019  ...  10/04/2021       Cycle Inspection / Re-inspection   

    Latitude  Longitude  Community Board Council District Census Tract  \
0  40.633865 -74.026798            310.0             43.0       6800.0   
1  40.791168 -73.974308            107.0              6.0      17900.0   
2  40.757850 -73.784593            411.0             19.0     145101.0   
3  40.756596 -73.968749            106.0              4.0       9800.0   
4  40.799318 -73.968440            107.0              6.0      19100.0   

         BIN           BBL   NTA  
0  3146519.0  3.058910e+09  BK31  
1  1033560.0  1.012380e+09  MN12  
2  4124565.0  4.055200e+09  QN48  
3  1038490.0  1.013250e+09  MN19  
4  1056562.0  1.018750e+09  MN12  

[5 rows x 27 columns]
```

## 3. Search
Search results are returned as Pandas DataFrame

### Search by DSL

```python
# add a search query. search all soup businesses
wr.opensearch.search(
    client,
    index="sf_restaurants_inspections",
    _source=["inspection_id", "business_name", "business_location"],
    filter_path=["hits.hits._id", "hits.hits._source"],
    search_body={"query": {"match": {"business_name": "soup"}}},
)
```

```text
                                    _id               business_name  \
0  ff2f50f6-5415-4706-9bcb-af7c5eb0afa3                  Soup House   
1  7ba4fb17-f9a9-49da-b90e-8b3553d6d97c               Soup-or-Salad   
2  b9e8f6a2-8fd1-4660-b041-2997a1a80984  San Francisco Soup Company   
3  56b352e6-102b-4eff-8296-7e1fb2459bab              Soup Unlimited   

    inspection_id  business_location.lon  business_location.lat  
0   5794_20160907            -122.481299              37.747228  
1   5794_20160907            -122.481299              37.747228  
2  24936_20160609            -122.400152              37.793199  
3  60354_20161123            -122.409061              37.783527
```

### Search by SQL

```python
wr.opensearch.search_by_sql(
    client,
    sql_query="""SELECT business_name, inspection_score
                    FROM sf_restaurants_inspections_dedup
                    WHERE business_name LIKE '%soup%'
                    ORDER BY inspection_score DESC LIMIT 5""",
)
```

```text
                             _index _type             _id _score  \
0  sf_restaurants_inspections_dedup  _doc   5794_20160907   None   
1  sf_restaurants_inspections_dedup  _doc  60354_20161123   None   
2  sf_restaurants_inspections_dedup  _doc  24936_20160609   None   

                business_name  inspection_score  
0               Soup-or-Salad                96  
1              Soup Unlimited                95  
2  San Francisco Soup Company                77
```

## 4. Delete Indices

```python
wr.opensearch.delete_index(client=client, index="sf_restaurants_inspections")
```

```text
{'acknowledged': True}
```

## 5. Bonus - Prepare data and index from DataFrame

For this exercise we'll use [DOHMH New York City Restaurant Inspection Results dataset](https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection-Results/43nn-pn8j)

```python
import pandas as pd
```

```python
df = pd.read_csv("https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD")
```

### Prepare the data for indexing

```python
# fields names underscore casing
df.columns = [col.lower().replace(" ", "_") for col in df.columns]

# convert lon/lat to OpenSearch geo_point
df["business_location"] = (
    "POINT (" + df.longitude.fillna("0").astype(str) + " " + df.latitude.fillna("0").astype(str) + ")"
)
```

### Create index with mapping

```python
# delete index if exists
wr.opensearch.delete_index(client=client, index="nyc_restaurants")

# use dynamic_template to map date fields
# define business_location as geo_point
wr.opensearch.create_index(
    client=client,
    index="nyc_restaurants_inspections",
    mappings={
        "dynamic_templates": [{"dates": {"match": "*date", "mapping": {"type": "date", "format": "MM/dd/yyyy"}}}],
        "properties": {"business_location": {"type": "geo_point"}},
    },
)
```

```text
{'acknowledged': True,
 'shards_acknowledged': True,
 'index': 'nyc_restaurants_inspections'}
```

### Index dataframe

```python
wr.opensearch.index_df(client, df=df, index="nyc_restaurants_inspections", id_keys=["camis"], bulk_size=1000)
```

```text
Indexing: 100% (382655/382655)|##########################|Elapsed Time: 0:04:15
```

```text
{'success': 382655, 'errors': []}
```

### Execute geo query
#### Sort restaurants by distance from Times-Square

```python
wr.opensearch.search(
    client,
    index="nyc_restaurants_inspections",
    filter_path=["hits.hits._source"],
    size=100,
    search_body={
        "query": {"match_all": {}},
        "sort": [
            {
                "_geo_distance": {
                    "business_location": {  # Times-Square - https://geojson.io/#map=16/40.7563/-73.9862
                        "lat": 40.75613228383523,
                        "lon": -73.9865791797638,
                    },
                    "order": "asc",
                }
            }
        ],
    },
)
```

```text
       camis                    dba       boro building            street  \
0   41551304            THE COUNTER  Manhattan        7      TIMES SQUARE   
1   50055665           ANN INC CAFE  Manhattan        7      TIMES SQUARE   
2   50049552        ERNST AND YOUNG  Manhattan        5          TIMES SQ   
3   50014078            RED LOBSTER  Manhattan        5          TIMES SQ   
4   50015171  NEW AMSTERDAM THEATER  Manhattan      214  WEST   42 STREET   
..       ...                    ...        ...      ...               ...   
95  41552060         PROSKAUER ROSE  Manhattan       11      TIMES SQUARE   
96  41242148         GABBY O'HARA'S  Manhattan      123  WEST   39 STREET   
97  50095860       THE TIMES EATERY  Manhattan      680          8 AVENUE   
98  50072861                   ITSU  Manhattan      530          7 AVENUE   
99  50068109         LUKE'S LOBSTER  Manhattan     1407          BROADWAY   

    zipcode       phone cuisine_description inspection_date  \
0   10036.0  2129976801            American      12/22/2016   
1   10036.0  2125413287            American      12/11/2019   
2   10036.0  2127739994          Coffee/Tea      11/30/2018   
3   10036.0  2127306706             Seafood      10/03/2017   
4   10036.0  2125825472            American      06/26/2018   
..      ...         ...                 ...             ...   
95  10036.0  2129695493            American      08/11/2017   
96  10018.0  2122788984               Irish      07/30/2019   
97  10036.0  6463867787            American      02/28/2020   
98  10018.0  9176393645  Asian/Asian Fusion      09/10/2018   
99  10018.0  9174759192             Seafood      09/06/2017   

                                             action  ...  \
0   Violations were cited in the following area(s).  ...   
1   Violations were cited in the following area(s).  ...   
2   Violations were cited in the following area(s).  ...   
3   Violations were cited in the following area(s).  ...   
4   Violations were cited in the following area(s).  ...   
..                                              ...  ...   
95  Violations were cited in the following area(s).  ...   
96  Violations were cited in the following area(s).  ...   
97  Violations were cited in the following area(s).  ...   
98  Violations were cited in the following area(s).  ...   
99  Violations were cited in the following area(s).  ...   

                                      inspection_type   latitude  longitude  \
… (output truncated, 39 more lines)
```
