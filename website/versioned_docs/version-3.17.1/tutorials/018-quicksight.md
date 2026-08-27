---
id: 018-quicksight
title: "QuickSight"
sidebar_position: 18
sidebar_label: "18 - QuickSight"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/018%20-%20QuickSight.ipynb
---

# QuickSight

> This page is generated from [`tutorials/018 - QuickSight.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/018%20-%20QuickSight.ipynb). Open it in Jupyter to run it yourself.
For this tutorial we will use the public AWS COVID-19 data lake.

References:

* [A public data lake for analysis of COVID-19 data](https://aws.amazon.com/blogs/big-data/a-public-data-lake-for-analysis-of-covid-19-data/)
* [Exploring the public AWS COVID-19 data lake](https://aws.amazon.com/blogs/big-data/exploring-the-public-aws-covid-19-data-lake/)
* [CloudFormation template](https://covid19-lake.s3.us-east-2.amazonaws.com/cfn/CovidLakeStack.template.json)

*Please, install the CloudFormation template above to have access to the public data lake.*

*P.S. To be able to access the public data lake, you must allow explicitly QuickSight to access the related external bucket.*

```python
from time import sleep

import awswrangler as wr
```

<strong>List users of QuickSight account<strong>

```python
[{"username": user["UserName"], "role": user["Role"]} for user in wr.quicksight.list_users("default")]
```

```text
[{'username': 'dev', 'role': 'ADMIN'}]
```

```python
wr.catalog.databases()
```

```text
            Database                                   Description
0  aws_sdk_pandas  AWS SDK for pandas Test Arena - Glue Database
1   awswrangler_test                                              
2           covid-19                                              
3            default                         Default Hive database
```

```python
wr.catalog.tables(database="covid-19")
```

```text
    Database                                    Table  \
0   covid-19        alleninstitute_comprehend_medical   
1   covid-19                  alleninstitute_metadata   
2   covid-19                            country_codes   
3   covid-19                       county_populations   
4   covid-19              covid_knowledge_graph_edges   
5   covid-19       covid_knowledge_graph_nodes_author   
6   covid-19      covid_knowledge_graph_nodes_concept   
7   covid-19  covid_knowledge_graph_nodes_institution   
8   covid-19        covid_knowledge_graph_nodes_paper   
9   covid-19        covid_knowledge_graph_nodes_topic   
10  covid-19               covid_testing_states_daily   
11  covid-19                   covid_testing_us_daily   
12  covid-19                   covid_testing_us_total   
13  covid-19                           covidcast_data   
14  covid-19                       covidcast_metadata   
15  covid-19                               enigma_jhu   
16  covid-19                    enigma_jhu_timeseries   
17  covid-19                            hospital_beds   
18  covid-19                         nytimes_counties   
19  covid-19                           nytimes_states   
20  covid-19     prediction_models_county_predictions   
21  covid-19         prediction_models_severity_index   
22  covid-19                    tableau_covid_datahub   
23  covid-19                              tableau_jhu   
24  covid-19                   us_state_abbreviations   
25  covid-19               world_cases_deaths_testing   

                                          Description  \
0   Comprehend Medical results run against Allen I...   
1   Metadata on papers pulled from the Allen Insti...   
2                      Lookup table for country codes   
3   Lookup table for population for each county ba...   
4               AWS Knowledge Graph for COVID-19 data   
5               AWS Knowledge Graph for COVID-19 data   
6               AWS Knowledge Graph for COVID-19 data   
7               AWS Knowledge Graph for COVID-19 data   
8               AWS Knowledge Graph for COVID-19 data   
9               AWS Knowledge Graph for COVID-19 data   
10  USA total test daily trend by state.  Sourced ...   
… (output truncated, 43 more lines)
```

<strong>Create data source of QuickSight<strong>
Note: data source stores the connection information.

```python
wr.quicksight.create_athena_data_source(
    name="covid-19",
    workgroup="primary",
    allowed_to_manage={"users": ["dev"]},
)
```

```python
wr.catalog.tables(database="covid-19", name_contains="nyt")
```

```text
   Database             Table  \
0  covid-19  nytimes_counties   
1  covid-19    nytimes_states   

                                         Description  \
0  Data on COVID-19 cases from NY Times at US cou...   
1  Data on COVID-19 cases from NY Times at US sta...   

                                    Columns Partitions  
0  date, county, state, fips, cases, deaths             
1          date, state, fips, cases, deaths
```

```python
wr.athena.read_sql_query("SELECT * FROM nytimes_counties limit 10", database="covid-19", ctas_approach=False)
```

```text
         date       county       state   fips  cases  deaths
0  2020-01-21    Snohomish  Washington  53061      1       0
1  2020-01-22    Snohomish  Washington  53061      1       0
2  2020-01-23    Snohomish  Washington  53061      1       0
3  2020-01-24         Cook    Illinois  17031      1       0
4  2020-01-24    Snohomish  Washington  53061      1       0
5  2020-01-25       Orange  California  06059      1       0
6  2020-01-25         Cook    Illinois  17031      1       0
7  2020-01-25    Snohomish  Washington  53061      1       0
8  2020-01-26     Maricopa     Arizona  04013      1       0
9  2020-01-26  Los Angeles  California  06037      1       0
```

```python
sql = """
SELECT
  j.*,
  co.Population,
  co.county AS county2,
  hb.*
FROM
  (
    SELECT
      date,
      county,
      state,
      fips,
      cases as confirmed,
      deaths
    FROM "covid-19".nytimes_counties
  ) j
  LEFT OUTER JOIN (
    SELECT
      DISTINCT county,
      state,
      "population estimate 2018" AS Population
    FROM
      "covid-19".county_populations
    WHERE
      state IN (
        SELECT
          DISTINCT state
        FROM
          "covid-19".nytimes_counties
      )
      AND county IN (
        SELECT
          DISTINCT county as county
        FROM "covid-19".nytimes_counties
      )
  ) co ON co.county = j.county
  AND co.state = j.state
  LEFT OUTER JOIN (
    SELECT
      count(objectid) as Hospital,
      fips as hospital_fips,
      sum(num_licensed_beds) as licensed_beds,
      sum(num_staffed_beds) as staffed_beds,
      sum(num_icu_beds) as icu_beds,
      avg(bed_utilization) as bed_utilization,
      sum(
        potential_increase_in_bed_capac
      ) as potential_increase_bed_capacity
    FROM "covid-19".hospital_beds
    WHERE
      fips in (
        SELECT
          DISTINCT fips
        FROM
          "covid-19".nytimes_counties
      )
    GROUP BY
      2
  ) hb ON hb.hospital_fips = j.fips
"""

wr.athena.read_sql_query(sql, database="covid-19", ctas_approach=False)
```

```text
              date      county     state   fips  confirmed  deaths population  \
0       2020-04-12        Park   Montana  30067          7       0      16736   
1       2020-04-12     Ravalli   Montana  30081          3       0      43172   
2       2020-04-12  Silver Bow   Montana  30093         11       0      34993   
3       2020-04-12        Clay  Nebraska  31035          2       0       6214   
4       2020-04-12      Cuming  Nebraska  31039          2       0       8940   
...            ...         ...       ...    ...        ...     ...        ...   
227684  2020-06-11     Hockley     Texas  48219         28       1      22980   
227685  2020-06-11    Hudspeth     Texas  48229         11       0       4795   
227686  2020-06-11       Jones     Texas  48253        633       0      19817   
227687  2020-06-11    La Salle     Texas  48283          4       0       7531   
227688  2020-06-11   Limestone     Texas  48293         36       1      23519   

           county2  Hospital hospital_fips  licensed_beds  staffed_beds  \
0             Park         0         30067             25            25   
1          Ravalli         0         30081             25            25   
2       Silver Bow         0         30093             98            71   
3             Clay      <NA>          <NA>           <NA>          <NA>   
4           Cuming         0         31039             25            25   
...            ...       ...           ...            ...           ...   
227684     Hockley         0         48219             48            48   
227685    Hudspeth      <NA>          <NA>           <NA>          <NA>   
227686       Jones         0         48253             45             7   
227687    La Salle      <NA>          <NA>           <NA>          <NA>   
227688   Limestone         0         48293             78            69   

        icu_beds  bed_utilization  potential_increase_bed_capacity  
0              4         0.432548                                0  
1              5         0.567781                                0  
2             11         0.551457                               27  
3           <NA>              NaN                             <NA>  
4              4         0.204493                                0  
...          ...              ...                              ...  
227684         8         0.120605                                0  
227685      <NA>              NaN                             <NA>  
227686         1         0.718591                               38  
227687      <NA>              NaN                             <NA>  
227688         9         0.163940                                9  

[227689 rows x 15 columns]
```

<strong>Create Dataset with custom SQL option<strong>

```python
wr.quicksight.create_athena_dataset(
    name="covid19-nytimes-usa",
    sql=sql,
    sql_name="CustomSQL",
    data_source_name="covid-19",
    import_mode="SPICE",
    allowed_to_manage={"users": ["dev"]},
)
```

```python
ingestion_id = wr.quicksight.create_ingestion("covid19-nytimes-usa")
```

<strong>Wait ingestion<strong>

```python
while wr.quicksight.describe_ingestion(ingestion_id=ingestion_id, dataset_name="covid19-nytimes-usa")[
    "IngestionStatus"
] not in ["COMPLETED", "FAILED"]:
    sleep(1)
```

<strong>Describe last ingestion<strong>

```python
wr.quicksight.describe_ingestion(ingestion_id=ingestion_id, dataset_name="covid19-nytimes-usa")["RowInfo"]
```

```text
{'RowsIngested': 227689, 'RowsDropped': 0}
```

<strong>List all ingestions<strong>

```python
[
    {"time": user["CreatedTime"], "source": user["RequestSource"]}
    for user in wr.quicksight.list_ingestions("covid19-nytimes-usa")
]
```

```text
[{'time': datetime.datetime(2020, 6, 12, 15, 13, 46, 996000, tzinfo=tzlocal()),
  'source': 'MANUAL'},
 {'time': datetime.datetime(2020, 6, 12, 15, 13, 42, 344000, tzinfo=tzlocal()),
  'source': 'MANUAL'}]
```

<strong>Create new dataset from a table directly<strong>

```python
wr.quicksight.create_athena_dataset(
    name="covid-19-tableau_jhu",
    table="tableau_jhu",
    data_source_name="covid-19",
    database="covid-19",
    import_mode="DIRECT_QUERY",
    rename_columns={"cases": "Count_of_Cases", "combined_key": "County"},
    cast_columns_types={"Count_of_Cases": "INTEGER"},
    tag_columns={"combined_key": [{"ColumnGeographicRole": "COUNTY"}]},
    allowed_to_manage={"users": ["dev"]},
)
```

<strong>Cleaning up<strong>

```python
wr.quicksight.delete_data_source("covid-19")
wr.quicksight.delete_dataset("covid19-nytimes-usa")
wr.quicksight.delete_dataset("covid-19-tableau_jhu")
```
