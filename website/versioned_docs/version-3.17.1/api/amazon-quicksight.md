---
id: amazon-quicksight
title: "Amazon QuickSight"
sidebar_position: 20
---

# Amazon QuickSight

Module: `wr.quicksight`

### cancel_ingestion

```python
wr.quicksight.cancel_ingestion(
    ingestion_id: 'str',
    dataset_name: 'str | None' = None,
    dataset_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Cancel an ongoing ingestion of data into SPICE.

:::note
You must pass a not None value for `dataset_name` or `dataset_id` argument.
:::

**Parameters**

- **`ingestion_id`** — Ingestion ID.
- **`dataset_name`** — Dataset name.
- **`dataset_id`** — Dataset ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>>  wr.quicksight.cancel_ingestion(ingestion_id="...", dataset_name="...")
```

---

### create_athena_data_source

```python
wr.quicksight.create_athena_data_source(
    name: 'str',
    workgroup: 'str' = 'primary',
    allowed_to_use: '_AllowedType' = None,
    allowed_to_manage: '_AllowedType' = None,
    tags: 'dict[str, str] | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    namespace: 'str' = 'default'
) -> 'None'
```

Create a QuickSight data source pointing to an Athena/Workgroup.

:::note
You will not be able to see the the data source in the console
if you not pass your user to one of the `allowed_*` arguments.
:::

**Parameters**

- **`name`** — Data source name.
- **`workgroup`** — Athena workgroup.
- **`tags`** — Key/Value collection to put on the Cluster. e.g. ``{"foo": "boo", "bar": "xoo"})``
- **`allowed_to_use`** — Dictionary containing usernames and groups that will be allowed to see and use the data. e.g. ``{"users": ["john", "Mary"], "groups": ["engineering", "customers"]}`` Alternatively, if a list of string is passed, it will be interpreted as a list of usernames only.
- **`allowed_to_manage`** — Dictionary containing usernames and groups that will be allowed to see, use, update and delete the data source. e.g. ``{"users": ["Mary"], "groups": ["engineering"]}`` Alternatively, if a list of string is passed, it will be interpreted as a list of usernames only.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`namespace`** — The namespace. Currently, you should set this to default.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.create_athena_data_source(
...     name="...",
...     allowed_to_manage=["john"],
... )
```

---

### create_athena_dataset

```python
wr.quicksight.create_athena_dataset(
    name: 'str',
    database: 'str | None' = None,
    table: 'str | None' = None,
    sql: 'str | None' = None,
    sql_name: 'str | None' = None,
    data_source_name: 'str | None' = None,
    data_source_arn: 'str | None' = None,
    import_mode: "Literal['SPICE', 'DIRECT_QUERY']" = 'DIRECT_QUERY',
    allowed_to_use: '_AllowedType' = None,
    allowed_to_manage: '_AllowedType' = None,
    logical_table_alias: 'str' = 'LogicalTable',
    rename_columns: 'dict[str, str] | None' = None,
    cast_columns_types: 'dict[str, str] | None' = None,
    tag_columns: 'dict[str,
    list[dict[str, Any]]] | None' = None,
    tags: 'dict[str, str] | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    namespace: 'str' = 'default'
) -> 'str'
```

Create a QuickSight dataset.

:::note
You will not be able to see the the dataset in the console
if you not pass your username to one of the `allowed_*` arguments.
:::
:::note
You must pass `database`/`table` OR `sql` argument.
:::
:::note
You must pass `data_source_name` OR `data_source_arn` argument.
:::

**Parameters**

- **`name`** — Dataset name.
- **`database`** — Athena's database name.
- **`table`** — Athena's table name.
- **`sql`** — Use a SQL query to define your table.
- **`sql_name`** — Query name.
- **`data_source_name`** — QuickSight data source name.
- **`data_source_arn`** — QuickSight data source ARN.
- **`import_mode`** — Indicates whether you want to import the data into SPICE.
- **`tags`** — Key/Value collection to put on the Cluster. e.g. {"foo": "boo", "bar": "xoo"}
- **`allowed_to_use`** — Dictionary containing usernames and groups that will be allowed to see and use the data. e.g. ``{"users": ["john", "Mary"], "groups": ["engineering", "customers"]}`` Alternatively, if a list of string is passed, it will be interpreted as a list of usernames only.
- **`allowed_to_manage`** — Dictionary containing usernames and groups that will be allowed to see, use, update and delete the data source. e.g. ``{"users": ["Mary"], "groups": ["engineering"]}`` Alternatively, if a list of string is passed, it will be interpreted as a list of usernames only.
- **`logical_table_alias`** — A display name for the logical table.
- **`rename_columns`** — Dictionary to map column renames. e.g. {"old_name": "new_name", "old_name2": "new_name2"}
- **`cast_columns_types`** — Dictionary to map column casts. e.g. {"col_name": "STRING", "col_name2": "DECIMAL"} Valid types: 'STRING'|'INTEGER'|'DECIMAL'|'DATETIME'
- **`tag_columns`** — Dictionary to map column tags. e.g. {"col_name": [{ "ColumnGeographicRole": "CITY" }],"col_name2": [{ "ColumnDescription": { "Text": "description" }}]} Valid geospatial roles: 'COUNTRY'|'STATE'|'COUNTY'|'CITY'|'POSTCODE'|'LONGITUDE'|'LATITUDE'
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`namespace`** — The namespace. Currently, you should set this to default.

**Returns**

- Dataset ID.

**Examples**

```python
>>> import awswrangler as wr
>>> dataset_id = wr.quicksight.create_athena_dataset(
...     name="...",
...     database="..."
...     table="..."
...     data_source_name="..."
...     allowed_to_manage=["Mary"],
... )
```

---

### create_ingestion

```python
wr.quicksight.create_ingestion(
    dataset_name: 'str | None' = None,
    dataset_id: 'str | None' = None,
    ingestion_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create and starts a new SPICE ingestion on a dataset.

:::note
You must pass **dataset_name** OR **dataset_id** argument.
:::

**Parameters**

- **`dataset_name`** — Dataset name.
- **`dataset_id`** — Dataset ID.
- **`ingestion_id`** — Ingestion ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Ingestion ID

**Examples**

```python
>>> import awswrangler as wr
>>> status = wr.quicksight.create_ingestion("my_dataset")
```

---

### delete_all_dashboards

```python
wr.quicksight.delete_all_dashboards(
    account_id: 'str | None' = None,
    regex_filter: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete all dashboards.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`regex_filter`** — Regex regex_filter that will delete all dashboards with a match in their `Name`
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_all_dashboards()
```

---

### delete_all_data_sources

```python
wr.quicksight.delete_all_data_sources(
    account_id: 'str | None' = None,
    regex_filter: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete all data sources.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`regex_filter`** — Regex regex_filter that will delete all data sources with a match in their `Name`
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_all_data_sources()
```

---

### delete_all_datasets

```python
wr.quicksight.delete_all_datasets(
    account_id: 'str | None' = None,
    regex_filter: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete all datasets.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`regex_filter`** — Regex regex_filter that will delete all datasets with a match in their `Name`
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_all_datasets()
```

---

### delete_all_templates

```python
wr.quicksight.delete_all_templates(
    account_id: 'str | None' = None,
    regex_filter: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete all templates.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`regex_filter`** — Regex regex_filter that will delete all templates with a match in their `Name`
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_all_templates()
```

---

### delete_dashboard

```python
wr.quicksight.delete_dashboard(
    name: 'str | None' = None,
    dashboard_id: 'str | None' = None,
    version_number: 'int | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a dashboard.

:::note
You must pass a not None `name` or `dashboard_id` argument.
:::

**Parameters**

- **`name`** — Dashboard name.
- **`dashboard_id`** — The ID for the dashboard.
- **`version_number`** — The version number of the dashboard. If the version number property is provided, only the specified version of the dashboard is deleted.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_dashboard(name="...")
```

---

### delete_data_source

```python
wr.quicksight.delete_data_source(
    name: 'str | None' = None,
    data_source_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a data source.

:::note
You must pass a not None `name` or `data_source_id` argument.
:::

**Parameters**

- **`name`** — Dashboard name.
- **`data_source_id`** — The ID for the data source.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_data_source(name="...")
```

---

### delete_dataset

```python
wr.quicksight.delete_dataset(
    name: 'str | None' = None,
    dataset_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a dataset.

:::note
You must pass a not None `name` or `dataset_id` argument.
:::

**Parameters**

- **`name`** — Dashboard name.
- **`dataset_id`** — The ID for the dataset.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_dataset(name="...")
```

---

### delete_template

```python
wr.quicksight.delete_template(
    name: 'str | None' = None,
    template_id: 'str | None' = None,
    version_number: 'int | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a template.

:::note
You must pass a not None `name` or `template_id` argument.
:::

**Parameters**

- **`name`** — Dashboard name.
- **`template_id`** — The ID for the dashboard.
- **`version_number`** — Specifies the version of the template that you want to delete. If you don't provide a version number, it deletes all versions of the template.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.quicksight.delete_template(name="...")
```

---

### describe_dashboard

```python
wr.quicksight.describe_dashboard(
    name: 'str | None' = None,
    dashboard_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Describe a QuickSight dashboard by name or ID.

:::note
You must pass a not None `name` or `dashboard_id` argument.
:::

**Parameters**

- **`name`** — Dashboard name.
- **`dashboard_id`** — Dashboard ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dashboard Description.

**Examples**

```python
>>> import awswrangler as wr
>>> description = wr.quicksight.describe_dashboard(name="my-dashboard")
```

---

### describe_data_source

```python
wr.quicksight.describe_data_source(
    name: 'str | None' = None,
    data_source_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Describe a QuickSight data source by name or ID.

:::note
You must pass a not None `name` or `data_source_id` argument.
:::

**Parameters**

- **`name`** — Data source name.
- **`data_source_id`** — Data source ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data source Description.

**Examples**

```python
>>> import awswrangler as wr
>>> description = wr.quicksight.describe_data_source("...")
```

---

### describe_data_source_permissions

```python
wr.quicksight.describe_data_source_permissions(
    name: 'str | None' = None,
    data_source_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Describe a QuickSight data source permissions by name or ID.

:::note
You must pass a not None `name` or `data_source_id` argument.
:::

**Parameters**

- **`name`** — Data source name.
- **`data_source_id`** — Data source ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data source Permissions Description.

**Examples**

```python
>>> import awswrangler as wr
>>> description = wr.quicksight.describe_data_source_permissions("my-data-source")
```

---

### describe_dataset

```python
wr.quicksight.describe_dataset(
    name: 'str | None' = None,
    dataset_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Describe a QuickSight dataset by name or ID.

:::note
You must pass a not None `name` or `dataset_id` argument.
:::

**Parameters**

- **`name`** — Dataset name.
- **`dataset_id`** — Dataset ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dataset Description.

**Examples**

```python
>>> import awswrangler as wr
>>> description = wr.quicksight.describe_dataset("my-dataset")
```

---

### describe_ingestion

```python
wr.quicksight.describe_ingestion(
    ingestion_id: 'str',
    dataset_name: 'str | None' = None,
    dataset_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Describe a QuickSight ingestion by ID.

:::note
You must pass a not None value for `dataset_name` or `dataset_id` argument.
:::

**Parameters**

- **`ingestion_id`** — Ingestion ID.
- **`dataset_name`** — Dataset name.
- **`dataset_id`** — Dataset ID.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Ingestion Description.

**Examples**

```python
>>> import awswrangler as wr
>>> description = wr.quicksight.describe_dataset(ingestion_id="...", dataset_name="...")
```

---

### get_dashboard_id

```python
wr.quicksight.get_dashboard_id(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get QuickSight dashboard ID given a name and fails if there is more than 1 ID associated with this name.

**Parameters**

- **`name`** — Dashboard name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dashboard ID.

**Examples**

```python
>>> import awswrangler as wr
>>> my_id = wr.quicksight.get_dashboard_id(name="...")
```

---

### get_dashboard_ids

```python
wr.quicksight.get_dashboard_ids(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Get QuickSight dashboard IDs given a name.

:::note
This function returns a list of ID because Quicksight accepts duplicated dashboard names,
so you may have more than 1 ID for a given name.
:::

**Parameters**

- **`name`** — Dashboard name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dashboard IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> ids = wr.quicksight.get_dashboard_ids(name="...")
```

---

### get_data_source_arn

```python
wr.quicksight.get_data_source_arn(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get QuickSight data source ARN given a name and fails if there is more than 1 ARN associated with this name.

:::note
This function returns a list of ARNs because Quicksight accepts duplicated data source names,
so you may have more than 1 ARN for a given name.
:::

**Parameters**

- **`name`** — Data source name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data source ARN.

**Examples**

```python
>>> import awswrangler as wr
>>> arn = wr.quicksight.get_data_source_arn("...")
```

---

### get_data_source_arns

```python
wr.quicksight.get_data_source_arns(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Get QuickSight Data source ARNs given a name.

:::note
This function returns a list of ARNs because Quicksight accepts duplicated data source names,
so you may have more than 1 ARN for a given name.
:::

**Parameters**

- **`name`** — Data source name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data source ARNs.

**Examples**

```python
>>> import awswrangler as wr
>>> arns = wr.quicksight.get_data_source_arns(name="...")
```

---

### get_data_source_id

```python
wr.quicksight.get_data_source_id(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get QuickSight data source ID given a name and fails if there is more than 1 ID associated with this name.

**Parameters**

- **`name`** — Data source name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dataset ID.

**Examples**

```python
>>> import awswrangler as wr
>>> my_id = wr.quicksight.get_data_source_id(name="...")
```

---

### get_data_source_ids

```python
wr.quicksight.get_data_source_ids(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Get QuickSight data source IDs given a name.

:::note
This function returns a list of ID because Quicksight accepts duplicated data source names,
so you may have more than 1 ID for a given name.
:::

**Parameters**

- **`name`** — Data source name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data source IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> ids = wr.quicksight.get_data_source_ids(name="...")
```

---

### get_dataset_id

```python
wr.quicksight.get_dataset_id(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get QuickSight Dataset ID given a name and fails if there is more than 1 ID associated with this name.

**Parameters**

- **`name`** — Dataset name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dataset ID.

**Examples**

```python
>>> import awswrangler as wr
>>> my_id = wr.quicksight.get_dataset_id(name="...")
```

---

### get_dataset_ids

```python
wr.quicksight.get_dataset_ids(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Get QuickSight dataset IDs given a name.

:::note
This function returns a list of ID because Quicksight accepts duplicated datasets names,
so you may have more than 1 ID for a given name.
:::

**Parameters**

- **`name`** — Dataset name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Datasets IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> ids = wr.quicksight.get_dataset_ids(name="...")
```

---

### get_template_id

```python
wr.quicksight.get_template_id(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get QuickSight template ID given a name and fails if there is more than 1 ID associated with this name.

**Parameters**

- **`name`** — Template name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Template ID.

**Examples**

```python
>>> import awswrangler as wr
>>> my_id = wr.quicksight.get_template_id(name="...")
```

---

### get_template_ids

```python
wr.quicksight.get_template_ids(
    name: 'str',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str]'
```

Get QuickSight template IDs given a name.

:::note
This function returns a list of ID because Quicksight accepts duplicated templates names,
so you may have more than 1 ID for a given name.
:::

**Parameters**

- **`name`** — Template name.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Template IDs.

**Examples**

```python
>>> import awswrangler as wr
>>> ids = wr.quicksight.get_template_ids(name="...")
```

---

### list_dashboards

```python
wr.quicksight.list_dashboards(
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List dashboards in an AWS account.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Dashboards.

**Examples**

```python
>>> import awswrangler as wr
>>> dashboards = wr.quicksight.list_dashboards()
```

---

### list_data_sources

```python
wr.quicksight.list_data_sources(
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all QuickSight Data sources summaries.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Data sources summaries.

**Examples**

```python
>>> import awswrangler as wr
>>> sources = wr.quicksight.list_data_sources()
```

---

### list_datasets

```python
wr.quicksight.list_datasets(
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all QuickSight datasets summaries.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Datasets summaries.

**Examples**

```python
>>> import awswrangler as wr
>>> datasets = wr.quicksight.list_datasets()
```

---

### list_groups

```python
wr.quicksight.list_groups(
    namespace: 'str' = 'default',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all QuickSight Groups.

**Parameters**

- **`namespace`** — The namespace. Currently, you should set this to default .
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Groups.

**Examples**

```python
>>> import awswrangler as wr
>>> groups = wr.quicksight.list_groups()
```

---

### list_group_memberships

```python
wr.quicksight.list_group_memberships(
    group_name: 'str',
    namespace: 'str' = 'default',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all QuickSight Group memberships.

**Parameters**

- **`group_name`** — The name of the group that you want to see a membership list of.
- **`namespace`** — The namespace. Currently, you should set this to default .
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Group memberships.

**Examples**

```python
>>> import awswrangler as wr
>>> memberships = wr.quicksight.list_group_memberships()
```

---

### list_iam_policy_assignments

```python
wr.quicksight.list_iam_policy_assignments(
    status: 'str | None' = None,
    namespace: 'str' = 'default',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List IAM policy assignments in the current Amazon QuickSight account.

**Parameters**

- **`status`** — The status of the assignments. 'ENABLED'|'DRAFT'|'DISABLED'
- **`namespace`** — The namespace. Currently, you should set this to default .
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- IAM policy assignments.

**Examples**

```python
>>> import awswrangler as wr
>>> assigns = wr.quicksight.list_iam_policy_assignments()
```

---

### list_iam_policy_assignments_for_user

```python
wr.quicksight.list_iam_policy_assignments_for_user(
    user_name: 'str',
    namespace: 'str' = 'default',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all the IAM policy assignments.

Including the Amazon Resource Names (ARNs) for the IAM policies assigned
to the specified user and group or groups that the user belongs to.

**Parameters**

- **`user_name`** — The name of the user.
- **`namespace`** — The namespace. Currently, you should set this to default .
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- IAM policy assignments.

**Examples**

```python
>>> import awswrangler as wr
>>> assigns = wr.quicksight.list_iam_policy_assignments_for_user()
```

---

### list_ingestions

```python
wr.quicksight.list_ingestions(
    dataset_name: 'str | None' = None,
    dataset_id: 'str | None' = None,
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List the history of SPICE ingestions for a dataset.

**Parameters**

- **`dataset_name`** — Dataset name.
- **`dataset_id`** — The ID of the dataset used in the ingestion.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- IAM policy assignments.

**Examples**

```python
>>> import awswrangler as wr
>>> ingestions = wr.quicksight.list_ingestions()
```

---

### list_templates

```python
wr.quicksight.list_templates(
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List all QuickSight templates.

**Parameters**

- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Templates summaries.

**Examples**

```python
>>> import awswrangler as wr
>>> templates = wr.quicksight.list_templates()
```

---

### list_users

```python
wr.quicksight.list_users(
    namespace: 'str' = 'default',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

Return a list of all of the Amazon QuickSight users belonging to this account.

**Parameters**

- **`namespace`** — The namespace. Currently, you should set this to default.
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Groups.

**Examples**

```python
>>> import awswrangler as wr
>>> users = wr.quicksight.list_users()
```

---

### list_user_groups

```python
wr.quicksight.list_user_groups(
    user_name: 'str',
    namespace: 'str' = 'default',
    account_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

List the Amazon QuickSight groups that an Amazon QuickSight user is a member of.

**Parameters**

- **`user_name`** — The Amazon QuickSight user name that you want to list group memberships for.
- **`namespace`** — The namespace. Currently, you should set this to default .
- **`account_id`** — If None, the account ID will be inferred from your boto3 session.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Groups.

**Examples**

```python
>>> import awswrangler as wr
>>> groups = wr.quicksight.list_user_groups()
```

---
