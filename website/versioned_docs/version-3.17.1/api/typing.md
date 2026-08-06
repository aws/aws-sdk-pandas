---
id: typing
title: "Typing"
sidebar_position: 24
---

# Typing

Module: `wr.typing`

### GlueTableSettings

```python
wr.typing.GlueTableSettings(...)
```

Typed dictionary defining the settings for the Glue table.

---

### AthenaCTASSettings

```python
wr.typing.AthenaCTASSettings(...)
```

Typed dictionary defining the settings for using CTAS (Create Table As Statement).

---

### AthenaUNLOADSettings

```python
wr.typing.AthenaUNLOADSettings(...)
```

Typed dictionary defining the settings for using UNLOAD.

---

### AthenaCacheSettings

```python
wr.typing.AthenaCacheSettings(...)
```

Typed dictionary defining the settings for using cached Athena results.

---

### AthenaPartitionProjectionSettings

```python
wr.typing.AthenaPartitionProjectionSettings(...)
```

Typed dictionary defining the settings for Athena Partition Projection.

https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html

---

### TimestreamBatchLoadReportS3Configuration

```python
wr.typing.TimestreamBatchLoadReportS3Configuration(...)
```

Report configuration for a batch load task. This contains details about where error reports are stored.

https://docs.aws.amazon.com/timestream/latest/developerguide/API_ReportS3Configuration.html

---

### ArrowDecryptionConfiguration

```python
wr.typing.ArrowDecryptionConfiguration(...)
```

Configuration for Arrow file decrypting.

---

### ArrowEncryptionConfiguration

```python
wr.typing.ArrowEncryptionConfiguration(...)
```

Configuration for Arrow file encrypting.

---

### RaySettings

```python
wr.typing.RaySettings(...)
```

Typed dictionary defining the settings for distributing calls using Ray.

---

### RayReadParquetSettings

```python
wr.typing.RayReadParquetSettings(...)
```

Typed dictionary defining the settings for distributing reading calls using Ray.

---

### _S3WriteDataReturnValue

```python
wr.typing._S3WriteDataReturnValue(...)
```

Typed dictionary defining the dictionary returned by S3 write functions.

---

### _ReadTableMetadataReturnValue

```python
wr.typing._ReadTableMetadataReturnValue(
    columns_types: ForwardRef('dict[str, str]'),
    partitions_types: ForwardRef('dict[str, str] | None')
)
```

Named tuple defining the return value of the `read_*_metadata` functions.

---
