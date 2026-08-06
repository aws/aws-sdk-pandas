---
id: global-configurations
title: "Global Configurations"
sidebar_position: 25
---

# Global Configurations

Module: `wr.config`

### reset

```python
wr.config.reset(item: 'str | None' = None) -> 'None'
```

Reset one or all (if None is received) configuration values.

**Parameters**

- **`item`** — Configuration item name.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.config.reset("database")  # Reset one specific configuration
>>> wr.config.reset()  # Reset all
```

---

### to_pandas

```python
wr.config.to_pandas() -> 'pd.DataFrame'
```

Load all configurations on a Pandas DataFrame.

**Returns**

- Configuration DataFrame.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.config.to_pandas()
```

---
