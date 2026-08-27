---
id: distributed-ray
title: "Distributed - Ray"
sidebar_position: 27
---

# Distributed - Ray

Module: `wr.distributed.ray`

### initialize_ray

```python
wr.distributed.ray.initialize_ray(
    address: 'str | None' = None,
    redis_password: 'str | None' = None,
    ignore_reinit_error: 'bool' = True,
    include_dashboard: 'bool | None' = False,
    configure_logging: 'bool' = True,
    log_to_driver: 'bool' = False,
    logging_level: 'int' = 20,
    object_store_memory: 'int | None' = None,
    cpu_count: 'int | None' = None,
    gpu_count: 'int | None' = None
) -> 'None'
```

Connect to an existing Ray cluster or start one and connect to it.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- address

- redis_password

- ignore_reinit_error

- include_dashboard

- configure_logging

- log_to_driver

- logging_level

- object_store_memory

- cpu_count

- gpu_count

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`address`** — Address of the Ray cluster to connect to, by default None
- **`redis_password`** — Password to the Redis cluster, by default None
- **`ignore_reinit_error`** — If true, Ray suppress errors from calling ray.init() twice, by default True
- **`include_dashboard`** — Boolean flag indicating whether or not to start the Ray dashboard, by default False
- **`configure_logging`** — Boolean flag indicating whether or not to enable logging, by default True
- **`log_to_driver`** — Boolean flag to enable routing of all worker logs to the driver, by default False
- **`logging_level`** — Logging level, defaults to logging.INFO. Ignored unless "configure_logging" is True
- **`object_store_memory`** — The amount of memory (in bytes) to start the object store with, by default None
- **`cpu_count`** — Number of CPUs to assign to each raylet, by default None
- **`gpu_count`** — Number of GPUs to assign to each raylet, by default None

---
