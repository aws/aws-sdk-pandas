---
id: amazon-emr-serverless
title: "Amazon EMR Serverless"
sidebar_position: 18
---

# Amazon EMR Serverless

Module: `wr.emr_serverless`

### create_application

```python
wr.emr_serverless.create_application(
    name: 'str',
    release_label: 'str',
    application_type: "Literal['Spark', 'Hive']" = 'Spark',
    initial_capacity: 'dict[str, str] | None' = None,
    maximum_capacity: 'dict[str, str] | None' = None,
    tags: 'dict[str, str] | None' = None,
    autostart: 'bool' = True,
    autostop: 'bool' = True,
    idle_timeout: 'int' = 15,
    network_configuration: 'dict[str, str] | None' = None,
    architecture: "Literal['ARM64', 'X86_64']" = 'X86_64',
    image_uri: 'str | None' = None,
    worker_type_specifications: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create an EMR Serverless application.

https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html


:::warning
This API is experimental and may change in future AWS SDK for Pandas releases.
:::

**Parameters**

- **`name`** — Name of EMR Serverless appliation
- **`release_label`** — Release label e.g. `emr-6.10.0`
- **`application_type`** — Application type: "Spark" or "Hive". Defaults to "Spark".
- **`initial_capacity`** — The capacity to initialize when the application is created.
- **`maximum_capacity`** — The maximum capacity to allocate when the application is created. This is cumulative across all workers at any given point in time, not just when an application is created. No new resources will be created once any one of the defined limits is hit.
- **`tags`** — Key/Value collection to put tags on the application. e.g. {"foo": "boo", "bar": "xoo"})
- **`autostart`** — Enables the application to automatically start on job submission. Defaults to true.
- **`autostop`** — Enables the application to automatically stop after a certain amount of time being idle. Defaults to true.
- **`idle_timeout`** — The amount of idle time in minutes after which your application will automatically stop. Defaults to 15 minutes.
- **`network_configuration`** — The network configuration for customer VPC connectivity.
- **`architecture`** — The CPU architecture of an application: "ARM64" or "X86_64". Defaults to "X86_64".
- **`image_uri`** — The URI of an image in the Amazon ECR registry.
- **`worker_type_specifications`** — The key-value pairs that specify worker type.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Application Id.

---

### run_job

```python
wr.emr_serverless.run_job(
    application_id: 'str',
    execution_role_arn: 'str',
    job_driver_args: 'dict[str, Any] | SparkSubmitJobArgs | HiveRunJobArgs',
    job_type: "Literal['Spark', 'Hive']" = 'Spark',
    wait: 'bool' = True,
    configuration_overrides: 'dict[str, Any] | None' = None,
    tags: 'dict[str, str] | None' = None,
    execution_timeout: 'int | None' = None,
    name: 'str | None' = None,
    emr_serverless_job_wait_polling_delay: 'float' = 5,
    boto3_session: 'boto3.Session | None' = None
) -> 'str | dict[str, Any]'
```

Run an EMR serverless job.

https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- emr_serverless_job_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::warning
This API is experimental and may change in future AWS SDK for Pandas releases.
:::

**Parameters**

- **`application_id`** — The id of the application on which to run the job.
- **`execution_role_arn`** — The execution role ARN for the job run.
- **`job_driver_args`** — The job driver arguments for the job run.
- **`job_type`** — Type of the job: "Spark" or "Hive". Defaults to "Spark".
- **`wait`** — Whether to wait for the job completion or not. Defaults to true.
- **`configuration_overrides`** — The configuration overrides for the job run.
- **`tags`** — Key/Value collection to put tags on the application. e.g. {"foo": "boo", "bar": "xoo"})
- **`execution_timeout`** — The maximum duration for the job run to run. If the job run runs beyond this duration, it will be automatically cancelled.
- **`name`** — Name of the job.
- **`emr_serverless_job_wait_polling_delay`** — Time to wait between polling attempts.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Job Id if wait=False, or job run details.

---

### wait_job

```python
wr.emr_serverless.wait_job(
    application_id: 'str',
    job_run_id: 'str',
    emr_serverless_job_wait_polling_delay: 'float' = 5,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Wait for the EMR Serverless job to finish.

https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- emr_serverless_job_wait_polling_delay

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::warning
This API is experimental and may change in future AWS SDK for Pandas releases.
:::

**Parameters**

- **`application_id`** — The id of the application on which the job is running.
- **`job_run_id`** — The id of the job.
- **`emr_serverless_job_wait_polling_delay`** — Time to wait between polling attempts.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Job run details.

---
