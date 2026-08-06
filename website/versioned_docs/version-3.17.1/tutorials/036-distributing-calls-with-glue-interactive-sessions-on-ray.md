---
id: 036-distributing-calls-with-glue-interactive-sessions-on-ray
title: "Distributing Calls with Glue Interactive Sessions on Ray"
sidebar_position: 36
sidebar_label: "36 - Distributing Calls with Glue Interactive Sessions on Ray"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/036%20-%20Distributing%20Calls%20with%20Glue%20Interactive%20Sessions%20on%20Ray.ipynb
---

# Distributing Calls with Glue Interactive Sessions on Ray

> This page is generated from [`tutorials/036 - Distributing Calls with Glue Interactive Sessions on Ray.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/036%20-%20Distributing%20Calls%20with%20Glue%20Interactive%20Sessions%20on%20Ray.ipynb). Open it in Jupyter to run it yourself.
AWS SDK for pandas is pre-loaded into [AWS Glue interactive sessions](https://docs.aws.amazon.com/glue/latest/dg/is-using-ray.html) with Ray kernel, making it by far the easiest way to experiment with the library at scale.

In AWS Glue Studio, choose `Notebook` to create an AWS Glue interactive session:

![](/img/tutorials/glue_is_create.png)

Then select `Ray` as the kernel. The IAM role must trust the AWS Glue service principal.

![](/img/tutorials/glue_is_setup.png)

Once the notebook is up and running you can import the library. You can install `awswrangler` and `modin` as additional dependencies.

```python
%additional_python_modules awswrangler,modin
```

```text
Additional python modules to be included:
awswrangler
modin
```

```python
import awswrangler as wr
```

```text
Authenticating with environment variables and user-defined glue_role_arn: arn:aws:iam::463623607974:role/service-role/AmazonSageMakerServiceCatalogProductsGlueRole
Trying to create a Glue session for the kernel.
Worker Type: Z.2X
Number of Workers: 5
Session ID: 32566e82-34d2-4db7-adac-cbee573e20bf
Job Type: glueray
Applying the following default arguments:
--glue_kernel_version 0.38.1
--enable-glue-datacatalog true
--auto-scaling-ray-min-workers 1
--additional-python-modules awswrangler,modin
Waiting for session 32566e82-34d2-4db7-adac-cbee573e20bf to get into ready status...
Session 32566e82-34d2-4db7-adac-cbee573e20bf has been created.
```

```python
df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2017/")
```

```python
df.head()
```

```text
  vendor_id           pickup_at  ... improvement_surcharge  total_amount
0         1 2017-01-09 11:13:28  ...                   0.3     15.300000
1         1 2017-01-09 11:32:27  ...                   0.3      7.250000
2         1 2017-01-09 11:38:20  ...                   0.3      7.300000
3         1 2017-01-09 11:52:13  ...                   0.3      8.500000
4         2 2017-01-01 00:00:00  ...                   0.3     52.799999

[5 rows x 17 columns]
```

<div class="alert alert-block alert-warning">
To avoid incurring a charge, make sure to delete the Jupyter Notebook when you are done experimenting.
</div>
