---
id: aws-secrets-manager
title: "AWS Secrets Manager"
sidebar_position: 22
---

# AWS Secrets Manager

Module: `wr.secretsmanager`

### get_secret

```python
wr.secretsmanager.get_secret(name: 'str', boto3_session: 'boto3.Session | None' = None) -> 'str | bytes'
```

Get secret value.

**Parameters**

- **`name`** — Specifies the secret containing the version that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Secret value.

**Examples**

```python
>>> import awswrangler as wr
>>> value = wr.secretsmanager.get_secret("my-secret")
```

---

### get_secret_json

```python
wr.secretsmanager.get_secret_json(
    name: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, Any]'
```

Get JSON secret value.

**Parameters**

- **`name`** — Specifies the secret containing the version that you want to retrieve. You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Secret JSON value parsed as a dictionary.

**Examples**

```python
>>> import awswrangler as wr
>>> value = wr.secretsmanager.get_secret_json("my-secret-with-json-content")
```

---
