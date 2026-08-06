---
id: aws-sts
title: "AWS STS"
sidebar_position: 21
---

# AWS STS

Module: `wr.sts`

### get_account_id

```python
wr.sts.get_account_id(boto3_session: 'boto3.Session | None' = None) -> 'str'
```

Get Account ID.

**Parameters**

- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- Account ID.

**Examples**

```python
>>> import awswrangler as wr
>>> account_id = wr.sts.get_account_id()
```

---

### get_current_identity_arn

```python
wr.sts.get_current_identity_arn(boto3_session: 'boto3.Session | None' = None) -> 'str'
```

Get current user/role ARN.

**Parameters**

- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- User/role ARN.

**Examples**

```python
>>> import awswrangler as wr
>>> arn = wr.sts.get_current_identity_arn()
```

---

### get_current_identity_name

```python
wr.sts.get_current_identity_name(boto3_session: 'boto3.Session | None' = None) -> 'str'
```

Get current user/role name.

**Parameters**

- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- User/role name.

**Examples**

```python
>>> import awswrangler as wr
>>> name = wr.sts.get_current_identity_name()
```

---
