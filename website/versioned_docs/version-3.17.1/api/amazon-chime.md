---
id: amazon-chime
title: "Amazon Chime"
sidebar_position: 23
---

# Amazon Chime

Module: `wr.chime`

### post_message

```python
wr.chime.post_message(webhook: 'str', message: 'str') -> 'Any | None'
```

Send message on an existing Chime Chat rooms.

**Parameters**

- **`webhook`** — Contains all the authentication information to send the message
- **`message`** — The actual message which needs to be posted on Slack channel

**Returns**

- The response from Chime

---
