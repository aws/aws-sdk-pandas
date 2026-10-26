from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

import pytest

import awswrangler as wr

pytest.importorskip("gremlin_python")


def _connect_and_capture_call_from_event_loop() -> bool:
    with patch("awswrangler.neptune._gremlin_init.Client") as client_cls:
        client_cls.return_value = MagicMock()
        wr.neptune.connect("localhost", 8182, iam_enabled=False)._get_gremlin_connection()
    return client_cls.call_args.kwargs["call_from_event_loop"]


def test_gremlin_client_does_not_request_nest_asyncio_outside_event_loop() -> None:
    assert _connect_and_capture_call_from_event_loop() is False


def test_gremlin_client_requests_nest_asyncio_inside_running_event_loop() -> None:
    async def inside_loop() -> bool:
        return _connect_and_capture_call_from_event_loop()

    assert asyncio.run(inside_loop()) is True
