"""Offline tests for ``awswrangler._utils.retry`` and ``awswrangler._utils.try_it``."""

import pytest

from awswrangler import _utils


class _CodedError(Exception):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class _Sleeps(list):  # type: ignore[type-arg]
    def __init__(self) -> None:
        super().__init__()
        self.uniform_calls: list[tuple[float, float]] = []


@pytest.fixture
def sleeps(monkeypatch):
    """Capture time.sleep calls and make random.uniform deterministic (always the upper bound)."""
    recorded = _Sleeps()
    uniform_calls = recorded.uniform_calls

    def fake_uniform(a: float, b: float) -> float:
        uniform_calls.append((a, b))
        return b

    monkeypatch.setattr(_utils.time, "sleep", recorded.append)
    monkeypatch.setattr(_utils.random, "uniform", fake_uniform)
    return recorded


def test_try_it_returns_result_without_sleeping(sleeps):
    assert _utils.try_it(lambda: 42, ValueError) == 42
    assert sleeps == []


def test_try_it_forwards_args_and_kwargs(sleeps):
    def f(a, b, c=0, d=0):
        return (a, b, c, d)

    assert _utils.try_it(f, ValueError, 1, 2, c=3, d=4) == (1, 2, 3, 4)


def test_try_it_does_not_swallow_option_names_as_kwargs(sleeps):
    def f(**kwargs):
        return kwargs

    assert _utils.try_it(f, ValueError, extra=1, base=2.0, max_num_tries=2) == {"extra": 1}


def test_try_it_retries_until_success(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError("boom")
        return "ok"

    assert _utils.try_it(f, ValueError, max_num_tries=3) == "ok"
    assert len(attempts) == 3
    assert len(sleeps) == 2


def test_try_it_raises_after_max_num_tries(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        raise ValueError(len(attempts))

    with pytest.raises(ValueError) as exc:
        _utils.try_it(f, ValueError, max_num_tries=4)
    assert len(attempts) == 4
    assert exc.value.args == (4,)
    assert len(sleeps) == 3


def test_try_it_single_try_does_not_sleep(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        raise ValueError

    with pytest.raises(ValueError):
        _utils.try_it(f, ValueError, max_num_tries=1)
    assert len(attempts) == 1
    assert sleeps == []


def test_try_it_default_max_num_tries_is_three(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        raise ValueError

    with pytest.raises(ValueError):
        _utils.try_it(f, ValueError)
    assert len(attempts) == 3


def test_try_it_unlisted_exception_is_not_retried(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        raise KeyError("x")

    with pytest.raises(KeyError):
        _utils.try_it(f, ValueError, max_num_tries=5)
    assert len(attempts) == 1
    assert sleeps == []


def test_try_it_accepts_tuple_of_exceptions(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        if len(attempts) == 1:
            raise KeyError("x")
        if len(attempts) == 2:
            raise ValueError("y")
        return "ok"

    assert _utils.try_it(f, (KeyError, ValueError), max_num_tries=3) == "ok"


def test_try_it_ex_code_mismatch_is_raised_immediately(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        raise _CodedError("Other")

    with pytest.raises(_CodedError):
        _utils.try_it(f, _CodedError, ex_code="Throttling", max_num_tries=5)
    assert len(attempts) == 1
    assert sleeps == []


def test_try_it_ex_code_match_is_retried(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        if len(attempts) < 2:
            raise _CodedError("Throttling")
        return "ok"

    assert _utils.try_it(f, _CodedError, ex_code="Throttling", max_num_tries=3) == "ok"
    assert len(attempts) == 2
    assert len(sleeps) == 1


def test_try_it_ex_code_without_response_attribute_is_retried(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        if len(attempts) < 2:
            raise ValueError("no response attribute")
        return "ok"

    assert _utils.try_it(f, ValueError, ex_code="Throttling", max_num_tries=3) == "ok"
    assert len(attempts) == 2


def test_try_it_ex_code_none_retries_regardless_of_code(sleeps):
    attempts = []

    def f():
        attempts.append(1)
        if len(attempts) < 2:
            raise _CodedError("Anything")
        return "ok"

    assert _utils.try_it(f, _CodedError, max_num_tries=3) == "ok"


def test_try_it_decorrelated_jitter_bounds(sleeps):
    def f():
        raise ValueError

    with pytest.raises(ValueError):
        _utils.try_it(f, ValueError, base=2.0, max_num_tries=4)
    # delay starts at ``base`` and each subsequent upper bound is three times the previous delay
    assert sleeps.uniform_calls == [(2.0, 6.0), (2.0, 18.0), (2.0, 54.0)]
    assert sleeps == [6.0, 18.0, 54.0]


def test_try_it_default_base_is_one(sleeps):
    def f():
        raise ValueError

    with pytest.raises(ValueError):
        _utils.try_it(f, ValueError, max_num_tries=2)
    assert sleeps.uniform_calls == [(1.0, 3.0)]


def test_try_it_real_jitter_stays_within_bounds(monkeypatch):
    delays: list[float] = []
    monkeypatch.setattr(_utils.time, "sleep", delays.append)

    def f():
        raise ValueError

    with pytest.raises(ValueError):
        _utils.try_it(f, ValueError, base=0.5, max_num_tries=5)
    assert len(delays) == 4
    for k, delay in enumerate(delays, start=1):
        assert 0.5 <= delay <= 0.5 * 3**k


def test_retry_decorator_retries_and_returns(sleeps):
    attempts = []

    @_utils.retry(ex=ValueError, max_num_tries=3)
    def f(x, y=1):
        attempts.append(1)
        if len(attempts) < 3:
            raise ValueError
        return x + y

    assert f(1, y=2) == 3
    assert len(attempts) == 3


def test_retry_decorator_preserves_metadata(sleeps):
    @_utils.retry(ex=ValueError)
    def my_function():
        """My docstring."""

    assert my_function.__name__ == "my_function"
    assert my_function.__doc__ == "My docstring."


def test_retry_decorator_passes_configuration(sleeps):
    attempts = []

    @_utils.retry(ex=_CodedError, ex_code="Throttling", base=5.0, max_num_tries=2)
    def f():
        attempts.append(1)
        raise _CodedError("Throttling")

    with pytest.raises(_CodedError):
        f()
    assert len(attempts) == 2
    assert sleeps.uniform_calls == [(5.0, 15.0)]


def test_retry_decorator_ex_code_mismatch_not_retried(sleeps):
    attempts = []

    @_utils.retry(ex=_CodedError, ex_code="Throttling", max_num_tries=5)
    def f():
        attempts.append(1)
        raise _CodedError("Other")

    with pytest.raises(_CodedError):
        f()
    assert len(attempts) == 1


def test_retry_decorator_defaults(sleeps):
    attempts = []

    @_utils.retry(ex=ValueError)
    def f():
        attempts.append(1)
        raise ValueError

    with pytest.raises(ValueError):
        f()
    assert len(attempts) == 3
    assert sleeps.uniform_calls == [(1.0, 3.0), (1.0, 9.0)]
