# 9. Engine selection and lazy initialization

Date: 2023-05-17

## Status

Accepted

## Context

In distributed mode, three approaches are possible when it comes to selecting and initializing a Ray engine:
1. Initialize the Ray runtime at import (current default). This option causes the least friction to the user but assumes that installing Ray as an optional dependency is enough to enable distributed mode. Moreover, the user cannot prevent/delay Ray initialization (as it's done at import)
2. Initialize the Ray runtime on the first distributed API call. The user can prevent Ray initialization by switching the engine/memory format with environment variables or between import and the first awswrangler distributed API call. However, by default this approach still assumes that installing Ray is equivalent to enabling distributed mode
3. Wait for the user to enable distributed mode, via environment variables and/or via `wr.engine.set`. This option makes no assumption on which mode to use (distributed vs non-distributed). Non-distributed would be the default and it's up to the user to switch the engine/memory format

## Decision

Option #1 is inflexible and gives little control to the user, while option #3 introduces too much friction and puts the burden on the user. Option #2 on the other hand gives full flexibility to the user while providing a sane default.

## Consequences

The only difference between the current default and the suggested approach is to delay engine initialization, which is not a breaking change. However, it means that in certain situations more than one Ray instance is initialized. For instance, when running tests across multiple threads, each thread runs its own Ray runtime.
