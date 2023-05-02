# 7. Design of engine and memory format

Date: 2023-03-16

## Status

Accepted

## Context

Ray and Modin are the two frameworks used to support running `awswrangler` APIs at scale. Adding them to the codebase requires significant refactoring work. The original approach considered was to handle both distributed and non-distributed code within the same modules. This quickly turned out to be undesirable as it affected the readability, maintainability and scalability of the codebase.

## Decision

Version 3.x of the library introduces two new constructs, `engine` and `memory_format`, which are designed to address the aforementioned shortcomings of the original approach, but also provide additional functionality.

Currently `engine` takes one of two values: `python` (default) or `ray`, but additional engines could be onboarded in the future. The value is determined at import based on installed dependencies. The user can override this value with `wr.engine.set("engine_name")`. Likewise, `memory_format` can be set to `pandas` (default) or `modin` and overridden with `wr.memory_format.set("memory_format_name")`.

A custom dispatcher is used to register functions based on the execution and memory format values. For instance, if the `ray` engine is detected at import, then methods distributed with Ray are used instead of the default AWS SDK for pandas code.

## Consequences

__The good__:

*Clear separation of concerns*: Distributed methods live outside non-distributed code, eliminating ugly if conditionals, allowing both to scale independently and making them easier to maintain in the future

*Better dispatching*: Adding a new engine/memory format is as simple as creating a new directory with its methods and registering them with the custom dispatcher based on the value of the engine or memory format

*Custom engine/memory format classes*: Give more flexibility than config when it comes to interacting with the engine and managing its state (initialising, registering, get/setting...)

__The bad__:

*Managing state*: Adding a custom dispatcher means that we must maintain its state. For instance, unregistering methods when a user sets a different engine (e.g. moving from ray to dask at execution time) is currently unsupported

*Detecting the engine*: Conditionals are simpler/easier when it comes to detecting an engine. With a custom dispatcher, the registration and dispatching process is more opaque/convoluted. For example, there is a higher risk of not realising that we are using a given engine vs another

__The ugly__:

*Unused arguments*: Each method registered with the dispatcher must accept the union of both non-distributed and distributed arguments, even though some would be unused. As the list of supported engines grows, so does the number of unused arguments. It also means that we must maintain the same list of arguments across the different versions of the method