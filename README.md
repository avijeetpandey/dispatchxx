# dispatchxx

A production-grade, resilient, in-process distributed-style job/task queue written
in modern C++20. It provides priority scheduling, a thread-safe worker pool,
visibility timeouts with automatic re-delivery, and a dead-letter queue — the core
building blocks of systems like Sidekiq, Celery, or Amazon SQS, implemented as a
single embeddable library.

## Features

- **Priority + FIFO scheduling** — higher-priority tasks are claimed first; ties
  break in strict arrival order.
- **Thread-safe broker** — concurrent producers and consumers with exactly-once
  claim semantics (no loss, no duplication).
- **Worker pool** — a configurable number of `std::thread` workers that block
  efficiently on a condition variable when idle.
- **Handler registry** — map string names (e.g. `"send_email"`) to C++ callables.
- **Visibility timeout + reaper** — a claimed-but-unacked task (e.g. from a crashed
  worker) is automatically re-queued after its timeout.
- **Retries + dead-letter queue** — tasks that keep failing are moved to a DLQ once
  they exhaust `max_retries`.
- **Verified under ThreadSanitizer** — the full test suite runs clean under TSAN.

## Architecture

```
                 enqueue()                claim_blocking()
   Producers ───────────────►  Broker  ◄──────────────── WorkerPool (N threads)
                               │  │  │                        │
        priority heap (ready)  │  │  └── dead-letter queue    │ dispatch by name
        in-flight map  ────────┘  │                           ▼
        (id → deadline)           │                      TaskRegistry
                                  │                    (name → std::function)
                   Reaper thread ─┘  requeue expired / dead-letter poison
```

- **`Task`** — a value type (id, handler name, payload, priority, retry policy,
  status, attempts, enqueue time). Copied freely across thread boundaries.
- **`Broker`** — abstract transport. `InMemoryBroker` is the provided
  implementation: a `std::priority_queue` guarded by a mutex, a condition variable
  for blocking consumers, an in-flight map keyed by task id with per-task
  deadlines, a background reaper thread, and a dead-letter queue.
- **`TaskRegistry`** — thread-safe (`std::shared_mutex`) map of handler name →
  `std::function<void(const Task&)>`.
- **`WorkerPool`** — owns the worker threads. Each worker claims a task, looks up
  its handler, invokes it, then `ack`s on success or `fail`s on exception / missing
  handler.
- **`TaskQueueClient`** — a façade that wires a broker, registry, and worker pool
  together behind a small API.

### Delivery & retry semantics

`attempts` is incremented every time a task is claimed. A task is delivered up to
`max_retries + 1` times. Once `attempts > max_retries` and it fails or times out
again, it is moved to the dead-letter queue instead of being re-queued. A
successful `ack` removes it from the in-flight set permanently.

The reaper provides **at-least-once** delivery: if a worker crashes after claiming
a task but before acking, the visibility timeout elapses and the reaper re-queues
the task for another worker. Handlers should therefore be idempotent.

## Requirements

- A C++20 compiler (tested with Apple Clang 21).
- CMake ≥ 3.20.
- GoogleTest is fetched automatically by CMake (`FetchContent`) for the test build.

## Building

```bash
cmake -S . -B build
cmake --build build -j
```

Run the test suite:

```bash
ctest --test-dir build --output-on-failure
```

Run the examples:

```bash
./build/examples/example_basic_usage
./build/examples/example_resilience
./build/examples/example_low_level
```

### Build options

| Option                        | Default | Description                          |
| ----------------------------- | ------- | ------------------------------------ |
| `DISPATCHXX_BUILD_TESTS`      | `ON`    | Build the GoogleTest suite.          |
| `DISPATCHXX_BUILD_EXAMPLES`   | `ON`    | Build the example programs.          |
| `DISPATCHXX_SANITIZE_THREAD`  | `OFF`   | Build with ThreadSanitizer.          |

### Running under ThreadSanitizer

```bash
cmake -S . -B build-tsan -DDISPATCHXX_SANITIZE_THREAD=ON -DDISPATCHXX_BUILD_EXAMPLES=OFF
cmake --build build-tsan -j
./build-tsan/tests/dispatchxx_tests
```

## Usage

### High-level API (`TaskQueueClient`)

```cpp
#include "dispatchxx/TaskQueueClient.hpp"

using namespace dispatchxx;

int main() {
    TaskQueueClient::Config config;
    config.worker_count = 4;
    // Optional: tune resilience timings.
    config.broker.visibility_timeout = std::chrono::seconds(30);
    config.broker.reaper_interval    = std::chrono::seconds(1);

    TaskQueueClient client(config);

    // 1. Register a handler by name.
    client.register_handler("send_email", [](const Task& task) {
        // task.payload holds whatever you enqueued.
        send_email(task.payload);
    });

    // 2. Start the worker pool (and the reaper).
    client.start();

    // 3. Enqueue work. Returns the generated task id.
    std::string id = client.enqueue(
        "send_email",
        "user@example.com",
        EnqueueOptions{.priority = 5, .max_retries = 3});

    // ... do other work; workers process tasks in the background ...

    // 4. Shut down cleanly (also called by the destructor).
    client.stop();

    // Inspect anything that ended up dead-lettered.
    for (const Task& dead : client.dead_letters()) {
        log_failure(dead.id, dead.handler, dead.attempts);
    }
}
```

### Enqueueing a fully-formed task

```cpp
Task task;
task.id          = "order-4711";       // caller-supplied id
task.handler     = "process_order";
task.payload     = R"({"order_id":4711})";
task.priority    = 10;
task.max_retries = 5;
client.enqueue_task(std::move(task));
```

### Low-level API (`InMemoryBroker` directly)

For callers who want to manage the claim/ack lifecycle themselves:

```cpp
#include "dispatchxx/InMemoryBroker.hpp"

using namespace dispatchxx;

BrokerConfig config;
config.visibility_timeout = std::chrono::seconds(10);

InMemoryBroker broker(config);
broker.start_reaper();   // optional background re-delivery

Task t;
t.id = "job-1";
t.handler = "noop";
t.max_retries = 2;
broker.enqueue(std::move(t));

if (auto claimed = broker.claim()) {          // or claim_blocking()
    try {
        do_work(*claimed);
        broker.ack(claimed->id);              // success
    } catch (...) {
        broker.fail(claimed->id);             // retry or dead-letter
    }
}

broker.stop_reaper();
```

## Project layout

```
include/dispatchxx/   Public headers
  Task.hpp            Domain model + TaskStatus
  Broker.hpp          Abstract broker interface
  BrokerConfig.hpp    Visibility-timeout / reaper tunables
  InMemoryBroker.hpp  Thread-safe broker implementation
  TaskRegistry.hpp    Name → handler registry
  WorkerPool.hpp      Thread pool that drains the broker
  TaskQueueClient.hpp High-level façade
src/                  Implementations
tests/                GoogleTest suite (broker, registry, pool, resilience, client)
examples/             Runnable example programs
```

## Testing strategy

The suite covers each layer independently and together:

- **Broker** — priority ordering, FIFO tie-breaking, ack semantics, and a stress
  test with 10 producers / 5 consumers / 1000 tasks asserting exactly-once
  delivery.
- **Registry** — registration, replacement, lookup, and invocation.
- **Worker pool** — full pipeline throughput, missing-handler failures, and a
  10-producer / 5-consumer / 1000-task concurrency test.
- **Resilience** — visibility-timeout reaping, retry-then-dead-letter routing,
  explicit failure handling, background-reaper re-assignment of "crashed" workers,
  and end-to-end retry/poison handling through the worker pool.
- **Client** — end-to-end processing, unique id generation, priority ordering, and
  DLQ surfacing.

All tests pass cleanly under ThreadSanitizer.