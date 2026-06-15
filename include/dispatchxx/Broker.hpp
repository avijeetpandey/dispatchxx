#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "dispatchxx/Task.hpp"

namespace dispatchxx {

// Abstract transport for tasks. Implementations own queueing semantics
// (ordering, visibility, durability) and must be safe to call concurrently from
// many producer and consumer threads.
class Broker {
public:
    virtual ~Broker() = default;

    // Submit a task for delivery. Implementations order delivery by priority
    // then arrival time.
    virtual void enqueue(Task task) = 0;

    // Atomically remove and return the next deliverable task, or std::nullopt if
    // none are available. The claimed task is owned by the caller until ack() or
    // fail(), or until its visibility timeout elapses.
    virtual std::optional<Task> claim() = 0;

    // Like claim(), but blocks until a task is available or the broker is shut
    // down. Returns std::nullopt only when shut down with no task to deliver.
    virtual std::optional<Task> claim_blocking() = 0;

    // Confirm successful processing of a claimed task. Returns false if the id is
    // not currently in flight (already acked/failed/reaped or never claimed).
    virtual bool ack(const std::string& task_id) = 0;

    // Report failed processing of a claimed task. The task is requeued for retry,
    // or moved to the dead-letter queue once it has exhausted its retries.
    // Returns false if the id is not currently in flight.
    virtual bool fail(const std::string& task_id) = 0;

    // Move every in-flight task whose visibility timeout has elapsed back to the
    // queue (or to the DLQ if retries are exhausted). Returns the number moved.
    // Callers may invoke this directly; a running reaper also calls it.
    virtual std::size_t reap_expired() = 0;

    // Unblock all threads waiting in claim_blocking() and prevent further blocking
    // waits from sleeping. Idempotent.
    virtual void shutdown() = 0;

    // Remove and return all dead-lettered tasks, emptying the DLQ.
    virtual std::vector<Task> drain_dead_letters() = 0;

    virtual std::size_t pending_count() const = 0;
    virtual std::size_t inflight_count() const = 0;
    virtual std::size_t dead_letter_count() const = 0;
};

}  // namespace dispatchxx
