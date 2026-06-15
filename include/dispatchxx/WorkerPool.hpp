#pragma once

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "dispatchxx/Broker.hpp"
#include "dispatchxx/TaskRegistry.hpp"

namespace dispatchxx {

// Owns a fixed set of worker threads that pull tasks from a Broker and dispatch
// them through a TaskRegistry. A task with no registered handler, or whose
// handler throws, is reported as a failure so the broker can retry or
// dead-letter it. The pool does not own the broker or registry; their lifetime
// must outlive the pool.
class WorkerPool {
public:
    WorkerPool(Broker& broker, TaskRegistry& registry, std::size_t worker_count);
    ~WorkerPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    // Spawn the worker threads. No-op if already running.
    void start();

    // Signal the broker to stop, then join all workers. Idempotent; also called
    // by the destructor.
    void stop();

    std::size_t worker_count() const { return worker_count_; }

    // Tasks acked successfully since construction.
    std::size_t processed_count() const {
        return processed_.load(std::memory_order_relaxed);
    }

    // Tasks reported as failures (missing handler or handler exception) since
    // construction.
    std::size_t failed_count() const {
        return failed_.load(std::memory_order_relaxed);
    }

private:
    void run();

    Broker& broker_;
    TaskRegistry& registry_;
    std::size_t worker_count_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
};

}  // namespace dispatchxx
