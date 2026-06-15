#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "dispatchxx/BrokerConfig.hpp"
#include "dispatchxx/InMemoryBroker.hpp"
#include "dispatchxx/TaskRegistry.hpp"
#include "dispatchxx/Task.hpp"
#include "dispatchxx/WorkerPool.hpp"

namespace dispatchxx {

// Options controlling how a single task is enqueued.
struct EnqueueOptions {
    int priority{0};
    int max_retries{0};
};

// High-level facade that wires together a broker, a handler registry, and a
// worker pool. Construct it, register handlers, start workers, then enqueue
// tasks. start() also launches the broker's visibility-timeout reaper.
class TaskQueueClient {
public:
    struct Config {
        std::size_t worker_count{4};
        BrokerConfig broker{};
    };

    TaskQueueClient() : TaskQueueClient(Config{}) {}
    explicit TaskQueueClient(Config config);
    ~TaskQueueClient();

    TaskQueueClient(const TaskQueueClient&) = delete;
    TaskQueueClient& operator=(const TaskQueueClient&) = delete;

    // Register the callable that processes tasks named `handler`.
    void register_handler(const std::string& handler, TaskRegistry::Handler fn);

    // Submit work. Returns the generated task id.
    std::string enqueue(const std::string& handler,
                        std::string payload,
                        EnqueueOptions options = {});

    // Submit a fully-formed task (e.g. with a caller-supplied id).
    void enqueue_task(Task task);

    // Launch the worker pool and the broker reaper. No-op if already started.
    void start();

    // Stop workers and reaper and join all threads. Idempotent; also called by
    // the destructor.
    void stop();

    // Drain and return the dead-letter queue.
    std::vector<Task> dead_letters();

    std::size_t pending_count() const { return broker_.pending_count(); }
    std::size_t inflight_count() const { return broker_.inflight_count(); }
    std::size_t dead_letter_count() const { return broker_.dead_letter_count(); }
    std::size_t processed_count() const { return pool_.processed_count(); }
    std::size_t failed_count() const { return pool_.failed_count(); }

    Broker& broker() { return broker_; }
    TaskRegistry& registry() { return registry_; }

private:
    InMemoryBroker broker_;
    TaskRegistry registry_;
    WorkerPool pool_;
    bool started_{false};
};

}  // namespace dispatchxx
