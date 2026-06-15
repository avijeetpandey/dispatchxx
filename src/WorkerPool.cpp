#include "dispatchxx/WorkerPool.hpp"

#include <algorithm>

namespace dispatchxx {

WorkerPool::WorkerPool(Broker& broker, TaskRegistry& registry, std::size_t worker_count)
    : broker_(broker),
      registry_(registry),
      worker_count_(std::max<std::size_t>(1, worker_count)) {}

WorkerPool::~WorkerPool() {
    stop();
}

void WorkerPool::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;
    }
    workers_.reserve(worker_count_);
    for (std::size_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back([this] { run(); });
    }
}

void WorkerPool::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    // Wake any worker blocked in claim_blocking() so they observe shutdown.
    broker_.shutdown();
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
}

void WorkerPool::run() {
    while (running_.load(std::memory_order_acquire)) {
        auto task = broker_.claim_blocking();
        if (!task.has_value()) {
            // Only happens on shutdown with an empty queue.
            break;
        }

        auto handler = registry_.find(task->handler);
        if (!handler.has_value()) {
            broker_.fail(task->id);
            failed_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        try {
            (*handler)(*task);
            broker_.ack(task->id);
            processed_.fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
            broker_.fail(task->id);
            failed_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

}  // namespace dispatchxx
