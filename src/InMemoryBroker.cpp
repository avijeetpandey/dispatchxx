#include "dispatchxx/InMemoryBroker.hpp"

#include <utility>

namespace dispatchxx {

InMemoryBroker::InMemoryBroker(BrokerConfig config) : config_(config) {}

InMemoryBroker::~InMemoryBroker() {
    stop_reaper();
    shutdown();
}

void InMemoryBroker::push_locked(Task task) {
    task.status = TaskStatus::Pending;
    queue_.push(Entry{std::move(task), next_sequence_++});
}

void InMemoryBroker::retry_or_dead_letter_locked(Task task) {
    // attempts was incremented on the claim that produced this task. Once it has
    // been delivered more times than retries allow, the task is poison.
    if (task.attempts > task.max_retries) {
        task.status = TaskStatus::DeadLettered;
        dead_letters_.push_back(std::move(task));
    } else {
        push_locked(std::move(task));
    }
}

void InMemoryBroker::enqueue(Task task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        push_locked(std::move(task));
    }
    not_empty_.notify_one();
}

std::optional<Task> InMemoryBroker::claim() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) {
        return std::nullopt;
    }

    Task task = std::move(const_cast<Entry&>(queue_.top()).task);
    queue_.pop();

    task.status = TaskStatus::InFlight;
    ++task.attempts;
    const TimePoint deadline = Clock::now() + config_.visibility_timeout;
    inflight_.emplace(task.id, InflightRecord{task, deadline});
    return task;
}

std::optional<Task> InMemoryBroker::claim_blocking() {
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, [this] { return !queue_.empty() || shutting_down_; });
    if (queue_.empty()) {
        return std::nullopt;
    }

    Task task = std::move(const_cast<Entry&>(queue_.top()).task);
    queue_.pop();

    task.status = TaskStatus::InFlight;
    ++task.attempts;
    const TimePoint deadline = Clock::now() + config_.visibility_timeout;
    inflight_.emplace(task.id, InflightRecord{task, deadline});
    return task;
}

bool InMemoryBroker::ack(const std::string& task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    return inflight_.erase(task_id) > 0;
}

bool InMemoryBroker::fail(const std::string& task_id) {
    bool requeued = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = inflight_.find(task_id);
        if (it == inflight_.end()) {
            return false;
        }
        Task task = std::move(it->second.task);
        inflight_.erase(it);
        const std::size_t before = queue_.size();
        retry_or_dead_letter_locked(std::move(task));
        requeued = queue_.size() > before;
    }
    if (requeued) {
        not_empty_.notify_one();
    }
    return true;
}

std::size_t InMemoryBroker::reap_expired() {
    std::size_t reaped = 0;
    std::size_t requeued = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const TimePoint now = Clock::now();
        for (auto it = inflight_.begin(); it != inflight_.end();) {
            if (it->second.deadline <= now) {
                Task task = std::move(it->second.task);
                it = inflight_.erase(it);
                const std::size_t before = queue_.size();
                retry_or_dead_letter_locked(std::move(task));
                requeued += queue_.size() - before;
                ++reaped;
            } else {
                ++it;
            }
        }
    }
    for (std::size_t i = 0; i < requeued; ++i) {
        not_empty_.notify_one();
    }
    return reaped;
}

void InMemoryBroker::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutting_down_ = true;
    }
    not_empty_.notify_all();
    reaper_cv_.notify_all();
}

std::vector<Task> InMemoryBroker::drain_dead_letters() {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::move(dead_letters_);
}

std::size_t InMemoryBroker::pending_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.size();
}

std::size_t InMemoryBroker::inflight_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return inflight_.size();
}

std::size_t InMemoryBroker::dead_letter_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dead_letters_.size();
}

void InMemoryBroker::start_reaper() {
    bool expected = false;
    if (!reaper_running_.compare_exchange_strong(expected, true)) {
        return;
    }
    reaper_thread_ = std::thread([this] {
        std::unique_lock<std::mutex> lock(mutex_);
        while (reaper_running_.load(std::memory_order_acquire) && !shutting_down_) {
            reaper_cv_.wait_for(lock, config_.reaper_interval);
            if (!reaper_running_.load(std::memory_order_acquire) || shutting_down_) {
                break;
            }
            lock.unlock();
            reap_expired();
            lock.lock();
        }
    });
}

void InMemoryBroker::stop_reaper() {
    if (!reaper_running_.exchange(false)) {
        return;
    }
    reaper_cv_.notify_all();
    if (reaper_thread_.joinable()) {
        reaper_thread_.join();
    }
}

}  // namespace dispatchxx
