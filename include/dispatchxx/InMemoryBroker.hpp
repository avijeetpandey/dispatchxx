#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "dispatchxx/Broker.hpp"
#include "dispatchxx/BrokerConfig.hpp"
#include "dispatchxx/Task.hpp"

namespace dispatchxx {

// In-process Broker backed by a priority heap guarded by a single mutex, with a
// condition variable for blocking consumers, a visibility-timeout reaper, and a
// dead-letter queue. Locking is coarse but every public operation is atomic with
// respect to concurrent callers.
class InMemoryBroker : public Broker {
public:
    InMemoryBroker() = default;
    explicit InMemoryBroker(BrokerConfig config);
    ~InMemoryBroker() override;

    InMemoryBroker(const InMemoryBroker&) = delete;
    InMemoryBroker& operator=(const InMemoryBroker&) = delete;

    void enqueue(Task task) override;
    std::optional<Task> claim() override;
    std::optional<Task> claim_blocking() override;
    bool ack(const std::string& task_id) override;
    bool fail(const std::string& task_id) override;
    std::size_t reap_expired() override;
    void shutdown() override;
    std::vector<Task> drain_dead_letters() override;

    std::size_t pending_count() const override;
    std::size_t inflight_count() const override;
    std::size_t dead_letter_count() const override;

    // Start a background thread that periodically calls reap_expired(). Safe to
    // call once; subsequent calls are no-ops while a reaper is running.
    void start_reaper();
    void stop_reaper();

private:
    // Wraps a Task with a monotonically increasing sequence so ties in priority
    // resolve to strict FIFO, which steady_clock timestamps alone cannot
    // guarantee at sub-tick resolution.
    struct Entry {
        Task task;
        std::uint64_t sequence;
    };

    struct EntryCompare {
        bool operator()(const Entry& a, const Entry& b) const {
            if (a.task.priority != b.task.priority) {
                return a.task.priority < b.task.priority;
            }
            return a.sequence > b.sequence;
        }
    };

    struct InflightRecord {
        Task task;
        TimePoint deadline;
    };

    // Push an already-prepared task onto the ready queue. Caller holds mutex_.
    void push_locked(Task task);

    // Route a no-longer-deliverable task to retry or the DLQ based on attempts vs
    // max_retries. Caller holds mutex_.
    void retry_or_dead_letter_locked(Task task);

    BrokerConfig config_{};

    mutable std::mutex mutex_;
    std::condition_variable not_empty_;   // Wakes consumers blocked in claim_blocking().
    std::condition_variable reaper_cv_;   // Wakes the reaper for its interval/shutdown.
    std::priority_queue<Entry, std::vector<Entry>, EntryCompare> queue_;
    std::unordered_map<std::string, InflightRecord> inflight_;
    std::vector<Task> dead_letters_;
    std::uint64_t next_sequence_{0};
    bool shutting_down_{false};

    std::thread reaper_thread_;
    std::atomic<bool> reaper_running_{false};
};

}  // namespace dispatchxx
