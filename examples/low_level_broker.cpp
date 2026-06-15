// Demonstrates using the Broker directly, without the worker pool, for callers
// who want full control over the claim/ack lifecycle and the reaper.

#include <chrono>
#include <iostream>
#include <thread>

#include "dispatchxx/InMemoryBroker.hpp"

using namespace dispatchxx;
using namespace std::chrono_literals;

int main() {
    BrokerConfig config;
    config.visibility_timeout = 30ms;

    InMemoryBroker broker(config);

    for (int i = 0; i < 3; ++i) {
        Task task;
        task.id = "task-" + std::to_string(i);
        task.handler = "manual";
        task.payload = "payload-" + std::to_string(i);
        task.priority = i;  // higher index claimed first
        task.max_retries = 1;
        broker.enqueue(std::move(task));
    }

    std::cout << "pending=" << broker.pending_count() << "\n";

    // Claim one and ack it.
    if (auto task = broker.claim()) {
        std::cout << "claimed " << task->id << " (priority " << task->priority << ")\n";
        broker.ack(task->id);
        std::cout << "acked " << task->id << "\n";
    }

    // Claim one and let it time out to show reaping.
    if (auto task = broker.claim()) {
        std::cout << "claimed " << task->id << " but simulating a crash (no ack)\n";
        std::this_thread::sleep_for(50ms);
        const std::size_t reaped = broker.reap_expired();
        std::cout << "reaper requeued " << reaped << " task(s); pending="
                  << broker.pending_count() << "\n";
    }

    std::cout << "remaining pending=" << broker.pending_count()
              << " inflight=" << broker.inflight_count() << "\n";
    return 0;
}
