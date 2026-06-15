// Demonstrates fault tolerance: a flaky handler that fails the first time but
// succeeds on retry, and a poison handler that always throws and ends up in the
// dead-letter queue after exhausting its retries.

#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

#include "dispatchxx/TaskQueueClient.hpp"

using namespace dispatchxx;
using namespace std::chrono_literals;

int main() {
    TaskQueueClient::Config config;
    config.worker_count = 3;
    // Short timeouts so the demo finishes quickly.
    config.broker.visibility_timeout = 50ms;
    config.broker.reaper_interval = 10ms;

    TaskQueueClient client(config);

    std::mutex mutex;
    std::map<std::string, int> attempts;
    std::atomic<int> succeeded{0};

    client.register_handler("flaky", [&](const Task& task) {
        int n;
        {
            std::lock_guard<std::mutex> lock(mutex);
            n = ++attempts[task.id];
        }
        if (n == 1) {
            throw std::runtime_error("transient failure (will retry)");
        }
        std::cout << "[flaky] task " << task.id << " succeeded on attempt " << n << "\n";
        succeeded.fetch_add(1, std::memory_order_relaxed);
    });

    client.register_handler("poison", [](const Task& task) {
        throw std::runtime_error("permanent failure for " + task.id);
    });

    client.start();

    constexpr int kFlaky = 5;
    for (int i = 0; i < kFlaky; ++i) {
        client.enqueue("flaky", "job-" + std::to_string(i), {.priority = 0, .max_retries = 3});
    }
    client.enqueue("poison", "bad-job", {.priority = 0, .max_retries = 2});

    while (succeeded.load() < kFlaky || client.dead_letter_count() < 1) {
        std::this_thread::sleep_for(2ms);
    }

    client.stop();

    std::cout << "\nflaky succeeded=" << succeeded.load()
              << " processed=" << client.processed_count()
              << " failed=" << client.failed_count() << "\n";

    auto dlq = client.dead_letters();
    std::cout << "dead-letter queue has " << dlq.size() << " task(s):\n";
    for (const auto& task : dlq) {
        std::cout << "  id=" << task.id << " handler=" << task.handler
                  << " attempts=" << task.attempts << "\n";
    }
    return 0;
}
