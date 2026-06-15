// Minimal end-to-end usage of the high-level TaskQueueClient: register handlers,
// start workers, enqueue work, and wait for completion.

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "dispatchxx/TaskQueueClient.hpp"

using namespace dispatchxx;
using namespace std::chrono_literals;

int main() {
    TaskQueueClient::Config config;
    config.worker_count = 4;

    TaskQueueClient client(config);

    std::mutex io_mutex;
    std::atomic<int> processed{0};

    client.register_handler("send_email", [&](const Task& task) {
        {
            std::lock_guard<std::mutex> lock(io_mutex);
            std::cout << "[send_email] worker handling task " << task.id
                      << " payload=\"" << task.payload << "\"\n";
        }
        std::this_thread::sleep_for(5ms);  // simulate IO
        processed.fetch_add(1, std::memory_order_relaxed);
    });

    client.register_handler("generate_report", [&](const Task& task) {
        {
            std::lock_guard<std::mutex> lock(io_mutex);
            std::cout << "[generate_report] priority=" << task.priority
                      << " payload=\"" << task.payload << "\"\n";
        }
        processed.fetch_add(1, std::memory_order_relaxed);
    });

    client.start();

    for (int i = 0; i < 10; ++i) {
        client.enqueue("send_email", "user" + std::to_string(i) + "@example.com");
    }

    // Higher priority is delivered first.
    client.enqueue("generate_report", "daily-summary", {.priority = 10, .max_retries = 2});

    constexpr int expected = 11;
    while (processed.load() < expected) {
        std::this_thread::sleep_for(1ms);
    }

    client.stop();

    std::cout << "\nDone. processed=" << client.processed_count()
              << " failed=" << client.failed_count()
              << " dead_letters=" << client.dead_letter_count() << "\n";
    return 0;
}
