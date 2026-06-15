#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "dispatchxx/TaskQueueClient.hpp"

using namespace dispatchxx;
using namespace std::chrono_literals;

namespace {

template <typename Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

}  // namespace

TEST(TaskQueueClient, EndToEndProcessing) {
    TaskQueueClient::Config config;
    config.worker_count = 4;

    TaskQueueClient client(config);
    std::atomic<int> total{0};
    client.register_handler("add", [&](const Task& t) {
        total.fetch_add(std::stoi(t.payload), std::memory_order_relaxed);
    });

    client.start();

    int expected = 0;
    for (int i = 1; i <= 50; ++i) {
        expected += i;
        client.enqueue("add", std::to_string(i));
    }

    ASSERT_TRUE(wait_until([&] { return client.processed_count() == 50; }));
    client.stop();

    EXPECT_EQ(total.load(), expected);
    EXPECT_EQ(client.pending_count(), 0u);
    EXPECT_EQ(client.inflight_count(), 0u);
}

TEST(TaskQueueClient, EnqueueReturnsUniqueIds) {
    TaskQueueClient client;
    const std::string a = client.enqueue("h", "1");
    const std::string b = client.enqueue("h", "2");
    EXPECT_NE(a, b);
    EXPECT_EQ(client.pending_count(), 2u);
}

TEST(TaskQueueClient, PriorityOrderingHonored) {
    TaskQueueClient::Config config;
    config.worker_count = 1;
    TaskQueueClient client(config);

    std::vector<std::string> order;
    std::mutex mutex;
    client.register_handler("rec", [&](const Task& t) {
        std::lock_guard<std::mutex> lock(mutex);
        order.push_back(t.payload);
    });

    client.enqueue("rec", "low", {.priority = 1});
    client.enqueue("rec", "high", {.priority = 10});
    client.enqueue("rec", "mid", {.priority = 5});

    client.start();
    ASSERT_TRUE(wait_until([&] { return client.processed_count() == 3; }));
    client.stop();

    ASSERT_EQ(order.size(), 3u);
    EXPECT_EQ(order[0], "high");
    EXPECT_EQ(order[1], "mid");
    EXPECT_EQ(order[2], "low");
}

TEST(TaskQueueClient, DeadLetterSurfacedThroughClient) {
    TaskQueueClient::Config config;
    config.worker_count = 2;
    config.broker.visibility_timeout = 20ms;
    config.broker.reaper_interval = 5ms;

    TaskQueueClient client(config);
    client.register_handler("boom",
                            [](const Task&) { throw std::runtime_error("boom"); });
    client.start();

    client.enqueue("boom", "payload", {.priority = 0, .max_retries = 1});

    ASSERT_TRUE(wait_until([&] { return client.dead_letter_count() == 1; }));
    client.stop();

    auto dlq = client.dead_letters();
    ASSERT_EQ(dlq.size(), 1u);
    EXPECT_EQ(dlq.front().handler, "boom");
}
