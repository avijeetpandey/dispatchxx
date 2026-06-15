#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "dispatchxx/InMemoryBroker.hpp"
#include "dispatchxx/TaskRegistry.hpp"
#include "dispatchxx/WorkerPool.hpp"

using namespace dispatchxx;
using namespace std::chrono_literals;

namespace {

// Spin until predicate holds or the deadline passes, to avoid flaky fixed sleeps.
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

TEST(WorkerPool, ProcessesAllEnqueuedTasks) {
    InMemoryBroker broker;
    TaskRegistry registry;
    std::atomic<int> sum{0};

    registry.register_handler("add", [&](const Task& t) {
        sum.fetch_add(std::stoi(t.payload), std::memory_order_relaxed);
    });

    WorkerPool pool(broker, registry, 4);
    pool.start();

    int expected = 0;
    for (int i = 1; i <= 100; ++i) {
        expected += i;
        Task t;
        t.id = "t" + std::to_string(i);
        t.handler = "add";
        t.payload = std::to_string(i);
        broker.enqueue(std::move(t));
    }

    ASSERT_TRUE(wait_until([&] { return pool.processed_count() == 100; }));
    pool.stop();

    EXPECT_EQ(sum.load(), expected);
    EXPECT_EQ(broker.pending_count(), 0u);
    EXPECT_EQ(broker.inflight_count(), 0u);
}

TEST(WorkerPool, MissingHandlerCountsAsFailure) {
    InMemoryBroker broker;
    TaskRegistry registry;

    WorkerPool pool(broker, registry, 2);
    pool.start();

    Task t;
    t.id = "orphan";
    t.handler = "does_not_exist";
    t.max_retries = 0;  // straight to DLQ after first failure
    broker.enqueue(std::move(t));

    ASSERT_TRUE(wait_until([&] { return broker.dead_letter_count() == 1; }));
    pool.stop();

    EXPECT_GE(pool.failed_count(), 1u);
    auto dlq = broker.drain_dead_letters();
    ASSERT_EQ(dlq.size(), 1u);
    EXPECT_EQ(dlq.front().id, "orphan");
}

// 10 producers and 5 consumers move 1000 tasks with no loss, no duplication,
// and no data races on the shared accumulator.
TEST(WorkerPool, TenProducersFiveConsumersThousandTasks) {
    constexpr int kProducers = 10;
    constexpr int kPerProducer = 100;
    constexpr int kTotal = kProducers * kPerProducer;

    InMemoryBroker broker;
    TaskRegistry registry;

    std::mutex seen_mutex;
    std::unordered_set<std::string> seen;

    registry.register_handler("collect", [&](const Task& t) {
        std::lock_guard<std::mutex> lock(seen_mutex);
        seen.insert(t.id);
    });

    WorkerPool pool(broker, registry, 5);
    pool.start();

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&broker, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                Task t;
                t.id = "p" + std::to_string(p) + "_" + std::to_string(i);
                t.handler = "collect";
                t.priority = i % 5;
                broker.enqueue(std::move(t));
            }
        });
    }
    for (auto& t : producers) t.join();

    ASSERT_TRUE(wait_until([&] {
        return pool.processed_count() == static_cast<std::size_t>(kTotal);
    }));
    pool.stop();

    std::lock_guard<std::mutex> lock(seen_mutex);
    EXPECT_EQ(seen.size(), static_cast<std::size_t>(kTotal));
    EXPECT_EQ(broker.pending_count(), 0u);
    EXPECT_EQ(broker.inflight_count(), 0u);
}

TEST(WorkerPool, StopIsIdempotentAndDestructorSafe) {
    InMemoryBroker broker;
    TaskRegistry registry;
    registry.register_handler("noop", [](const Task&) {});

    WorkerPool pool(broker, registry, 3);
    pool.start();
    pool.start();  // second start is a no-op
    pool.stop();
    pool.stop();   // second stop is a no-op
    SUCCEED();
}
