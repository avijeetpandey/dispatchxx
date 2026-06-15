#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "dispatchxx/BrokerConfig.hpp"
#include "dispatchxx/InMemoryBroker.hpp"
#include "dispatchxx/TaskRegistry.hpp"
#include "dispatchxx/WorkerPool.hpp"

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

BrokerConfig fast_config() {
    BrokerConfig cfg;
    cfg.visibility_timeout = 20ms;
    cfg.reaper_interval = 5ms;
    return cfg;
}

Task make_task(std::string id, int max_retries) {
    Task t;
    t.id = std::move(id);
    t.handler = "noop";
    t.max_retries = max_retries;
    return t;
}

}  // namespace

// A claimed-but-unacked task is reclaimable once its visibility timeout passes.
TEST(Resilience, ExpiredInflightIsRequeued) {
    InMemoryBroker broker(fast_config());
    broker.enqueue(make_task("a", /*max_retries=*/5));

    auto first = broker.claim();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->attempts, 1);
    EXPECT_EQ(broker.inflight_count(), 1u);
    EXPECT_EQ(broker.pending_count(), 0u);

    // Deliberately do not ack — simulate a crashed worker.
    std::this_thread::sleep_for(40ms);
    EXPECT_EQ(broker.reap_expired(), 1u);
    EXPECT_EQ(broker.inflight_count(), 0u);
    EXPECT_EQ(broker.pending_count(), 1u);

    auto second = broker.claim();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->id, "a");
    EXPECT_EQ(second->attempts, 2);  // reclaim increments the attempt counter
}

// A still-fresh in-flight task must not be reaped.
TEST(Resilience, FreshInflightIsNotReaped) {
    InMemoryBroker broker(fast_config());
    broker.enqueue(make_task("a", 5));
    auto claimed = broker.claim();
    ASSERT_TRUE(claimed.has_value());

    EXPECT_EQ(broker.reap_expired(), 0u);
    EXPECT_EQ(broker.inflight_count(), 1u);
}

// Acking before the timeout prevents any reaping.
TEST(Resilience, AckedTaskIsNotReaped) {
    InMemoryBroker broker(fast_config());
    broker.enqueue(make_task("a", 5));
    auto claimed = broker.claim();
    ASSERT_TRUE(claimed.has_value());
    EXPECT_TRUE(broker.ack("a"));

    std::this_thread::sleep_for(40ms);
    EXPECT_EQ(broker.reap_expired(), 0u);
    EXPECT_EQ(broker.inflight_count(), 0u);
    EXPECT_EQ(broker.pending_count(), 0u);
}

// Repeated timeouts beyond max_retries route the task to the DLQ.
TEST(Resilience, RepeatedTimeoutsDeadLetter) {
    InMemoryBroker broker(fast_config());
    broker.enqueue(make_task("poison", /*max_retries=*/1));

    // Delivery 1 (attempts == 1): timeout -> requeue (within retry budget).
    auto first = broker.claim();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->attempts, 1);
    std::this_thread::sleep_for(40ms);
    EXPECT_EQ(broker.reap_expired(), 1u);
    EXPECT_EQ(broker.pending_count(), 1u);
    EXPECT_EQ(broker.dead_letter_count(), 0u);

    // Delivery 2 (attempts == 2 > max_retries): timeout -> dead-letter.
    auto second = broker.claim();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->attempts, 2);
    std::this_thread::sleep_for(40ms);
    EXPECT_EQ(broker.reap_expired(), 1u);

    EXPECT_EQ(broker.dead_letter_count(), 1u);
    EXPECT_EQ(broker.pending_count(), 0u);
    auto dlq = broker.drain_dead_letters();
    ASSERT_EQ(dlq.size(), 1u);
    EXPECT_EQ(dlq.front().id, "poison");
    EXPECT_EQ(dlq.front().attempts, 2);
    EXPECT_EQ(dlq.front().status, TaskStatus::DeadLettered);
}

// Explicit failure requeues until retries are exhausted, then dead-letters.
TEST(Resilience, ExplicitFailRequeuesThenDeadLetters) {
    InMemoryBroker broker(fast_config());
    broker.enqueue(make_task("f", /*max_retries=*/1));

    auto a = broker.claim();  // attempts 1
    ASSERT_TRUE(a.has_value());
    EXPECT_TRUE(broker.fail("f"));
    EXPECT_EQ(broker.pending_count(), 1u);

    auto b = broker.claim();  // attempts 2 (> max_retries)
    ASSERT_TRUE(b.has_value());
    EXPECT_TRUE(broker.fail("f"));

    EXPECT_EQ(broker.pending_count(), 0u);
    EXPECT_EQ(broker.dead_letter_count(), 1u);
    EXPECT_FALSE(broker.fail("f"));  // no longer in flight
}

// The background reaper thread requeues a crashed worker's task without manual
// intervention, and a second worker completes it.
TEST(Resilience, BackgroundReaperReassignsCrashedTask) {
    InMemoryBroker broker(fast_config());
    broker.start_reaper();

    broker.enqueue(make_task("crash-me", /*max_retries=*/5));

    auto claimed = broker.claim();  // worker that "crashes" — never acks
    ASSERT_TRUE(claimed.has_value());

    // The reaper should move it back to pending automatically.
    ASSERT_TRUE(wait_until([&] { return broker.pending_count() == 1u; }));

    auto reclaimed = broker.claim();
    ASSERT_TRUE(reclaimed.has_value());
    EXPECT_EQ(reclaimed->id, "crash-me");
    EXPECT_GE(reclaimed->attempts, 2);
    EXPECT_TRUE(broker.ack("crash-me"));

    broker.stop_reaper();
    EXPECT_EQ(broker.inflight_count(), 0u);
}

// Worker pool integration: a handler that throws on its first delivery but
// succeeds on retry. The broker's fail() path requeues it; the DLQ stays empty.
TEST(Resilience, WorkerPoolRetriesThenSucceeds) {
    InMemoryBroker broker(fast_config());
    TaskRegistry registry;

    std::mutex mutex;
    std::map<std::string, int> attempts;
    std::atomic<int> succeeded{0};

    registry.register_handler("flaky", [&](const Task& t) {
        int n;
        {
            std::lock_guard<std::mutex> lock(mutex);
            n = ++attempts[t.id];
        }
        if (n == 1) {
            throw std::runtime_error("transient failure");
        }
        succeeded.fetch_add(1, std::memory_order_relaxed);
    });

    WorkerPool pool(broker, registry, 3);
    pool.start();

    constexpr int kTasks = 50;
    for (int i = 0; i < kTasks; ++i) {
        Task t;
        t.id = "j" + std::to_string(i);
        t.handler = "flaky";
        t.max_retries = 3;
        broker.enqueue(std::move(t));
    }

    ASSERT_TRUE(wait_until([&] { return succeeded.load() == kTasks; }));
    pool.stop();

    EXPECT_EQ(succeeded.load(), kTasks);
    EXPECT_EQ(broker.dead_letter_count(), 0u);
    EXPECT_EQ(broker.pending_count(), 0u);
    EXPECT_EQ(broker.inflight_count(), 0u);
}

// Worker pool integration: a handler that always throws ends up dead-lettered
// after exhausting retries.
TEST(Resilience, WorkerPoolDeadLettersPoisonTask) {
    InMemoryBroker broker(fast_config());
    TaskRegistry registry;
    registry.register_handler("always_fail",
                              [](const Task&) { throw std::runtime_error("nope"); });

    WorkerPool pool(broker, registry, 2);
    pool.start();

    Task t;
    t.id = "poison";
    t.handler = "always_fail";
    t.max_retries = 2;  // 3 deliveries total, then DLQ
    broker.enqueue(std::move(t));

    ASSERT_TRUE(wait_until([&] { return broker.dead_letter_count() == 1u; }));
    pool.stop();

    auto dlq = broker.drain_dead_letters();
    ASSERT_EQ(dlq.size(), 1u);
    EXPECT_EQ(dlq.front().id, "poison");
    EXPECT_EQ(dlq.front().attempts, 3);
}
