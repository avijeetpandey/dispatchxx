#include <atomic>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "dispatchxx/InMemoryBroker.hpp"

using namespace dispatchxx;

namespace {

Task make_task(std::string id, int priority = 0) {
    Task t;
    t.id = std::move(id);
    t.handler = "noop";
    t.priority = priority;
    return t;
}

}  // namespace

TEST(InMemoryBroker, ClaimReturnsEnqueuedTask) {
    InMemoryBroker broker;
    broker.enqueue(make_task("a"));

    ASSERT_EQ(broker.pending_count(), 1u);
    auto claimed = broker.claim();

    ASSERT_TRUE(claimed.has_value());
    EXPECT_EQ(claimed->id, "a");
    EXPECT_EQ(claimed->status, TaskStatus::InFlight);
    EXPECT_EQ(claimed->attempts, 1);
    EXPECT_EQ(broker.pending_count(), 0u);
    EXPECT_EQ(broker.inflight_count(), 1u);
}

TEST(InMemoryBroker, ClaimOnEmptyReturnsNullopt) {
    InMemoryBroker broker;
    EXPECT_FALSE(broker.claim().has_value());
}

TEST(InMemoryBroker, HigherPriorityClaimedFirst) {
    InMemoryBroker broker;
    broker.enqueue(make_task("low", 1));
    broker.enqueue(make_task("high", 10));
    broker.enqueue(make_task("mid", 5));

    EXPECT_EQ(broker.claim()->id, "high");
    EXPECT_EQ(broker.claim()->id, "mid");
    EXPECT_EQ(broker.claim()->id, "low");
}

TEST(InMemoryBroker, FifoWithinSamePriority) {
    InMemoryBroker broker;
    for (int i = 0; i < 5; ++i) {
        broker.enqueue(make_task("t" + std::to_string(i), 3));
    }
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(broker.claim()->id, "t" + std::to_string(i));
    }
}

TEST(InMemoryBroker, AckRemovesInflight) {
    InMemoryBroker broker;
    broker.enqueue(make_task("a"));
    auto claimed = broker.claim();
    ASSERT_TRUE(claimed.has_value());

    EXPECT_TRUE(broker.ack("a"));
    EXPECT_EQ(broker.inflight_count(), 0u);
    EXPECT_FALSE(broker.ack("a"));
    EXPECT_FALSE(broker.ack("never-existed"));
}

// Each task must be claimed exactly once across all consumers, with no loss and
// no duplication, under heavy contention.
TEST(InMemoryBroker, ConcurrentProducersAndConsumers) {
    constexpr int kProducers = 10;
    constexpr int kConsumers = 5;
    constexpr int kPerProducer = 100;
    constexpr int kTotal = kProducers * kPerProducer;

    InMemoryBroker broker;

    std::vector<std::thread> producers;
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&broker, p] {
            for (int i = 0; i < kPerProducer; ++i) {
                broker.enqueue(make_task("p" + std::to_string(p) + "_" + std::to_string(i),
                                         i % 7));
            }
        });
    }

    std::atomic<int> claimed_count{0};
    std::atomic<bool> producers_done{false};
    std::vector<std::vector<std::string>> per_consumer(kConsumers);

    std::vector<std::thread> consumers;
    for (int c = 0; c < kConsumers; ++c) {
        consumers.emplace_back([&, c] {
            while (true) {
                auto task = broker.claim();
                if (task.has_value()) {
                    per_consumer[c].push_back(task->id);
                    broker.ack(task->id);
                    claimed_count.fetch_add(1, std::memory_order_relaxed);
                } else if (producers_done.load(std::memory_order_acquire) &&
                           claimed_count.load(std::memory_order_relaxed) >= kTotal) {
                    break;
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }

    for (auto& t : producers) t.join();
    producers_done.store(true, std::memory_order_release);
    for (auto& t : consumers) t.join();

    std::unordered_set<std::string> all;
    for (const auto& ids : per_consumer) {
        for (const auto& id : ids) {
            EXPECT_TRUE(all.insert(id).second) << "duplicate claim: " << id;
        }
    }

    EXPECT_EQ(all.size(), static_cast<std::size_t>(kTotal));
    EXPECT_EQ(broker.pending_count(), 0u);
    EXPECT_EQ(broker.inflight_count(), 0u);
}
