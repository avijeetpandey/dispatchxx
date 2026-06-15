#include <atomic>
#include <string>

#include <gtest/gtest.h>

#include "dispatchxx/TaskRegistry.hpp"

using namespace dispatchxx;

TEST(TaskRegistry, FindMissingReturnsNullopt) {
    TaskRegistry registry;
    EXPECT_FALSE(registry.find("nope").has_value());
    EXPECT_FALSE(registry.contains("nope"));
    EXPECT_EQ(registry.size(), 0u);
}

TEST(TaskRegistry, RegisterAndInvoke) {
    TaskRegistry registry;
    std::atomic<int> calls{0};

    EXPECT_FALSE(registry.register_handler("inc", [&](const Task&) {
        calls.fetch_add(1, std::memory_order_relaxed);
    }));

    ASSERT_TRUE(registry.contains("inc"));
    EXPECT_EQ(registry.size(), 1u);

    auto handler = registry.find("inc");
    ASSERT_TRUE(handler.has_value());

    Task t;
    t.handler = "inc";
    (*handler)(t);
    EXPECT_EQ(calls.load(), 1);
}

TEST(TaskRegistry, ReRegisterReplacesHandler) {
    TaskRegistry registry;
    registry.register_handler("h", [](const Task&) {});

    bool replaced = registry.register_handler("h", [](const Task&) {});
    EXPECT_TRUE(replaced);
    EXPECT_EQ(registry.size(), 1u);
}

TEST(TaskRegistry, HandlerReceivesTaskPayload) {
    TaskRegistry registry;
    std::string seen;
    registry.register_handler("echo", [&](const Task& t) { seen = t.payload; });

    Task t;
    t.handler = "echo";
    t.payload = "hello";
    (*registry.find("echo"))(t);
    EXPECT_EQ(seen, "hello");
}
