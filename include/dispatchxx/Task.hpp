#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace dispatchxx {

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

enum class TaskStatus {
    Pending,
    InFlight,
    Completed,
    Failed,
    DeadLettered,
};

// A unit of work flowing through the broker. Tasks are value types so they can
// be copied freely between producer, broker, and worker boundaries without
// shared-ownership lifetime concerns.
struct Task {
    std::string id;
    std::string handler;   // Logical name resolved against the TaskRegistry.
    std::string payload;
    int priority{0};       // Higher values are claimed first.
    int max_retries{0};
    int attempts{0};       // Number of times this task has been claimed.
    TaskStatus status{TaskStatus::Pending};
    TimePoint enqueue_time{Clock::now()};
};

}  // namespace dispatchxx
