#include "dispatchxx/TaskQueueClient.hpp"

#include <atomic>
#include <utility>

namespace dispatchxx {

namespace {

// Process-unique, monotonically increasing id suffix for generated task ids.
std::string next_generated_id() {
    static std::atomic<std::uint64_t> counter{0};
    return "task-" + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

}  // namespace

TaskQueueClient::TaskQueueClient(Config config)
    : broker_(config.broker),
      pool_(broker_, registry_, config.worker_count) {}

TaskQueueClient::~TaskQueueClient() {
    stop();
}

void TaskQueueClient::register_handler(const std::string& handler,
                                       TaskRegistry::Handler fn) {
    registry_.register_handler(handler, std::move(fn));
}

std::string TaskQueueClient::enqueue(const std::string& handler,
                                     std::string payload,
                                     EnqueueOptions options) {
    Task task;
    task.id = next_generated_id();
    task.handler = handler;
    task.payload = std::move(payload);
    task.priority = options.priority;
    task.max_retries = options.max_retries;
    const std::string id = task.id;
    broker_.enqueue(std::move(task));
    return id;
}

void TaskQueueClient::enqueue_task(Task task) {
    broker_.enqueue(std::move(task));
}

void TaskQueueClient::start() {
    if (started_) {
        return;
    }
    started_ = true;
    broker_.start_reaper();
    pool_.start();
}

void TaskQueueClient::stop() {
    if (!started_) {
        return;
    }
    started_ = false;
    pool_.stop();
    broker_.stop_reaper();
}

std::vector<Task> TaskQueueClient::dead_letters() {
    return broker_.drain_dead_letters();
}

}  // namespace dispatchxx
