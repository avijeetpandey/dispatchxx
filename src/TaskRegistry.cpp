#include "dispatchxx/TaskRegistry.hpp"

#include <utility>

namespace dispatchxx {

bool TaskRegistry::register_handler(const std::string& name, Handler handler) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto [it, inserted] = handlers_.insert_or_assign(name, std::move(handler));
    (void)it;
    return !inserted;
}

std::optional<TaskRegistry::Handler> TaskRegistry::find(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = handlers_.find(name);
    if (it == handlers_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool TaskRegistry::contains(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return handlers_.find(name) != handlers_.end();
}

std::size_t TaskRegistry::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return handlers_.size();
}

}  // namespace dispatchxx
