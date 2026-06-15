#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "dispatchxx/Task.hpp"

namespace dispatchxx {

// Thread-safe mapping from logical handler names to the C++ callables that
// process them. Handlers may be registered before or after workers start;
// lookups take a shared lock so dispatch is contention-free across workers.
class TaskRegistry {
public:
    using Handler = std::function<void(const Task&)>;

    // Register (or replace) the handler for a name. Returns true if a previous
    // handler was overwritten.
    bool register_handler(const std::string& name, Handler handler);

    // Return the handler for a name, or std::nullopt if none is registered.
    std::optional<Handler> find(const std::string& name) const;

    bool contains(const std::string& name) const;
    std::size_t size() const;

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, Handler> handlers_;
};

}  // namespace dispatchxx
