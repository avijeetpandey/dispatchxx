#pragma once

#include <chrono>

namespace dispatchxx {

// Tunables shared by broker and worker components. Defaults favour correctness
// and quick test turnaround over throughput.
struct BrokerConfig {
    // How long a claimed task may remain unacked before the reaper considers it
    // lost and makes it reclaimable.
    std::chrono::milliseconds visibility_timeout{std::chrono::seconds(30)};

    // How often the background reaper scans for expired in-flight tasks.
    std::chrono::milliseconds reaper_interval{std::chrono::seconds(1)};
};

}  // namespace dispatchxx
