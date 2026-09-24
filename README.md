Based on candidate feedback from interview platforms like Glassdoor, Reddit, and technical prep sites, Shield AI’s 120-minute coding assessment for Application and Sustainment Engineering avoids abstract brain-teasers in favor of practical, multi-part scenarios. These problems test your ability to handle telemetry, out-of-order data, concurrency, and system reliability—core components of their autonomous defense systems.
Here are five specific types of problems that fit this two-hour allocation:

**1. Time-Series Fault Telemetry (Sliding Window)**
 * **The Scenario:** You are provided a stream of system fault logs from an autonomous drone. You must implement an interface with methods like add_fault(timestamp, error_code) and get_recent_fault_count(current_time, time_window).
 * **The Challenge:** As the volume of data grows, you must efficiently count faults within a sliding window (e.g., the last 10 seconds) without running out of memory.
 * **What it Tests:** Space/time complexity and memory management. You are expected to use double-ended queues (deques) to evict stale timestamps efficiently rather than rescanning the entire history on every call.

**2. Out-of-Order Versioned Configuration Delivery**
 * **The Scenario:** A command center sends configuration updates to edge vehicles. Because these vehicles frequently lose connection, they receive these updates in batches, out-of-order, or with duplicates.
 * **The Challenge:** Write a parser that ingests this disordered stream of JSON or custom-delimited commands and determines the final, correct state of the vehicle's configuration.
 * **What it Tests:** Idempotency and sequence tracking. You will need to implement logic that compares version IDs or sequence numbers, ensuring an older delayed packet does not overwrite a newer configuration.

**3. Bounded Telemetry Buffer with Backpressure**
 * **The Scenario:** A vehicle generates high-frequency telemetry data, but the network link to the ground station is slow or intermittent.
 * **The Challenge:** Implement a bounded buffer system that temporarily stores this telemetry. When the queue is full, you must decide how to shed load—for example, dropping the oldest non-critical logs while preserving critical flight-control state.
 * **What it Tests:** Resource ceilings and defensive programming. They want to see that your logging or upload mechanics never block the critical control path or crash the system due to an out-of-memory exception.

**4. Sensor Log Merging and Reconciliation**
 * **The Scenario:** You receive multiple independent log files from different subsystems (e.g., camera timestamps, flight controller events, and network state changes).
 * **The Challenge:** Merge these sorted sequences into a single chronological master log. In later parts of the 120-minute window, you may be asked to handle missing timestamps by applying basic interpolation or dropping orphaned events.
 * **What it Tests:** Pointer manipulation and data reconciliation. This is a practical application of "Merge K Sorted Lists," requiring you to handle edge cases like mismatched timestamps or corrupted lines gracefully.

**5. Vehicle State Replay and Fault Identification**
 * **The Scenario:** You are given a historical log of telemetry commands and state changes from a vehicle that experienced a failure in the field.
 * **The Challenge:** Build a state machine that reads the log line-by-line, updating internal variables (e.g., velocity, altitude, battery level). You must write an algorithm to flag exactly when the vehicle breached a predefined safety boundary (e.g., altitude dropped below a threshold while velocity exceeded a limit).
 * **What it Tests:** Object-oriented design, state management, and debugging real-world sustainment issues. This tests your ability to translate physical constraints into code logic and isolate exactly where a system degraded.




## 1. Time-Series Fault Telemetry (Sliding Window)
This problem requires managing a stream of incoming data while calculating metrics over a moving time window. In a real-time defense context, you must avoid scanning the entire history of logs to find the current count.
```cpp
#include <deque>
#include <mutex>
#include <cstdint>

class FaultTelemetry {
private:
    struct Fault {
        uint64_t timestamp;
        int error_code;
    };
    std::deque<Fault> window;
    std::mutex mtx;

public:
    // Pushes new telemetry events onto the back of the queue
    void add_fault(uint64_t timestamp, int error_code) {
        std::lock_guard<std::mutex> lock(mtx);
        window.push_back({timestamp, error_code});
    }

    // Evicts stale data and returns the active count
    int get_recent_fault_count(uint64_t current_time, uint64_t time_window) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Remove logs older than the allowed time window
        while (!window.empty() && (current_time - window.front().timestamp > time_window)) {
            window.pop_front();
        }
        return window.size();
    }
};

```
## 2. Out-of-Order Versioned Configuration Delivery
When edge devices lose connection, they receive configuration updates out of order. You must build a state manager that applies updates safely by tracking sequence numbers and ignoring outdated payloads.
```cpp
#include <unordered_map>
#include <mutex>
#include <string>

class ConfigurationManager {
private:
    std::unordered_map<std::string, int> config_versions;
    std::unordered_map<std::string, std::string> config_state;
    std::mutex mtx;

public:
    void apply_update(const std::string& key, const std::string& value, int sequence_num) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Only apply if the sequence number is strictly greater than the known version
        if (config_versions.find(key) == config_versions.end() || sequence_num > config_versions[key]) {
            config_state[key] = value;
            config_versions[key] = sequence_num;
        }
    }
    
    std::string get_config(const std::string& key) {
        std::lock_guard<std::mutex> lock(mtx);
        return config_state.count(key) ? config_state[key] : "";
    }
};

```
## 3. Bounded Telemetry Buffer with Backpressure
Autonomy systems generate massive amounts of logging data. If the network stalls, unbounded queues will consume all available memory and crash the vehicle. You must implement a buffer that enforces a hard memory limit and gracefully drops non-critical data.
```cpp
#include <deque>
#include <mutex>
#include <condition_variable>
#include <string>
#include <optional>

class TelemetryBuffer {
private:
    std::deque<std::string> buffer;
    size_t max_capacity;
    std::mutex mtx;
    std::condition_variable cv;

public:
    explicit TelemetryBuffer(size_t capacity) : max_capacity(capacity) {}

    void push_telemetry(const std::string& data, bool is_critical) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // Backpressure implementation
        if (buffer.size() >= max_capacity) {
            if (is_critical) {
                buffer.pop_front(); // Shed oldest load to make room for critical data
            } else {
                return; // Drop non-critical data instantly to avoid blocking control loops
            }
        }
        
        buffer.push_back(data);
        cv.notify_one(); // Signal consumer thread that data is available
    }

    std::optional<std::string> pop_telemetry() {
        std::unique_lock<std::mutex> lock(mtx);
        
        // Wait efficiently rather than spinning the CPU
        if (cv.wait_for(lock, std::chrono::milliseconds(10), [this]{ return !buffer.empty(); })) {
            std::string data = buffer.front();
            buffer.pop_front();
            return data;
        }
        return std::nullopt; // Return empty if timeout occurs
    }
};

```
## 4. Sensor Log Merging and Reconciliation
You will often need to merge multiple sorted sensor streams (e.g., flight controller, camera, GPS) into a single chronological log for post-flight analysis. A min-heap (std::priority_queue) is the optimal data structure to merge K sorted arrays.
```cpp
#include <vector>
#include <queue>
#include <string>

struct LogEntry {
    uint64_t timestamp;
    int stream_idx;
    int element_idx;

    // C++ priority queues are max-heaps by default. We overload '>' to create a min-heap based on timestamp.
    bool operator>(const LogEntry& other) const {
        return timestamp > other.timestamp; 
    }
};

std::vector<LogEntry> merge_sensor_logs(const std::vector<std::vector<LogEntry>>& streams) {
    std::priority_queue<LogEntry, std::vector<LogEntry>, std::greater<LogEntry>> min_heap;
    std::vector<LogEntry> merged_log;

    // Push the first element of each stream into the heap
    for (size_t i = 0; i < streams.size(); ++i) {
        if (!streams[i].empty()) {
            min_heap.push(streams[i][0]);
        }
    }

    // Extract the globally oldest log and push the next element from its origin stream
    while (!min_heap.empty()) {
        LogEntry current = min_heap.top();
        min_heap.pop();
        merged_log.push_back(current);

        if (current.element_idx + 1 < streams[current.stream_idx].size()) {
            min_heap.push(streams[current.stream_idx][current.element_idx + 1]);
        }
    }
    return merged_log;
}

```
## 5. Vehicle State Replay and Fault Identification
Sustainment engineering involves root-causing field failures. You may be asked to iterate over a chronological log, update an internal state machine, and halt execution the exact moment a safety invariant is breached.
```cpp
#include <vector>
#include <iostream>

struct TelemetryEvent {
    uint64_t timestamp;
    double new_altitude;
    double new_velocity;
};

struct VehicleState {
    double altitude = 0.0;
    double velocity = 0.0;
};

void replay_and_find_fault(const std::vector<TelemetryEvent>& logs) {
    VehicleState current_state;
    const double MIN_SAFE_ALTITUDE = 100.0;
    const double MAX_SAFE_VELOCITY = 50.0;

    for (const auto& event : logs) {
        // Sequentially update the physical state model
        current_state.altitude = event.new_altitude;
        current_state.velocity = event.new_velocity;

        // Evaluate physical constraints
        if (current_state.altitude < MIN_SAFE_ALTITUDE && current_state.velocity > MAX_SAFE_VELOCITY) {
            std::cout << "Safety boundary breached at timestamp: " << event.timestamp << "\n";
            std::cout << "State -> Alt: " << current_state.altitude << ", Vel: " << current_state.velocity << "\n";
            return; // Halt replay to isolate the fault
        }
    }
    std::cout << "No faults detected during replay sequence.\n";
}

```
Are there any specific modern C++ features, such as smart pointers or atomic variables, that you want to integrate into these examples?

