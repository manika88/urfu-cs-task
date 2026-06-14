#pragma once
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>
#include <unordered_set>

#include <chrono>
#include <cstdint>
#include <functional>

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

class CallbackScheduler
{
public:
    using TaskId = std::uint64_t;

    CallbackScheduler();
    ~CallbackScheduler();

    CallbackScheduler(const CallbackScheduler&) = delete;
    CallbackScheduler& operator=(const CallbackScheduler&) = delete;

    CallbackScheduler(CallbackScheduler&&) = delete;
    CallbackScheduler& operator=(CallbackScheduler&&) = delete;

    TaskId Schedule(std::function<void()> callback, TimePoint when);
    bool Cancel(TaskId id);
private:
    
    struct Task {
        TaskId id;
        TimePoint when;
        std::function<void()> callback;
    };

    struct Compare {
        bool operator()(const Task& a, const Task& b) const {
            return a.when > b.when; // min-heap (сначала самые ранние задачи)
        }
    };

    void WorkerLoop();

    std::priority_queue<Task, std::vector<Task>, Compare> queue_;
    std::unordered_set<TaskId> active_ids_;   // Для мгновенной проверки существования в Cancel
    std::unordered_set<TaskId> cancelled_;    // Для ленивого удаления

    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::thread worker_;
    std::uint64_t next_id_{ 0 };
    bool stop_ = false;
};
