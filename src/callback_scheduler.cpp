#include "callback_scheduler.h"

#include <exception>

CallbackScheduler::CallbackScheduler() {
    // Запускаем фоновый рабочий поток, который будет управлять задачами
    worker_ = std::thread(&CallbackScheduler::WorkerLoop, this);
}

CallbackScheduler::~CallbackScheduler() {
    {
        std::lock_guard<std::mutex> lock(mtx_);
        stop_ = true;
    }
    
    cv_.notify_all();

    if (worker_.joinable()) {
        worker_.join();
    }
}

CallbackScheduler::TaskId CallbackScheduler::Schedule(std::function<void()> callback, TimePoint when)
{
    if (!callback) return 0;

    std::lock_guard<std::mutex> lock(mtx_);

    TaskId id = ++next_id_;
    Task task{ id, when, std::move(callback) };

    queue_.push(std::move(task));
    active_ids_.insert(id); // Запоминаем ID как активный

    
    cv_.notify_all();

    return id;
}

bool CallbackScheduler::Cancel(TaskId id)
{
    std::lock_guard<std::mutex> lock(mtx_);

    // Если задачи нет среди активных, значит она либо уже выполнена, либо не существовала
    if (active_ids_.find(id) == active_ids_.end()) {
        return false;
    }

    // Если она уже была отменена ранее, повторно true не возвращаем
    if (cancelled_.find(id) != cancelled_.end()) {
        return false;
    }

    // Маркируем задачу как отмененную (ленивое удаление)
    cancelled_.insert(id);

    
    cv_.notify_all();

    return true;
}
void CallbackScheduler::WorkerLoop() {
    while (true) {
        std::unique_lock<std::mutex> lock(mtx_);

        // Ждем появления задач или сигнала об остановке планировщика
        cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });

        // Если вызван деструктор — выходим немедленно
        if (stop_) {
            break;
        }

        // Чистим верхушку очереди от отмененных пользователем задач
        while (!queue_.empty() && cancelled_.find(queue_.top().id) != cancelled_.end()) {
            TaskId id = queue_.top().id;
            queue_.pop();
            cancelled_.erase(id);
            active_ids_.erase(id);
        }

        // Если после чистки задач не осталось — возвращаемся к ожиданию
        if (queue_.empty()) {
            continue;
        }

        // Смотрим на самую ближайшую по времени задачу
        Task current_task = queue_.top();
        auto now = std::chrono::system_clock::now();

        if (current_task.when <= now) {
            
            queue_.pop();
            active_ids_.erase(current_task.id);

            
            lock.unlock();

            try {
                if (current_task.callback) {
                    current_task.callback();
                }
            }
            catch (...) {
                
            }

            
        }
        else {
            
            cv_.wait_until(lock, current_task.when);
        }
    }
}
