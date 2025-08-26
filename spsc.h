#pragma once
#include <array>
#include <atomic>
#include <cstddef>
#include <optional>
#include <utility>

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

template <typename T, size_t SIZE>
class LockFreeQueue {
    static constexpr size_t CAPACITY = SIZE;
    static constexpr size_t PADDING = (CACHE_LINE_SIZE - 1) / sizeof(T) + 1;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> head_{0};
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail_{0};
    alignas(CACHE_LINE_SIZE) size_t head_cache_{0};
    alignas(CACHE_LINE_SIZE) size_t tail_cache_{0};
    alignas(CACHE_LINE_SIZE) std::array<std::aligned_storage_t<sizeof(T), alignof(T)>,
                                        CAPACITY + 2 * PADDING> buffer_{};

    T* get_slot(size_t idx) {
        return reinterpret_cast<T*>(&buffer_[idx + PADDING]);
    }

public:
    LockFreeQueue() = default;

    ~LockFreeQueue() {
        size_t curr_head = head_.load(std::memory_order_acquire);
        size_t curr_tail = tail_.load(std::memory_order_acquire);

        while (curr_head != curr_tail) {
            get_slot(curr_head)->~T();
            curr_head = (curr_head + 1) % CAPACITY;
        }
    }

    LockFreeQueue(const LockFreeQueue&) = delete;
    LockFreeQueue& operator=(const LockFreeQueue&) = delete;
    LockFreeQueue(LockFreeQueue&&) = delete;
    LockFreeQueue& operator=(LockFreeQueue&&) = delete;

    template <typename U>
    bool enqueue(U&& item) {
        const size_t curr_tail = tail_.load(std::memory_order_relaxed);
        const size_t next_tail = (curr_tail + 1) % CAPACITY;

        if (next_tail == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (next_tail == head_cache_) {
                return false;
            }
        }

        new(get_slot(curr_tail)) T(std::forward<U>(item));

        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    T* front() {
        const size_t curr_head = head_.load(std::memory_order_relaxed);

        if (curr_head == tail_cache_) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (curr_head == tail_cache_) {
                return nullptr;
            }
        }

        return get_slot(curr_head);
    }

    void pop() {
        const size_t curr_head = head_.load(std::memory_order_relaxed);
        get_slot(curr_head)->~T();
        head_.store((curr_head + 1) % CAPACITY, std::memory_order_release);
    }

    std::optional<T> dequeue() {
        const size_t curr_head = head_.load(std::memory_order_relaxed);

        if (curr_head == tail_cache_) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (curr_head == tail_cache_) {
                return std::nullopt;
            }
        }

        T* item_ptr = get_slot(curr_head);
        std::optional<T> result(std::move(*item_ptr));
        item_ptr->~T();
        head_.store((curr_head + 1) % CAPACITY, std::memory_order_release);
        return result;
    }

    bool empty() const {
        return head_.load(std::memory_order_acquire) ==
            tail_.load(std::memory_order_acquire);
    }

    size_t size() const {
        const size_t head = head_.load(std::memory_order_acquire);
        const size_t tail = tail_.load(std::memory_order_acquire);

        if (tail >= head) {
            return tail - head;
        }
        else {
            return CAPACITY - (head - tail);
        }
    }

    size_t capacity() const { return CAPACITY - 1; }
};
