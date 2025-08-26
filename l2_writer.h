#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include "spsc.h"

enum : uint32_t { COL_TS = 0, COL_PX = 1, COL_QTY = 2, COL_SIDE = 3, COL_COUNT = 4 };

struct L2Row {
    uint64_t ts_ns;
    uint32_t price;
    float qty;
    uint8_t side;
};

struct L2WriterOpt {
    std::string base_dir;
    std::string product;
    static constexpr uint64_t rows_per_hr = 1ull << 24;
    uint32_t fsync_every_rows{0};

    L2WriterOpt(std::string base, std::string prod) : base_dir(std::move(base)), product(std::move(prod)) {}
};

struct alignas(64) L2ColFileHeader {
    char magic[6];
    uint16_t header_size;
    uint16_t version;
    uint16_t pad16{0};
    uint32_t _pad32{0};
    char product[16];
    uint64_t hour_epoch_start;
    uint64_t rows;
    uint64_t capacity;
    uint64_t col_off[COL_COUNT];
    uint64_t col_sz[COL_COUNT];
    uint8_t pad[256 - 6 - 2 - 2 - 2 - 4 - 16 - 8 - 8 - 8 - (8 * COL_COUNT) - (8 * COL_COUNT)];
};

static_assert(sizeof(L2ColFileHeader) == 256, "header must be 256 bytes");

class L2Writer {
public:
    explicit L2Writer(const L2WriterOpt& opt);
    ~L2Writer();

    void start();
    void stop();
    void join();

    bool enqueue(const L2Row& r) noexcept { return queue_.enqueue(r); }
    uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
    uint64_t rows() const noexcept { return rows_.load(std::memory_order_acquire); }
    uint64_t hour_s() const noexcept { return hour_start_; }

private:
    int fd_{-1};
    uint8_t* base_{nullptr};
    size_t map_bytes_{0};
    uint64_t* ts_{nullptr};
    uint32_t* price_{nullptr};
    float* qty_{nullptr};
    uint8_t* side_{nullptr};
    L2ColFileHeader hdr_{};
    uint64_t col_off_[COL_COUNT]{};
    uint64_t col_sz_[COL_COUNT]{};
    std::atomic<uint64_t> rows_{0};
    std::atomic<uint64_t> dropped_{0};
    uint64_t capacity_{L2WriterOpt::rows_per_hr};
    uint64_t hour_start_{~0ull};
    L2WriterOpt opt_;
    static constexpr size_t kQueueCapacity = (1ull << 18);
    LockFreeQueue<L2Row, kQueueCapacity> queue_;
    std::unique_ptr<std::thread> thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stop_{false};

    void run();
    bool rotate_to_hour(uint64_t hour_s);
    void close_file();
    static constexpr size_t HEADER_SZ = 256;
    bool open_file(uint64_t hour_s);
    bool map_file(size_t bytes);
    bool write_header();
    bool update_rows_in_header();

    static inline uint64_t hour_start_from_ns(uint64_t ts_ns) noexcept {
        const uint64_t s = ts_ns / 1'000'000'000ull;
        return (s / 3600ull) * 3600ull;
    }

    static bool mkdir_p(const std::string& dir);
    static std::string date_dir(const std::string& base, uint64_t hour_s);
    static std::string hour_basename(uint64_t hour_s);
    static bool preallocate(int fd, size_t bytes);
};
