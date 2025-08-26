// L2_writer.cpp
#include "l2_writer.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <cstddef>
#include <fcntl.h>
#include <linux/limits.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono;

static inline std::string join_path(const std::string& a, const std::string& b) {
    if (a.empty()) {
        return b;
    }
    return a.back() == '/' ? a + b : a + '/' + b;
}

bool L2Writer::mkdir_p(const std::string& dir) {
    char tmp[PATH_MAX];
    std::snprintf(tmp, sizeof(tmp), "%s", dir.c_str());
    size_t n = std::strlen(tmp);
    if (!n) {
        return true;
    }
    if (tmp[n - 1] == '/') {
        tmp[n - 1] = '\0';
    }
    for (char* p = tmp + 1; *p; ++p) {
        if (*p == '/') {
            *p = '\0';
            ::mkdir(tmp, 0755);
            *p = '/';
        }
    }
    ::mkdir(tmp, 0755);
    return true;
}

std::string L2Writer::date_dir(const std::string& base, uint64_t hour_s) {
    time_t tt = static_cast<time_t>(hour_s);
    struct tm tm{};
    localtime_r(&tt, &tm);
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%04d%02d%02d",
                  tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
    return join_path(base, buf);
}

std::string L2Writer::hour_basename(uint64_t hour_s) {
    time_t tt = static_cast<time_t>(hour_s);
    struct tm tm{};
    localtime_r(&tt, &tm);
    char buf[9];
    std::snprintf(buf, sizeof(buf), "%02d%02d.bin", tm.tm_hour, 0);
    return std::string(buf);
}

bool L2Writer::preallocate(int fd, size_t bytes) {
#if defined(_POSIX_C_SOURCE) && (_POSIX_C_SOURCE >= 200112L)
    int rc = ::posix_fallocate(fd, 0, (off_t)bytes);
    if (rc == 0) {
        return true;
    }
#endif
    return ::ftruncate(fd, (off_t)bytes) == 0;
}

L2Writer::L2Writer(const L2WriterOpt& opt) : opt_(opt) {}

L2Writer::~L2Writer() {
    stop();
    join();
}

void L2Writer::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;
    }
    stop_.store(false, std::memory_order_release);
    thread_ = std::make_unique<std::thread>(&L2Writer::run, this);
}

void L2Writer::stop() {
    stop_.store(true, std::memory_order_release);
}

void L2Writer::join() {
    if (thread_ && thread_->joinable()) {
        thread_->join();
    }
    thread_.reset();
    running_.store(false, std::memory_order_release);
}

bool L2Writer::open_file(uint64_t hour_s) {
    close_file();

    const uint64_t cap = capacity_;
    const uint64_t ts_bytes = cap * sizeof(uint64_t);
    const uint64_t px_bytes = cap * sizeof(uint32_t);
    const uint64_t qty_bytes = cap * sizeof(float);
    const uint64_t side_bytes = cap * sizeof(uint8_t);

    col_off_[COL_TS] = HEADER_SZ;
    col_sz_[COL_TS] = ts_bytes;
    col_off_[COL_PX] = col_off_[COL_TS] + col_sz_[COL_TS];
    col_sz_[COL_PX] = px_bytes;
    col_off_[COL_QTY] = col_off_[COL_PX] + col_sz_[COL_PX];
    col_sz_[COL_QTY] = qty_bytes;
    col_off_[COL_SIDE] = col_off_[COL_QTY] + col_sz_[COL_QTY];
    col_sz_[COL_SIDE] = side_bytes;

    const size_t file_bytes = static_cast<size_t>(col_off_[COL_SIDE] + col_sz_[COL_SIDE]);

    std::string dir = date_dir(opt_.base_dir, hour_s);
    (void)mkdir_p(dir);
    std::string file = join_path(dir, hour_basename(hour_s));

    fd_ = ::open(file.c_str(), O_CREAT | O_TRUNC | O_RDWR | O_CLOEXEC, 0644);
    if (fd_ < 0) {
        return false;
    }

    if (!preallocate(fd_, file_bytes)) {
        return false;
    }
    if (!map_file(file_bytes)) {
        return false;
    }

    std::memset(&hdr_, 0, sizeof(hdr_));
    std::memcpy(hdr_.magic, "L2COL\n", 6);
    hdr_.header_size = HEADER_SZ;
    hdr_.version = 1;
    std::memset(hdr_.product, 0, sizeof(hdr_.product));
    std::memcpy(hdr_.product, opt_.product.data(),
                std::min(opt_.product.size(), sizeof(hdr_.product)));
    hdr_.hour_epoch_start = hour_s;
    hdr_.rows = 0;
    hdr_.capacity = cap;
    for (int i = 0; i < COL_COUNT; ++i) {
        hdr_.col_off[i] = col_off_[i];
        hdr_.col_sz[i] = col_sz_[i];
    }

    if (!write_header()) {
        return false;
    }

    ts_ = reinterpret_cast<uint64_t*>(base_ + col_off_[COL_TS]);
    price_ = reinterpret_cast<uint32_t*>(base_ + col_off_[COL_PX]);
    qty_ = reinterpret_cast<float*>(base_ + col_off_[COL_QTY]);
    side_ = reinterpret_cast<uint8_t*>(base_ + col_off_[COL_SIDE]);

    rows_.store(0, std::memory_order_release);
    hour_start_ = hour_s;
    return true;
}

bool L2Writer::map_file(size_t bytes) {
    base_ = static_cast<uint8_t*>(::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0));
    if (base_ == MAP_FAILED) {
        base_ = nullptr;
        return false;
    }
    map_bytes_ = bytes;
    return true;
}

bool L2Writer::write_header() {
    std::memcpy(base_, &hdr_, sizeof(hdr_));
    return true;
}

bool L2Writer::update_rows_in_header() {
    std::memcpy(base_ + offsetof(L2ColFileHeader, rows), &hdr_.rows, sizeof(hdr_.rows));
    return true;
}

void L2Writer::close_file() {
    if (fd_ < 0) {
        return;
    }
    hdr_.rows = rows_.load(std::memory_order_acquire);
    (void)update_rows_in_header();
    ::msync(base_, map_bytes_, MS_SYNC);
    ::munmap(base_, map_bytes_); base_ = nullptr; map_bytes_ = 0;
    ::fsync(fd_); ::close(fd_); fd_ = -1;
    ts_ = nullptr; price_ = nullptr; qty_ = nullptr; side_ = nullptr;
    rows_.store(0, std::memory_order_release);
    hour_start_ = ~0ull;
}

bool L2Writer::rotate_to_hour(uint64_t hour_s) {
    return open_file(hour_s);
}

void L2Writer::run() {
    uint32_t last_sync = 0;

    while (true) {
        if (stop_.load(std::memory_order_acquire)) {
            if (auto row = queue_.dequeue()) {
                const uint64_t h = hour_start_from_ns(row->ts_ns);
                if (hour_start_ != h) {
                    if (!rotate_to_hour(h)) {
                        dropped_.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                    last_sync = 0;
                }
                uint64_t idx = rows_.load(std::memory_order_relaxed);
                if (idx >= capacity_) {
                    dropped_.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
                ts_[idx] = row->ts_ns;
                price_[idx] = row->price;
                qty_[idx] = row->qty;
                side_[idx] = row->side;
                rows_.store(idx + 1, std::memory_order_release);
                hdr_.rows = idx + 1;
                continue;
            } else {
                break;
            }
        }

        auto row = queue_.dequeue();
        if (!row) {
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            continue;
        }

        const uint64_t h = hour_start_from_ns(row->ts_ns);
        if (hour_start_ != h) {
            if (!rotate_to_hour(h)) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            last_sync = 0;
        }

        uint64_t idx = rows_.load(std::memory_order_relaxed);
        if (idx >= capacity_) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }

        ts_[idx] = row->ts_ns;
        price_[idx] = row->price;
        qty_[idx] = row->qty;
        side_[idx] = row->side;

        rows_.store(idx + 1, std::memory_order_release);
        hdr_.rows = idx + 1;

        if (opt_.fsync_every_rows && ++last_sync >= opt_.fsync_every_rows) {
            (void)update_rows_in_header();
            ::fdatasync(fd_);
            last_sync = 0;
        }
    }

    close_file();
}
