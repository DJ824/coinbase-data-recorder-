#pragma once

#include "spsc.h"

#include <libwebsockets.h>
//#include <simdjson.h>

#include <atomic>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>

#include "l2_writer.h"

struct Config {
    std::string pair;
};

struct CoinbaseCredentials {
    std::string key_name;
    std::string private_key;
};

class CoinbaseFeed final  {
    lws_context* ctx_ = nullptr;
    lws* client_ = nullptr;
    std::string rx_buf_;
    std::string tx_buf_;
    //simdjson::ondemand::parser parser_;
    std::optional<CoinbaseCredentials> creds_;
    const std::string product_id_;
    const uint32_t price_scale_;
    lws_ring* ring_ = nullptr;
    uint32_t ring_tail_ = 0;
    struct PSD {
        CoinbaseFeed* self;
    };

    std::string root_ = []{
        if (const char* home = std::getenv("HOME")) {
            return (std::filesystem::path(home) / "hft-data").string();
        }
        return std::string("/tmp/hft-data");
    }();

    L2Writer writer_;
    uint64_t open_hour_{~0ull};
    uint64_t cap_estimate_{5'000'000};


    static inline uint64_t hour_start_from_ns(uint64_t ts_ns) noexcept {
        const uint64_t sec = ts_ns / 1'000'000'000ull;
        return sec - (sec % 3600ull);
    }

    std::string path_for_ts(uint64_t ts_ns) const;
    void ensure_writer_for(uint64_t ts_ns);


    int ct = 0;


    enum class FeedState { Disconnected, Connected, Subscribed };
    //std::vector<double> latencies_{10000};
    void run();
    static int lws_cb(lws*, lws_callback_reasons, void*, void*, size_t);
    void subscribe_to_level2();
    void handle_level2_update(const char* buf, size_t len);
    //void handle_level2(const char* json, size_t len);
    bool send_text(const std::string&);
    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> run_thread_;
    FeedState state_{FeedState::Disconnected};

public:
    explicit CoinbaseFeed(const Config& cfg);
    ~CoinbaseFeed();
    void start();
    void stop();
    void join();
};

