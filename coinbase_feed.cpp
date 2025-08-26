#include <immintrin.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include "coinbase_feed.h"
#include <curl/curl.h>
#include <netinet/tcp.h>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <sys/mman.h>


using namespace std::chrono;

// template <uint32_t SCALE = 100>
// inline uint64_t parse_price_snapshot(const char* p, size_t n) noexcept {
//     double d;
//     auto [_, ec] = fast_float::from_chars(p, p + n, d);
//     if (ec != std::errc()) {
//         return 0;
//     }
//
//     return static_cast<uint64_t>(std::llround(d * SCALE));
// }
//
// inline float parse_qty_snapshot(const char* p, size_t n) noexcept {
//     float q;
//     auto [_, ec] = fast_float::from_chars(p, p + n, q);
//     return (ec == std::errc()) ? q : 0.0f;
// }


std::string CoinbaseFeed::path_for_ts(uint64_t ts_ns) const {
    const time_t sec = static_cast<time_t>(ts_ns / 1'000'000'000ull);
    struct tm tm{};
    localtime_r(&sec, &tm);

    char day[9];
    char hhmm[5];

    std::snprintf(day, sizeof(day), "%02d%02d%04d", tm.tm_mon + 1, tm.tm_mday, tm.tm_year + 1900);
    std::snprintf(hhmm, sizeof(hhmm), "%02d00", tm.tm_hour);

    std::filesystem::path dir = std::filesystem::path(root_) / day;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);

    return (dir / hhmm).string();
}


inline uint64_t now_ns() {
    return duration_cast<nanoseconds>(
        high_resolution_clock::now().time_since_epoch()).count();
}

CoinbaseFeed::CoinbaseFeed(const Config& cfg)
    : product_id_{cfg.pair}
      , price_scale_(100), writer_{L2WriterOpt{root_, product_id_}} {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    const char* k = std::getenv("COINBASE_KEY_NAME");
    const char* p = std::getenv("COINBASE_PRIVATE_KEY");
    if (k && p) {
        creds_ = CoinbaseCredentials{k, p};
    }

    mlockall(MCL_CURRENT | MCL_FUTURE);
}

CoinbaseFeed::~CoinbaseFeed() {
    stop();
    writer_.stop();
    open_hour_ = ~0ull;
    if (run_thread_ && run_thread_->joinable()) run_thread_->join();
    curl_global_cleanup();
}

void CoinbaseFeed::start() {
    if (running_.exchange(true)) {
        return;
    }
    run_thread_ = std::make_unique<std::thread>(&CoinbaseFeed::run, this);
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(0, &mask);
    int rc = pthread_setaffinity_np(run_thread_->native_handle(), sizeof(mask), &mask);
    if (rc != 0) {
        std::cerr << "[CoinbaseFeed] unable to pin cpu" << std::strerror(rc) << std::endl;
    }
    else {
        std::cout << "[CoinbaseFeed] cpu pinned to core 0" << std::endl;
    }
    writer_.start();
    //  sched_param sch{ .sched_priority = 80 };
    //  if (pthread_setschedparam(run_thread_->native_handle(),
    //                            SCHED_FIFO, &sch) != 0) {
    //      perror("pthread_setschedparam");
    //                            }
    // std::cout << "scheduled fifo + high priority" << std::endl;
}

void CoinbaseFeed::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    if (ctx_) {
        lws_cancel_service(ctx_);
    }


    // // FILE* f = fopen("latencies.csv", "w");
    // // for (double t : latencies_) {
    // //     fprintf(f, "%f\n", t);
    // // }
    // fclose(f);
}

void CoinbaseFeed::join() {
    if (run_thread_ && run_thread_->joinable()) run_thread_->join();
}

static inline void save_snapshot_json(const char* data, size_t len) {
    std::ofstream f("snapshot.json", std::ios::binary);
    f.write(data, static_cast<std::streamsize>(len));
}


int CoinbaseFeed::lws_cb(lws* wsi, lws_callback_reasons why,
                         void* user, void* in, size_t len) {
    auto* self = static_cast<CoinbaseFeed*>(user);

    switch (why) {
    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        {
            self->client_ = wsi;
            self->state_ = FeedState::Connected;
            std::cout << "[CoinbaseFeed] OPEN\n";
            int one = 1;
            setsockopt(lws_get_socket_fd(wsi),IPPROTO_TCP,TCP_NODELAY, &one, sizeof(one));
            int prio = 6;
            setsockopt(lws_get_socket_fd(wsi), SOL_SOCKET, SO_PRIORITY, &prio, sizeof(prio));

            int tos = IPTOS_LOWDELAY;
            setsockopt(lws_get_socket_fd(wsi), IPPROTO_IP, IP_TOS, &tos, sizeof(tos));
            self->subscribe_to_level2();
            break;
        }

    case LWS_CALLBACK_CLIENT_RECEIVE:
        {
            auto* self = static_cast<CoinbaseFeed*>(user);
            bool first = lws_is_first_fragment(wsi);
            bool final = lws_is_final_fragment(wsi);

            if (first && final) {
                // self->ct++;
                // uint64_t t0 = tsc();
                self->handle_level2_update(static_cast<char*>(in), len);
                // uint64_t t1 = tsc();
                // double time = cycles_to_ns(t1 - t0);
                // self->latencies_.push_back(time);
                // if (self->ct == 500) {
                //     int sum = 0;
                //     for (auto& i : self->latencies_) {
                //         sum += i;
                //     }
                //     double avg = static_cast<double>(sum) / self->latencies_.size();
                //     std::cout << "average latency is " << avg << std::endl;
                //     std::cout << "num samples " << self->latencies_.size() << std::endl;
                //     exit(0);
                // }
                break;
            }

            // if fragmented message, build in receive buffer
            self->rx_buf_.append(static_cast<char*>(in), len);

            if (final) {
                self->handle_level2_update(self->rx_buf_.data(), self->rx_buf_.size());
                self->rx_buf_.clear();
            }
        }
        break;

    case LWS_CALLBACK_CLIENT_WRITEABLE:
        if (!self->tx_buf_.empty()) {
            std::vector<unsigned char> buf(LWS_PRE + self->tx_buf_.size());
            std::memcpy(buf.data() + LWS_PRE,
                        self->tx_buf_.data(), self->tx_buf_.size());
            lws_write(wsi, buf.data() + LWS_PRE,
                      self->tx_buf_.size(), LWS_WRITE_TEXT);
            self->tx_buf_.clear();
        }
        break;

    case LWS_CALLBACK_CLIENT_CLOSED:
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        {
            std::cerr << "[CoinbaseFeed] CLOSED/ERROR\n";
            self->running_ = false;
            break;
        }

    default: break;
    }
    return 0;
}

void CoinbaseFeed::run() {
    lws_context_creation_info info{};
    info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT |
        LWS_SERVER_OPTION_VALIDATE_UTF8;
    info.port = CONTEXT_PORT_NO_LISTEN;

    static lws_protocols prot[] = {
        {
            "json", &CoinbaseFeed::lws_cb,
            sizeof(PSD), 8 * 1024, 0, nullptr, 0
        },
        {nullptr, nullptr, 0, 0, 0, nullptr, 0}
    };
    info.protocols = prot;
    info.user = this;

    ctx_ = lws_create_context(&info);
    if (!ctx_) {
        running_ = false;
        return;
    }

    lws_client_connect_info cc{};
    cc.context = ctx_;
    cc.address = "advanced-trade-ws.coinbase.com";
    cc.port = 443;
    cc.path = "/";
    cc.ssl_connection = LCCSCF_USE_SSL | LCCSCF_ALLOW_INSECURE;
    cc.host = cc.origin = cc.address;
    cc.protocol = "json";
    cc.userdata = this;
    cc.pwsi = &client_;

    if (!lws_client_connect_via_info(&cc)) {
        std::cerr << "[CoinbaseFeed] dial failed\n";
        running_ = false;
    }

    std::cout << "[CoinbaseFeed] event-loop running …\n";
    while (running_)
        lws_service(ctx_, 0);

    std::cout << "[CoinbaseFeed] event-loop exited\n";

    lws_context_destroy(ctx_);
    ctx_ = nullptr;
}

bool CoinbaseFeed::send_text(const std::string& p) {
    tx_buf_ = p;
    if (client_) {
        lws_callback_on_writable(client_);
    }
    return true;
}

void CoinbaseFeed::subscribe_to_level2() {
    state_ = FeedState::Subscribed;
    std::string sub = R"({"type":"subscribe","product_ids":[")" + product_id_ +
        R"("],"channel":"level2"})";
    send_text(sub);
    std::cout << "[CoinbaseFeed] request sent for " << product_id_ << '\n';
}

void CoinbaseFeed::handle_level2_update(const char* buf, size_t len) {
    static constexpr char PREFIX[] = R"({"channel":"l2_data")";
    if (len < sizeof(PREFIX) - 1 || memcmp(buf, PREFIX, sizeof(PREFIX) - 1)) {
        return;
    }
    if (len >= 3 && buf[len - 3] == '[' && buf[len - 2] == ']') {
        return;
    }


    auto find_char_fast = [](const char* p, const char* end, char target) noexcept -> const char* {
        // if target is close by, fast path
        for (int i = 0; i < 8 && p < end; ++i, ++p) {
            if (*p == target) {
                return p;
            }
        }

        constexpr uint64_t m1 = 0x0101010101010101ULL;
        constexpr uint64_t m2 = 0x8080808080808080ULL;

        // 8 byte lanes, each lane stores target char
        const uint64_t rep = m1 * static_cast<unsigned char>(target);
        while (p + 8 <= end) {
            uint64_t w;
            memcpy(&w, p, 8);
            uint64_t x = w ^ rep;
            uint64_t z = (x - m1) & ~x & m2;
            if (z) {
                return p + (static_cast<unsigned>(__builtin_ctzll(z)) >> 3);
            }
            p += 8;
        }

        // const __m256i target_vec = _mm256_set1_epi8(target);
        // while (p + 32 <= end) {
        //     __m256i chunk = _mm256_loadu_si256((const __m256i*)p);
        //     __m256i matches = _mm256_cmpeq_epi8(chunk, target_vec);
        //     uint32_t mask = _mm256_movemask_epi8(matches);
        //     if (mask) {
        //         return p + __builtin_ctz(mask);
        //     }
        //     p += 32;
        // }

        while (p < end && *p != target) {
            ++p;
        }
        return (p < end) ? p : nullptr;
    };

    auto parse_price_fast = [](const char* p) -> uint32_t {
        uint32_t int_part = 0;
        while (*p >= '0' && *p <= '9') {
            int_part = int_part * 10 + (*p++ - '0');
        }
        if (*p == '.') {
            ++p;
            uint32_t frac_part = 0;
            if (*p >= '0' && *p <= '9') {
                frac_part += static_cast<uint32_t>(*p++ - '0') * 10u;
            }
            if (*p >= '0' && *p <= '9') {
                frac_part += static_cast<uint32_t>(*p - '0');
            }
            return int_part * 100u + frac_part;
        }
        return int_part * 100u;
    };

    auto parse_quantity_fast = [](const char* p) -> float {
        uint64_t int_part = 0;
        while (*p >= '0' && *p <= '9') {
            int_part = int_part * 10 + static_cast<uint64_t>(*p++ - '0');
        }
        if (*p != '.') {
            return static_cast<float>(int_part);
        }
        ++p;
        uint64_t frac_part = 0;
        int n = 0;
        while (*p >= '0' && *p <= '9' && n < 9) {
            frac_part = frac_part * 10 + static_cast<uint64_t>(*p++ - '0');
            // count how many fractional digits
            ++n;
        }
        static const float inv10[10] = {1.0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f};
        return static_cast<float>(int_part) + static_cast<float>(frac_part) * inv10[n];
    };

    auto days_from_civil = [](int y, unsigned m, unsigned d) noexcept -> int64_t {
        y -= (m <= 2);
        const int era = (y >= 0 ? y : y - 399) / 400;
        const unsigned yoe = static_cast<unsigned>(y - era * 400);
        const unsigned mp = m + (m > 2 ? -3 : 9);
        const unsigned doy = (153 * mp + 2) / 5 + d - 1;
        const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + yoe / 400 + doy;
        return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) - 719468;
    };

    auto parse_rfc3339_ns = [&](const char* ts, const char* endq) -> uint64_t {
        int y = (ts[0] - '0') * 1000 + (ts[1] - '0') * 100 + (ts[2] - '0') * 10 + (ts[3] - '0');
        int mo = (ts[5] - '0') * 10 + (ts[6] - '0');
        int d = (ts[8] - '0') * 10 + (ts[9] - '0');
        int hh = (ts[11] - '0') * 10 + (ts[12] - '0');
        int mm = (ts[14] - '0') * 10 + (ts[15] - '0');
        int ss = (ts[17] - '0') * 10 + (ts[18] - '0');

        thread_local int last_ymd = -1;
        thread_local int64_t last_days = 0;
        int ymd = y * 10000 + mo * 100 + d;
        if (ymd != last_ymd) {
            last_days = days_from_civil(y, static_cast<unsigned>(mo),
                                        static_cast<unsigned>(d));
            last_ymd = ymd;
        }

        uint32_t frac_ns = 0;
        const char* s = ts + 19;
        if (s < endq && *s == '.') {
            ++s;
            int n = 0;
            while (s < endq && n < 9 && *s >= '0' && *s <= '9') {
                frac_ns = frac_ns * 10u + static_cast<uint32_t>(*s - '0');
                ++s;
                ++n;
            }
            while (n < 9) {
                frac_ns *= 10u;
                ++n;
            }
        }
        int64_t secs = last_days * 86400 + hh * 3600 + mm * 60 + ss;
        return static_cast<uint64_t>(secs) * 1000000000ULL + frac_ns;
    };

    const char* end = buf + len;
    const char* p = buf;

    while (p < end - 11) {
        if (memcmp(p, "\"updates\":[", 11) == 0) {
            p += 11;
            break;
        }
        ++p;
    }

    if (p >= end - 11) {
        return;
    }

    while (p < end && *p != ']') {
        p = find_char_fast(p, end, '{');
        if (!p) {
            break;
        }
        ++p;

        const char* obj_end = find_char_fast(p, end, '}');
        if (!obj_end) {
            break;
        }

        const char* k = find_char_fast(p, obj_end, '"');
        if (!k) {
            p = obj_end + 1;
            continue;
        }

        // side
        const char* v = k + 1 + 4 + 2 + 1;
        bool is_bid = (*v == 'b');
        const char* v_end = find_char_fast(v, obj_end, '"');
        if (!v_end) {
            p = obj_end + 1;
            continue;
        }
        p = v_end + 1;

        // event_time
        k = find_char_fast(p, obj_end, '"');
        if (!k) {
            p = obj_end + 1;
            continue;
        }
        v = k + 1 + 10 + 2 + 1;
        const char* ts_end = find_char_fast(v, obj_end, '"');
        if (!ts_end) {
            p = obj_end + 1;
            continue;
        }
        uint64_t timestamp = parse_rfc3339_ns(v, ts_end);
        p = ts_end + 1;

        // price
        k = find_char_fast(p, obj_end, '"');
        if (!k) {
            p = obj_end + 1;
            continue;
        }
        v = k + 1 + 11 + 2 + 1;
        uint32_t price100 = parse_price_fast(v);
        v_end = find_char_fast(v, obj_end, '"');
        if (!v_end) {
            p = obj_end + 1;
            continue;
        }
        p = v_end + 1;

        // new quantity
        k = find_char_fast(p, obj_end, '"');
        if (!k) {
            p = obj_end + 1;
            continue;
        }
        v = k + 1 + 12 + 2 + 1;
        float qty;
        if (*v == '0' && v[1] != '.') {
            // if quantity is 0, skip to end
            qty = 0.0f;
            v_end = find_char_fast(v, obj_end, '"');
            if (!v_end) {
                p = obj_end + 1;
                continue;
            }
            p = v_end + 1;
        }
        else {
            qty = parse_quantity_fast(v);
            v_end = find_char_fast(v, obj_end, '"');
            if (!v_end) {
                p = obj_end + 1;
                continue;
            }
            p = v_end + 1;
        }

        //std::cout << timestamp << std::endl;

        writer_.enqueue({timestamp, price100, qty, is_bid});

        p = obj_end + 1;
    }
}
