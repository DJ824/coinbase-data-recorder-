#include <csignal>
#include <iostream>
#include <atomic>
#include <thread>
#include <chrono>
#include "coinbase_feed.h"

std::atomic<bool> shutdown_requested{false};

void signal_handler(int signal) {
    std::cout << "\n[main] Shutdown signal received (" << signal << ")\n";
    shutdown_requested.store(true);
}

int main() {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    std::cout << "[main] Starting BTC-USD data recorder...\n";

    try {
        Config config;
        config.pair = "BTC-USD";

        CoinbaseFeed feed(config);

        std::cout << "[main] Starting feed connection...\n";
        feed.start();

        std::cout << "[main] Recording data. Press Ctrl+C to stop.\n";
        std::cout << "[main] Data will be saved to ~/hft-data/ directory\n";

        while (!shutdown_requested.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }

        std::cout << "[main] Stopping feed...\n";
        feed.stop();
        feed.join();

        std::cout << "[main] Data recording stopped. Files saved to ~/hft-data/\n";

    } catch (const std::exception& e) {
        std::cerr << "[main] Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}