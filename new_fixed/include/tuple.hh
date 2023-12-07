#pragma once
#include <string>
#include <atomic>

class Tuple
{
public:
    std::atomic<uint64_t> lock_;
    uint64_t value_;
    std::atomic<uint32_t> r_tid_;
    std::atomic<uint32_t> w_tid_;
    std::atomic<uint32_t> w_batch_id_;
    std::atomic<uint32_t> r_batch_id_;
    size_t thread_id_;
};
