#pragma once
#include "status.hh"
#include "operation.hh"
#include "task.hh"
#include "util.hh"
#include <vector>

class Transaction
{
public:
    Status status_;
    std::vector<Task> task_set_;
    std::vector<ReadOperation> read_set_;
    std::vector<WriteOperation> write_set_;
    Transaction() : status_(Status::IN_FLIGHT){};
    void read(const uint64_t key);
    void write(const uint64_t key);
    void update();
    void ReserveWrite(uint32_t my_tid, uint32_t my_batch_id, size_t thread_id_);
    void ReserveRead(uint32_t my_tid, uint32_t my_batch_id);
    bool WAW(uint32_t my_tid, uint32_t my_batch_id);
    bool RAW(uint32_t my_tid, uint32_t my_batch_id, std::array<uint32_t, THREAD_NUM> &aborted_list);
    bool WAR(uint32_t my_tid, uint32_t my_batch_id);
    void begin();
    void commit();
    void abort();
};