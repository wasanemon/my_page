#include "../include/status.hh"
#include "../include/tuple.hh"
#include "../include/operation.hh"
#include "../include/task.hh"
#include "../include/transaction.hh"
#include "../include/util.hh"
#include <string>

Tuple *Table;

void Transaction::read(const uint64_t key)
{
    Tuple *tuple = &Table[key];
    uint64_t read_value = tuple->value_;
    read_set_.emplace_back(key, read_value, tuple);
    return;
}

void Transaction::write(const uint64_t key)
{
    Tuple *tuple = &Table[key];
    uint64_t write_value = 100;
    write_set_.emplace_back(key, write_value, tuple);
    return;
}

void Transaction::update()
{
    for (auto &wset : write_set_)
    {
        wset.tuple_->value_ = wset.value_;
    }
    return;
}

// write reservationを実行。関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
void Transaction ::ReserveWrite(uint32_t my_tid, uint32_t my_batch_id, size_t my_thread_id)
{
    uint64_t expected_lock = 0;
    for (auto &wset : write_set_)
    {
        expected_lock = 0;
        while (!wset.tuple_->lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) // lock取得
        {
            expected_lock = 0;
        }
        // 条件チェック
        if (wset.tuple_->w_tid_ < my_tid && wset.tuple_->w_batch_id_ == my_batch_id) // すでに他のsmall tidを持つTxによってreservationされていて、reservationが失敗する場合
        {
            wset.tuple_->lock_.store(0, std::memory_order_release);
            continue;
        }
        if (wset.tuple_->w_tid_ == my_tid && wset.tuple_->w_batch_id_ == my_batch_id) // 自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
        {
            wset.tuple_->lock_.store(0, std::memory_order_release);
            continue;
        }
        if (wset.tuple_->w_tid_ > my_tid || wset.tuple_->w_batch_id_ < my_batch_id || wset.tuple_->w_tid_ == 0) // reservationが成功する場合
        {
            wset.tuple_->w_tid_ = my_tid;               // w_tid_の更新
            wset.tuple_->thread_id_ = my_thread_id;     // thread_id_の更新
            if (wset.tuple_->w_batch_id_ < my_batch_id) // そのbatch内において初めてw_tid_の更新が行われる場合にw_batch_id_も更新する
            {
                wset.tuple_->w_batch_id_ = my_batch_id;
            }
        }
        wset.tuple_->lock_.store(0, std::memory_order_release); // unlock
    }
    return;
}

void Transaction::ReserveRead(uint32_t my_tid, uint32_t my_batch_id)
{
    uint64_t expected_lock = 0;
    for (auto &rset : read_set_)
    {
        expected_lock = 0;
        while (!rset.tuple_->lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) // lock取得
        {
            expected_lock = 0;
        }
        // 条件チェック
        if (rset.tuple_->r_tid_ < my_tid && rset.tuple_->r_batch_id_ == my_batch_id) // すでに他のsmall tidを持つTxによってreservationされていて、reservationが失敗する場合
        {
            rset.tuple_->lock_.store(0, std::memory_order_release);
            continue;
        }
        if (rset.tuple_->r_tid_ == my_tid && rset.tuple_->r_batch_id_ == my_batch_id) // 自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
        {
            rset.tuple_->lock_.store(0, std::memory_order_release);
            continue;
        }
        if (rset.tuple_->r_tid_ > my_tid || rset.tuple_->r_batch_id_ < my_batch_id || rset.tuple_->r_tid_ == 0) // reservationが成功する場合
        {
            rset.tuple_->r_tid_ = my_tid;               // r_tid_の更新
            if (rset.tuple_->r_batch_id_ < my_batch_id) // そのbatch内において初めてr_tid_の更新が行われる場合にr_batch_id_も更新する
            {
                rset.tuple_->r_batch_id_ = my_batch_id;
            }
        }
        rset.tuple_->lock_.store(0, std::memory_order_release); // unlock
    }
    return;
}

// 関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
bool Transaction::WAW(uint32_t my_tid, uint32_t my_batch_id)
{
    // 関数を実行したTxのwrite set内のdataのw_tidと引数にとったmy_tidを比較する。
    for (auto &wset : write_set_)
    {
        if (wset.tuple_->w_tid_ != my_tid || wset.tuple_->w_batch_id_ != my_batch_id) // wawの確認
        {
            return true;
        }
    }
    return false;
}

// 関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
bool Transaction::RAW(uint32_t my_tid, uint32_t my_batch_id, std::array<uint32_t, THREAD_NUM> &aborted_list)
{
    for (auto &rset : read_set_)
    {
        if (my_tid > rset.tuple_->w_tid_ && rset.tuple_->w_batch_id_ == my_batch_id && rset.tuple_->w_tid_ != 0) // rawが存在する場合
        {
            if (aborted_list[rset.tuple_->thread_id_] == 0) // 対象のdata項目をreservationしていたTxがabortしていた場合、rawにはならない
            {
                return true;
            }
        }
    }

    return false;
}

// 関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
bool Transaction::WAR(uint32_t my_tid, uint32_t my_batch_id)
{
    // 関数を実行したTxのwrite set内のdataのr_tidと、引数にとったmy_tidを比較する。
    for (auto &wset : write_set_)
    {
        if (my_tid > wset.tuple_->r_tid_ && wset.tuple_->r_batch_id_ == my_batch_id && wset.tuple_->r_tid_ != 0) // warの確認
        {
            return true;
        }
    }
    return false;
}

void Transaction::begin()
{
    status_ = Status::IN_FLIGHT;
    return;
}

void Transaction::commit()
{
    read_set_.clear();
    write_set_.clear();
    return;
}
void Transaction::abort()
{
    read_set_.clear();
    write_set_.clear();
    return;
}