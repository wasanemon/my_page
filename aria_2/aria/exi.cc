// #include "gflags/gflags.h"
#include "../include/zipf.hh"
#include "../include/random.hh"

#include <iostream>
#include <vector>
#include <thread>
#include <functional>
#include <atomic>
#include <string>
#include <algorithm>
#include <utility>
#include <barrier>
#include <array>
#include <mutex>

#define PAGE_SIZE 4096
#define THREAD_NUM 64
#define TUPLE_NUM 1000000
#define MAX_OPE 100
#define RW_RATE 95
#define EX_TIME 3
#define PRE_NUM 100000
#define SLEEP_TIME 0
#define SLEEP_TIME_INIT 2900 * 1000
//#define SKEW_PAR 0.80
#define BACKOFF_TIME 0
#define SLEEP_RATE 0

double SKEW_PAR = 0.99;



uint64_t tx_counter;

// DEFINE_uint64(tuple_num, 1000000, "Total number of records");

class Result
{
public:
    uint64_t commit_cnt_;
};

std::vector<Result> AllResult(THREAD_NUM);

enum class Ope
{
    READ,
    WRITE,
    SLEEP,
};
class Task
{
public:
    Ope ope_;
    uint64_t key_;

    Task() : ope_(Ope::READ), key_(0) {}

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
};

// transaction取得においてのみ用いる
class RWLock
{
public:
    std::atomic<int> counter;
    RWLock() { counter.store(0, std::memory_order_release); }

    bool w_try_lock()
    {
        int expected, desired(-1);
        expected = counter.load(std::memory_order_acquire);
        for (;;)
        {
            if (expected != 0)
            {
                return false;
            }

            if (counter.compare_exchange_strong(
                    expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                return true;
            }
        }
    }
    void w_unlock()
    {
        counter++;
    }
};

RWLock lock_for_locks;

class Tuple
{
public:
    std::atomic<uint64_t> w_lock_;
    std::atomic<uint64_t> r_lock_;
    uint64_t value_;
    std::atomic<uint32_t> r_tid_;
    std::atomic<uint32_t> w_tid_;
    std::atomic<uint32_t> w_batch_id_;
    std::atomic<uint32_t> r_batch_id_;
    // size_t thread_id_;
};

class ReadOperation
{
public:
    uint64_t key_;
    uint64_t value_;
    Tuple *tuple_;

    ReadOperation(uint64_t key, uint64_t value, Tuple *tuple) : key_(key), value_(value), tuple_(tuple) {}
};

class WriteOperation
{
public:
    uint64_t key_;
    uint64_t value_;
    Tuple *tuple_;

    WriteOperation(uint64_t key, uint64_t value, Tuple *tuple) : key_(key), value_(value), tuple_(tuple) {}
};

Tuple *Table;

enum class Status
{
    IN_FLIGHT,
    COMMITTED,
    ABORTED
};

class Pre
{
public:
    std::vector<Task> task_set_;
};

std::vector<std::pair<Pre, uint32_t>> Pre_tx_set(PRE_NUM);

class Transaction
{
public:
    Status status_;
    std::vector<Task> task_set_;
    std::vector<ReadOperation> read_set_;
    std::vector<WriteOperation> write_set_;
    Transaction() : status_(Status::IN_FLIGHT) {}

    void read(const uint64_t key)
    {
        Tuple *tuple = &Table[key];
        uint64_t read_value = tuple->value_;
        read_set_.emplace_back(key, read_value, tuple);

        return;
    }

    void write(const uint64_t key)
    {
        Tuple *tuple = &Table[key];
        uint64_t write_value = 100;
        write_set_.emplace_back(key, write_value, tuple);
        return;
    }

    void update()
    {
        for (auto &wset : write_set_)
        {
            wset.tuple_->value_ = wset.value_;
        }
        return;
    }

    // write reservationを実行。関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
    void ReserveWrite(uint32_t my_tid, uint32_t my_batch_id)
    {
        uint64_t expected_lock = 0;
        for (auto &wset : write_set_)
        {
            expected_lock = 0;
            // lockを取得できるまで先に進まない

            while (!wset.tuple_->w_lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) // lock取得
            {
                expected_lock = 0;
            }
            // すでに同一batch内の自身より小さいtidを持つTxによってreservationされていて、reservationが失敗する場合
            if (wset.tuple_->w_tid_ < my_tid && wset.tuple_->w_batch_id_ == my_batch_id)
            {
                wset.tuple_->w_lock_.store(0, std::memory_order_release);
                continue;
            }
            // 同一batch内で自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
            if (wset.tuple_->w_tid_ == my_tid && wset.tuple_->w_batch_id_ == my_batch_id)
            {
                wset.tuple_->w_lock_.store(0, std::memory_order_release);
                continue;
            }

            // write set内のdata itemについて、引数のmy_tidとそのdata itemのw_tidを比較し、my_tidがw_tidよりも小さければ、my_tidでw_tidを更新する
            // 例外 : w_tidが以前のbatchにおいて書かれていたものであった場合、もしくはw_tidが初期値だった場合においては、tidの大小関係に関わらずw_tidを更新する
            if (wset.tuple_->w_tid_ > my_tid || wset.tuple_->w_batch_id_ < my_batch_id || wset.tuple_->w_tid_ == 0)
            {
                wset.tuple_->w_tid_ = my_tid;               // w_tid_の更新
                if (wset.tuple_->w_batch_id_ < my_batch_id) // そのbatch内において初めてw_tid_の更新が行われる場合にw_batch_id_も更新する
                {
                    wset.tuple_->w_batch_id_ = my_batch_id;
                }
            }

            wset.tuple_->w_lock_.store(0, std::memory_order_release); // unlock
        }
        return;
    }

    // read reservationを実行。関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
    void ReserveRead(uint32_t my_tid, uint32_t my_batch_id)
    {
        uint64_t expected_lock = 0;
        // lockを取得できるまで進まない
        for (auto &rset : read_set_)
        {
            expected_lock = 0;
            while (!rset.tuple_->r_lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) // lock取得
            {
                expected_lock = 0;
            }

            // すでに同一batch内の自身より小さいtidを持つTxによってreservationされていて、reservationが失敗する場合
            if (rset.tuple_->r_tid_ < my_tid && rset.tuple_->r_batch_id_ == my_batch_id)
            {
                rset.tuple_->r_lock_.store(0, std::memory_order_release);
                continue;
            }

            // 同一batch内で自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
            if (rset.tuple_->r_tid_ == my_tid && rset.tuple_->r_batch_id_ == my_batch_id)
            {
                rset.tuple_->r_lock_.store(0, std::memory_order_release);
                continue;
            }
            // read set内のdata itemについて、引数のmy_tidとそのdata itemのr_tidを比較し、my_tidがr_tidよりも小さければ、my_tidでr_tidを更新する
            // 例外 : r_tidが以前のbatchにおいて書かれていたものであった場合、もしくはr_tidが初期値だった場合においては、tidの大小関係に関わらずr_tidを更新する
            if (rset.tuple_->r_tid_ > my_tid || rset.tuple_->r_batch_id_ < my_batch_id || rset.tuple_->r_tid_ == 0) // reservationが成功する場合
            {
                rset.tuple_->r_tid_ = my_tid; // r_tid_の更新
                // そのbatch内において初めてr_tid_の更新が行われる場合にr_batch_id_も更新する
                if (rset.tuple_->r_batch_id_ < my_batch_id)
                {
                    rset.tuple_->r_batch_id_ = my_batch_id;
                }
            }
            rset.tuple_->r_lock_.store(0, std::memory_order_release);
        }
        return;
    }

    // 関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
    bool WAW(uint32_t my_tid, uint32_t my_batch_id)
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
    bool RAW(uint32_t my_tid, uint32_t my_batch_id)
    {
        // 関数を実行したTxのread set内のdataのw_tidと引数にとったmy_tidを比較する。
        for (auto &rset : read_set_)
        {
            if (my_tid > rset.tuple_->w_tid_ && rset.tuple_->w_batch_id_ == my_batch_id && rset.tuple_->w_tid_ != 0) // rawの確認
            {
                return true;
            }
        }
        return false;
    }

    // 関数を実行するTxのtid(my_tid)、そのTxが実行されているbatch_id(my_batch_id)を引数にとる
    bool WAR(uint32_t my_tid, uint32_t my_batch_id)
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

    void begin()
    {
        status_ = Status::IN_FLIGHT;
        return;
    }

    void commit()
    {
        task_set_.clear();
        read_set_.clear();
        write_set_.clear();
        return;
    }
    void abort()
    {
        task_set_.clear();
        read_set_.clear();
        write_set_.clear();
        return;
    }
};

void makeSleep(std::vector<Task> &tasks, Xoroshiro128Plus &rnd, FastZipf &zipf)
{
    tasks.clear();
    tasks.emplace_back(Ope::SLEEP, 0);
}

void makeTask(std::vector<Task> &tasks, Xoroshiro128Plus &rnd, FastZipf &zipf)
{
    tasks.clear();
    for (size_t i = 0; i < MAX_OPE; ++i)
    {
        uint64_t random_gen_key = zipf();
        // std::cout << random_gen_key << std::endl;
        assert(random_gen_key < TUPLE_NUM);
        if ((rnd.next() % 100) < RW_RATE)
        {
            tasks.emplace_back(Ope::READ, random_gen_key);
        }
        else
        {
            tasks.emplace_back(Ope::WRITE, random_gen_key);
        }
    }
}

void makeTask_init(std::vector<Task> &tasks, Xoroshiro128Plus &rnd, FastZipf &zipf)
{
    tasks.clear();
    uint64_t random_gen_key = zipf() % 1;
    // std::cout << random_gen_key << std::endl;
    tasks.emplace_back(Ope::WRITE, 1);
    tasks.emplace_back(Ope::WRITE, 1);
}

void makeDB()
{
    posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    for (int i = 0; i < TUPLE_NUM; i++)
    {
        Table[i].w_lock_ = 0;
        Table[i].r_lock_ = 0;
        Table[i].value_ = 0;
        Table[i].r_tid_ = 0;
        Table[i].w_tid_ = 0;
        Table[i].w_batch_id_ = 0;
        Table[i].r_batch_id_ = 0;
    }
}

std::atomic<uint64_t> tx_lock = 0;
void worker(int thread_id, int &ready, const bool &start, const bool &quit, std::barrier<> &sync_point)
{
    Result &myres = std::ref(AllResult[thread_id]);
    Transaction trans;
    uint32_t batch_id = 0;
    uint64_t tx_pos;
    uint64_t sleep_flg = 0;
    uint64_t expected_lock = 0;
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);

    // Thread starts
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }

//POINT:

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))
    {
        // sequencing layer starts

        // １つ前のepochにおいて、abortしていなかった場合、新たなTxを取得、実行する
        if (trans.status_ != Status::ABORTED)
        {
            // aquire giant lock
            expected_lock = 0;
            while (!tx_lock.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) // lock取得
            {
                expected_lock = 0;
            }
            //if (!lock_for_locks.w_try_lock())
            //{
                //std::this_thread::sleep_for(std::chrono::microseconds(BACKOFF_TIME));
                //goto POINT;
            //}
            tx_pos = __atomic_load_n(&tx_counter, __ATOMIC_SEQ_CST);
            if (tx_pos >= PRE_NUM)
            {
                return;
            }
            __atomic_store_n(&tx_counter, tx_pos + 1, __ATOMIC_SEQ_CST);
            //lock_for_locks.w_unlock();
            tx_lock.store(0, std::memory_order_release);
        }
        // Txの実行内容(task_set)の取得、tidの取得、batch_idの更新
        trans.task_set_ = Pre_tx_set[tx_pos].first.task_set_;
        uint32_t tid = Pre_tx_set[tx_pos].second;
        batch_id++;
        //sleep_flg = 0;
        trans.begin();
        // sequencing layer ends

        // execution phase starts
        // make read & write set
        
        for (auto &task : trans.task_set_)
        {
            switch (task.ope_)
            {
            case Ope::READ:
                trans.read(task.key_);
                break;
            case Ope::WRITE:
                trans.write(task.key_);
                break;
            case Ope::SLEEP:
                sleep_flg = 1;
                break;
            default:
                std::cout << "fail" << std::endl;
                break;
            }
        }
        
        // do W & R reservation
        trans.ReserveWrite(tid, batch_id);
        trans.ReserveRead(tid, batch_id);
        
        // 性能計測用
        //if (sleep_flg == 1)
        //{
            //std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_TIME));
        //}
        // execution phase ends
        sync_point.arrive_and_wait();
        

        // commit phase starts
        // check waw conflict
        if (trans.WAW(tid, batch_id))
        {
            trans.status_ = Status::ABORTED;
        }
        // WAW関数においてabortしなかったTxのみ、WAR & RAWを実行する。Reordering
        if (trans.status_ != Status::ABORTED)
        {
            if (trans.RAW(tid, batch_id))
            {
                if (trans.WAR(tid, batch_id))
                {
                    // RAW() と WAR() が両方Trueの場合に、Reordering失敗,abortとなる
                    trans.status_ = Status::ABORTED;
                }
            }
        }

        // commitしたTxはupdateを実行
        if (trans.status_ == Status::ABORTED)
        {
            trans.abort();
        }
        else
        {
            //trans.update();
            trans.commit();
        }
        // commit phase ends
        sync_point.arrive_and_wait();

        if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && trans.status_ != Status::ABORTED)
        {
            myres.commit_cnt_++;
        }
    }
    // 各threadがworker関数を抜ける際に、他の同期ポイントで永遠に待つことがないようにするためのもの
    sync_point.arrive_and_drop();

}

int main(int argc, char *argv[])
{
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, SKEW_PAR, TUPLE_NUM);

    makeDB();

    std::barrier sync_point(THREAD_NUM);
    tx_counter = 0;

    bool start = false;
    bool quit = false;

    // transaction generate

    uint32_t tid = 1;
    int tx_make_count = 0;
    for (auto &pre : Pre_tx_set)
    {

        if (rnd.next() % 100 < SLEEP_RATE)
        {
            makeSleep(pre.first.task_set_, rnd, zipf);
        }
        else
        {
            makeTask(pre.first.task_set_, rnd, zipf);
        }
        pre.second = tid;
        tx_make_count++;
        tid++;
    }
    

    std::vector<int> readys;
    for (size_t i = 0; i < THREAD_NUM; ++i)
    {
        readys.emplace_back(0);
    }

    std::vector<std::thread> thv;
    for (size_t i = 0; i < THREAD_NUM; ++i)
    {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(sync_point));
    }

    while (true)
    {
        bool failed = false;
        for (auto &re : readys)
        {
            if (!__atomic_load_n(&re, __ATOMIC_SEQ_CST))
            {
                failed = true;
                break;
            }
        }
        if (!failed)
        {
            break;
        }
    }
    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000 * EX_TIME));

    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);

    for (auto &th : thv)
    {
        th.join();
    }

    uint64_t total_count = 0;
    for (auto &re : AllResult)
    {
        total_count += re.commit_cnt_;
    }

    // float tps = total_count / (SLEEP_TIME_INIT / 1000 / 1000);
    //std::cout << "throughput exi:" << SKEW_PAR << " " << total_count / EX_TIME << " " << result << " " <<  batch__ <<  std::endl;
    std::cout << SKEW_PAR << " " << total_count / EX_TIME << std::endl;
    return 0;
}
