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
#define MAX_OPE 10
#define RW_RATE 50
#define EX_TIME 3
#define PRE_NUM 1000000
#define SLEEP_TIME 1000000
#define SLEEP_TIME_INIT 2900 * 1000
#define SKEW_PAR 0.0
#define BACKOFF_TIME 0
#define SLEEP_RATE 1

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
    std::atomic<uint64_t> lock_;
    uint64_t value_;
    std::atomic<uint32_t> r_tid_;
    std::atomic<uint32_t> w_tid_;
    std::atomic<uint32_t> batch_id_w_;
    std::atomic<uint32_t> batch_id_r_;
    //size_t thread_id_;
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
        for(auto &wset : write_set_){
            wset.tuple_->value_ = wset.value_;
        }
        return;
    }

    void ReserveWrite(uint32_t my_tid, uint32_t my_batch_id) 
    {
        uint64_t expected_lock = 0;
        for(auto &wset : write_set_){
            expected_lock = 0;
            while (!wset.tuple_->lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) 
            {
                expected_lock = 0;
            }
            // 条件チェック
            if (wset.tuple_->w_tid_ < my_tid && wset.tuple_->batch_id_w_ == my_batch_id) 
            {
                wset.tuple_->lock_.store(0, std::memory_order_release); 
                continue;
            }
            if(wset.tuple_->w_tid_ == my_tid && wset.tuple_->batch_id_w_ == my_batch_id){
                wset.tuple_->lock_.store(0, std::memory_order_release);
                continue;
            }

            //自分のtidの方が小さい、もしくは自分のbatchの方が小さい、もしくは、tidが初期値
            if (wset.tuple_->w_tid_ > my_tid || wset.tuple_->batch_id_w_ < my_batch_id ||wset.tuple_->w_tid_ == 0)
            {
                wset.tuple_->w_tid_ = my_tid;
                if(wset.tuple_->batch_id_w_ < my_batch_id){
                    wset.tuple_->batch_id_w_ = my_batch_id;
                }
            }
  
            wset.tuple_->lock_.store(0, std::memory_order_release);
        }
        return ;
    }

    void ReserveRead(uint32_t my_tid,  uint32_t my_batch_id) 
    {
        uint64_t expected_lock = 0;
        for(auto &rset : read_set_){
            expected_lock = 0;
            while (!rset.tuple_->lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) 
            {
                expected_lock = 0; 
            }
            // 条件チェック
            if (rset.tuple_->r_tid_ < my_tid && rset.tuple_->batch_id_r_ == my_batch_id) 
            {
                rset.tuple_->lock_.store(0, std::memory_order_release); 
                continue;
            }
            if(rset.tuple_->r_tid_ == my_tid && rset.tuple_->batch_id_r_ == my_batch_id){
                rset.tuple_->lock_.store(0, std::memory_order_release);
                continue;
            }
            if (rset.tuple_->r_tid_ > my_tid || rset.tuple_->batch_id_r_ < my_batch_id || rset.tuple_->r_tid_ == 0) 
            {
                rset.tuple_->r_tid_ = my_tid;
                if(rset.tuple_->batch_id_r_ < my_batch_id){
                    rset.tuple_->batch_id_r_ = my_batch_id;
                }
            }
            rset.tuple_->lock_.store(0, std::memory_order_release);
        }
        return ;
    }

    bool WAW(uint32_t my_tid, uint32_t my_batch_id)
    {
        for(auto &wset : write_set_){
            if(wset.tuple_->w_tid_ != my_tid || wset.tuple_->batch_id_w_ != my_batch_id){
                return true;
            }
        }
        return false;
    }

    bool RAW(uint32_t my_tid, uint32_t my_batch_id)
    {
        for(auto &rset : read_set_){
            if(my_tid > rset.tuple_->w_tid_ && rset.tuple_->batch_id_w_ == my_batch_id && rset.tuple_->w_tid_ != 0)
            {
                    return true;
            }
        }
        return false;
    }

    bool WAR(uint32_t my_tid, uint32_t my_batch_id)
    {
        for(auto &wset : write_set_){
            if(my_tid > wset.tuple_->r_tid_ && wset.tuple_->batch_id_r_ == my_batch_id && wset.tuple_->r_tid_ != 0)
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
        read_set_.clear();
        write_set_.clear();
        return;
    }
    void abort()
    {
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
            tasks.emplace_back(Ope::READ, random_gen_key + 1);
        }
        else
        {
            tasks.emplace_back(Ope::WRITE, random_gen_key + 1);
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
        Table[i].lock_ = 0;
        Table[i].value_ = 0;
        Table[i].r_tid_ = 0;
        Table[i].w_tid_ = 0;
        Table[i].batch_id_w_ = 0;
        Table[i].batch_id_r_ = 0;
    }
}

void worker(int thread_id, int &ready, const bool &start, const bool &quit, std::barrier<> &sync_point)
{
    Result &myres = std::ref(AllResult[thread_id]);
    Transaction trans;
    uint32_t batch_id = 0;
    uint64_t tx_pos;
    uint64_t sleep_flg = 0;
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);


//Thread starts
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }

POINT:

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))
    {
//sequencing layer starts

        if(trans.status_ != Status::ABORTED){
            // aquire giant lock
            if (!lock_for_locks.w_try_lock())
            {
                std::this_thread::sleep_for(std::chrono::microseconds(BACKOFF_TIME));
                goto POINT;
            }

            // 取得すべきtxの現在地　ロック必要か
            tx_pos = __atomic_load_n(&tx_counter, __ATOMIC_SEQ_CST);
            if (tx_pos >= PRE_NUM)
            {
                return;
            }
            __atomic_store_n(&tx_counter, tx_pos + 1, __ATOMIC_SEQ_CST);
            lock_for_locks.w_unlock();
        }
    
        Pre &work_tx = Pre_tx_set[tx_pos].first;
        trans.task_set_ = work_tx.task_set_;

        uint32_t tid = Pre_tx_set[tx_pos].second;
        batch_id++;
        sleep_flg = 0;
        trans.begin(); 
//sequencing layer ends

//execution phase starts
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
    //do W & R reservation
        trans.ReserveWrite(tid, batch_id);
        trans.ReserveRead(tid, batch_id){
        if(sleep_flg == 1){
            std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_TIME));
        }
//execution phase ends
        sync_point.arrive_and_wait();

//commit phase starts
    //check waw conflict
        if(trans.WAW(tid, batch_id))
        {
            trans.status_ = Status::ABORTED;
        }
    //check war & raw conflict (reordering)
        if(trans.status_ != Status::ABORTED)
        {
            if(trans.RAW(tid, batch_id))
            {
                if(trans.WAR(tid, batch_id))
                {
                    trans.status_ = Status::ABORTED;
                }
            }
        }
//commit phase ends

        if(trans.status_ == Status::ABORTED)
        {
            trans.abort();
        }else{
            trans.update();
            trans.commit();
        }
        sync_point.arrive_and_wait();

        if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && trans.status_ != Status::ABORTED)
        {
            myres.commit_cnt_++;
        }

    }
    }

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

        if(rnd.next() % 100 < SLEEP_RATE)
        {
            makeSleep(pre.first.task_set_, rnd, zipf);
        }
        else{
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
    std::cout << "throughput exi:" << SLEEP_TIME << " " << total_count / EX_TIME << std::endl;

    return 0;
}

