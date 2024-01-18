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
#define THREAD_NUM 16
#define TUPLE_NUM 1000000
#define MAX_OPE 100
//#define RW_RATE 50
#define EX_TIME 3
#define PRE_NUM 3000000
#define SLEEP_TIME 0
#define SLEEP_TIME_INIT 2900 * 1000
//#define SKEW_PAR 0.80
#define BACKOFF_TIME 0
#define SLEEP_RATE 0
//#define BATCH_SIZE 16000
//#define TX_PAR_THREAD BATCH_SIZE/THREAD_NUM

int RW_RATE = 80;
int BATCH_SIZE = 1024;
double SKEW_PAR = 0.6;
int TX_PER_THREAD = BATCH_SIZE/THREAD_NUM;

uint64_t tx_counter;

// DEFINE_uint64(tuple_num, 1000000, "Total number of records");
class Result
{
public:
    uint64_t commit_cnt_;
    Result() : commit_cnt_(0) {}
};

std::vector<Result> AllResult(THREAD_NUM);

enum class Ope
{
    READ,
    WRITE,
    SLEEP,
};

class spin_barrier {
public:
  spin_barrier(size_t threads) :
      threads_(static_cast<int>(threads)),
      waits_(static_cast<int>(threads)),
      sense_(false),
      my_sense_(threads << 6) {
    for (size_t i = 0; i < threads; ++i) {
      my_sense_[i << 6] = true;
    }
  }

  void wait(int i) {
    int sense = my_sense_[i << 6];
    if (waits_.fetch_sub(1) == 1) {
      waits_.store(threads_.load());
      sense_.store(sense != 0, std::memory_order_release);
    } else {
      while (sense != sense_.load(std::memory_order_acquire));
    }
    my_sense_[i << 6] = !sense;
  }

  void arrive_and_drop(int i) {
    int sense = my_sense_[i << 6];
    if (waits_.fetch_sub(1) == 1) {
      int new_thread_count = threads_.fetch_sub(1) - 1;
      waits_.store(new_thread_count);
      sense_.store(sense != 0, std::memory_order_release);
    } else {
      while (sense != sense_.load(std::memory_order_acquire));
    }
    // このスレッドはもうバリアで待機しない
  }
  
private:
  std::atomic<int> threads_;  // const を取り除き、std::atomic<int> に変更
  std::atomic<int> waits_;
  std::atomic<bool> sense_;
  std::vector<int> my_sense_;
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
    uint64_t tid_;
    uint64_t tx_pos_;
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
    int ret;
    ret = posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    if (ret != 0) {
        // エラーハンドリング: 例えば、エラーメッセージを出力し、関数から抜ける
        cout << "error" << std::endl;
        return;
    }
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


std::vector<uint64_t> Next_Batch(BATCH_SIZE);
std::atomic<int> abort_counter = 0;
uint64_t new_tx_num = 0;
void worker(int thread_id, int &ready, const bool &start, const bool &quit, spin_barrier& barrier)
{
    Result &myres = std::ref(AllResult[thread_id]);
    std::vector<Transaction> transactions;
    
    for (int i = 0; i < TX_PER_THREAD; ++i) {
        transactions.push_back(Transaction());
    }
    uint32_t batch_id = 0;
    uint64_t sleep_flg = 0;
    uint64_t abort_index = 0;
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
        new_tx_num = BATCH_SIZE - abort_counter;

        for(int i = 0;i < TX_PER_THREAD;i++){
                if(THREAD_NUM*i + thread_id < abort_counter){
                    transactions[i].tx_pos_ = Next_Batch[THREAD_NUM*i + thread_id] - 1;
                    Next_Batch[THREAD_NUM*i + thread_id] = 0;
                }else{
                    transactions[i].tx_pos_ = THREAD_NUM*i + thread_id + tx_counter - abort_counter;
                }
            // Txの実行内容(task_set)の取得、tidの取得、batch_idの更新
        }
        barrier.wait(thread_id);//シーケンシング
        if(thread_id == 0){
            tx_counter += new_tx_num;
            abort_counter = 0;
        }
        for(int i = 0;i < TX_PER_THREAD;i++){
            transactions[i].task_set_ = Pre_tx_set[transactions[i].tx_pos_].first.task_set_;
            transactions[i].tid_ = Pre_tx_set[transactions[i].tx_pos_].second;
            transactions[i].begin();
        }
        batch_id++;
        //if(thread_id == 1 && batch_id < 70){
            //cout << "batch" << batch_id << std::endl;
        //}
        // sequencing layer ends

        // execution phase starts
        // make read & write set
        for(int i = 0;i < TX_PER_THREAD;i++){
            for (auto &task : transactions[i].task_set_)
            {
                switch (task.ope_)
                {
                case Ope::READ:
                    transactions[i].read(task.key_);
                    break;
                case Ope::WRITE:
                    transactions[i].write(task.key_);
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
            transactions[i].ReserveWrite(transactions[i].tid_, batch_id);
            transactions[i].ReserveRead(transactions[i].tid_, batch_id);
        }
        
        
        // execution phase ends
        barrier.wait(thread_id);
        // commit phase starts
        // check waw conflict
        for(int i = 0;i < TX_PER_THREAD;i++){
            if (transactions[i].WAW(transactions[i].tid_, batch_id))
            {
                transactions[i].status_ = Status::ABORTED;
            }
            // WAW関数においてabortしなかったTxのみ、WAR & RAWを実行する。Reordering
            if (transactions[i].status_ != Status::ABORTED)
            {
                if (transactions[i].RAW(transactions[i].tid_, batch_id))
                {
                    if (transactions[i].WAR(transactions[i].tid_, batch_id))
                    {
                        // RAW() と WAR() が両方Trueの場合に、Reordering失敗,abortとなる
                        transactions[i].status_ = Status::ABORTED;
                    }
                }
            }

            // commitしたTxはupdateを実行
            if (transactions[i].status_ == Status::ABORTED)
            {
                abort_index = abort_counter.fetch_add(1);
                Next_Batch[abort_index] = transactions[i].tid_;
                transactions[i].abort();
            }
            else
            {
                transactions[i].commit();
                transactions[i].update();
            }
        }
        // commit phase ends
        barrier.wait(thread_id);

        for(int i = 0;i < TX_PER_THREAD;i++){
            if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && transactions[i].status_ != Status::ABORTED)
            {
                myres.commit_cnt_++;
            }
        }
    }
    // 各threadがworker関数を抜ける際に、他の同期ポイントで永遠に待つことがないようにするためのもの
    barrier.arrive_and_drop(thread_id);


}

int main(int argc, char *argv[])
{
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, SKEW_PAR, TUPLE_NUM);
    spin_barrier barrier(THREAD_NUM);

    makeDB();


    tx_counter = 0;

    bool start = false;
    bool quit = false;

    // transaction generate

    uint32_t tid = 1;
    int tx_make_count = 0;
    for (auto &pre : Pre_tx_set)
    {
        //if(tx_make_count >= 60){
        makeTask(pre.first.task_set_, rnd, zipf);
        //}
        
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
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(barrier));
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
    std::cout << RW_RATE << " "<<  SKEW_PAR << " " << BATCH_SIZE << " " << total_count / EX_TIME << std::endl;
    return 0;
}
