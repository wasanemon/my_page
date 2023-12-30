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

#define PAGE_SIZE 4096
#define THREAD_NUM 12
#define TUPLE_NUM 1000000
#define MAX_OPE 10
#define RW_RATE 50
#define EX_TIME 3
#define PRE_NUM 10000000
#define SLEEP_TIME 0
#define SLEEP_TIME_INIT 2900 * 1000
//#define SKEW_PAR 0.80
#define BACKOFF_TIME 0
#define SLEEP_RATE 0
#define BATCH_SIZE 120
#define TX_PAR_THREAD BATCH_SIZE/THREAD_NUM

double SKEW_PAR = 0;
uint64_t tx_counter;
std::vector<uint32_t> aborted_list(BATCH_SIZE);
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



//transaction取得においてのみ用いる
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


RWLock lock_for_locks;

class Tuple
{
public:
    std::atomic<uint64_t> lock_;
    uint64_t value_;
    std::atomic<uint32_t> r_tid_;
    std::atomic<uint32_t> w_tid_;
    std::atomic<uint32_t> w_batch_id_;
    std::atomic<uint32_t> r_batch_id_;
    size_t tx_id_;
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
    uint64_t tx_id_;
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

    void ReserveWrite(uint32_t my_tid, uint32_t my_batch_id, size_t my_tx_id) 
    {
        uint64_t expected_lock = 0;
        for(auto &wset : write_set_){
            expected_lock = 0;
            while (!wset.tuple_->lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) //lock取得
            {
                expected_lock = 0;
            }
            // 条件チェック
            if (wset.tuple_->w_tid_ < my_tid && wset.tuple_->w_batch_id_ == my_batch_id) //すでに他のsmall tidを持つTxによってreservationされていて、reservationが失敗する場合
            {
                wset.tuple_->lock_.store(0, std::memory_order_release); 
                continue;
            }
            if(wset.tuple_->w_tid_ == my_tid && wset.tuple_->w_batch_id_ == my_batch_id) //自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
            {
                wset.tuple_->lock_.store(0, std::memory_order_release);
                continue;
            }
            if (wset.tuple_->w_tid_ > my_tid || wset.tuple_->w_batch_id_ < my_batch_id ||wset.tuple_->w_tid_ == 0) //reservationが成功する場合
            {
                wset.tuple_->w_tid_ = my_tid; //w_tid_の更新
                wset.tuple_->tx_id_ = my_tx_id; //thread_id_の更新
                if(wset.tuple_->w_batch_id_ < my_batch_id) //そのbatch内において初めてw_tid_の更新が行われる場合にw_batch_id_も更新する
                {
                    wset.tuple_->w_batch_id_ = my_batch_id;
                }
                
            }
            wset.tuple_->lock_.store(0, std::memory_order_release); //unlock
        }
        return ;
    }

    void ReserveRead(uint32_t my_tid,  uint32_t my_batch_id) 
    {
        uint64_t expected_lock = 0;
        for(auto &rset : read_set_){
            expected_lock = 0;
            while (!rset.tuple_->lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) //lock取得
            {
                expected_lock = 0; 
            }
            // 条件チェック
            if (rset.tuple_->r_tid_ < my_tid && rset.tuple_->r_batch_id_ == my_batch_id) //すでに他のsmall tidを持つTxによってreservationされていて、reservationが失敗する場合
            {
                rset.tuple_->lock_.store(0, std::memory_order_release); 
                continue;
            }
            if(rset.tuple_->r_tid_ == my_tid && rset.tuple_->r_batch_id_ == my_batch_id) //自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
            {
                rset.tuple_->lock_.store(0, std::memory_order_release);
                continue;
            }
            if (rset.tuple_->r_tid_ > my_tid || rset.tuple_->r_batch_id_ < my_batch_id || rset.tuple_->r_tid_ == 0) //reservationが成功する場合
            {
                rset.tuple_->r_tid_ = my_tid; //r_tid_の更新
                if(rset.tuple_->r_batch_id_ < my_batch_id) //そのbatch内において初めてr_tid_の更新が行われる場合にr_batch_id_も更新する
                {
                    rset.tuple_->r_batch_id_ = my_batch_id;
                }
            }
            rset.tuple_->lock_.store(0, std::memory_order_release); //unlock
        }
        return ;
    }

    bool WAW(uint32_t my_tid, uint32_t my_batch_id)
    {
        for(auto &wset : write_set_){
            if(wset.tuple_->w_tid_ != my_tid || wset.tuple_->w_batch_id_ != my_batch_id) //wawが存在する場合
            {
                return true;
            }
        }
        return false;
    }

    bool RAW(uint32_t my_tid, uint32_t my_batch_id, std::vector<uint32_t>& aborted_list)
    {
        if(my_tid == 3){
            //cout << "tid W3" << std::endl;
        }
        for(auto &rset : read_set_){
            if(my_tid == 3){
                //cout << rset.tuple_->w_tid_ << std::endl;
            }
            if(my_tid > rset.tuple_->w_tid_ && rset.tuple_->w_batch_id_ == my_batch_id && rset.tuple_->w_tid_ != 0) //rawが存在する場合
            {
                if(aborted_list[rset.tuple_->tx_id_] == 0) //対象のdata項目をreservationしていたTxがabortしていた場合、rawにはならない
                {
                    return true;
                }
            }
        }
        
        return false;
    }

    bool WAR(uint32_t my_tid, uint32_t my_batch_id,std::vector<uint32_t>& aborted_list)
    {
        if(my_tid == 3){
            //cout << "tid R3" << std::endl;
        }
        for(auto &wset : write_set_){
            if(my_tid == 3){
                //cout << wset.tuple_->r_tid_ << std::endl;
            }
            if(my_tid > wset.tuple_->r_tid_ && wset.tuple_->r_batch_id_ == my_batch_id && wset.tuple_->r_tid_ != 0) //warが存在する場合
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
    
    // posix_memalignの呼び出しと戻り値のチェック
    ret = posix_memalign((void **)&Table, PAGE_SIZE, TUPLE_NUM * sizeof(Tuple));
    if (ret != 0) {
        // エラーハンドリング: 例えば、エラーメッセージを出力し、関数から抜ける
        cout << "error" << std::endl;
        return;
    }
    for (int i = 0; i < TUPLE_NUM; i++)
    {
        Table[i].lock_ = 0;
        Table[i].value_ = 0;
        Table[i].r_tid_ = 0;
        Table[i].w_tid_ = 0;
        Table[i].w_batch_id_ = 0;
        Table[i].r_batch_id_ = 0;
        Table[i].tx_id_ = 0;
    }
}


uint32_t batch;
std::atomic<uint64_t> tx_lock = 0;
std::vector<uint64_t> abort_tid_list(BATCH_SIZE);
std::atomic<int> abort_counter = 0;
uint64_t new_tx_num = 0;
std::atomic<uint64_t> round_robin = 0;
void worker(int thread_id, int &ready, const bool &start, const bool &quit, spin_barrier& barrier)
{
    Result &myres = std::ref(AllResult[thread_id]);
    std::vector<Transaction> transactions;
    
    for (int i = 0; i < TX_PAR_THREAD; ++i) {
        transactions.push_back(Transaction());
    }
    for (int i = 0; i < TX_PAR_THREAD; ++i) {
        transactions[i].tx_id_ = thread_id*TX_PAR_THREAD + i;
    }
    uint64_t tx_pos;
    uint32_t batch_id = 0;
    uint64_t sleep_flg = 0;
    uint64_t expected_lock = 0;
    uint64_t abort_index = 0;
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))//0, 64,128
    {
        
        //前のbatchにおいてabortしていた場合は、同一のTxを実行
        new_tx_num = BATCH_SIZE - abort_counter;

        for(int i = 0;i < TX_PAR_THREAD;i++){
                if(THREAD_NUM*i + thread_id < abort_counter){
                    transactions[i].tx_pos_ = abort_tid_list[THREAD_NUM*i + thread_id] - 1;
                    if(abort_tid_list[THREAD_NUM*i + thread_id] == 0){
                        //cout << "aborror" << std::endl;
                    }
                    abort_tid_list[THREAD_NUM*i + thread_id] = 0;
                }else{
                    transactions[i].tx_pos_ = THREAD_NUM*i + thread_id + tx_counter - abort_counter;
                }
            // Txの実行内容(task_set)の取得、tidの取得、batch_idの更新
                transactions[i].task_set_ = Pre_tx_set[transactions[i].tx_pos_].first.task_set_;
                transactions[i].tid_ = Pre_tx_set[transactions[i].tx_pos_].second;
                
                //sleep_flg = 0;
                transactions[i].begin();
                aborted_list[transactions[i].tx_id_] = 0;
        }
        batch_id++;
        //if(thread_id == 0 && batch_id < 20){
            //cout << "batch" << batch_id << std::endl;
        //}

        
        
        //sleep_flg = 0;
        
    

    //execution phase
        //make R&W-set
        for(int i = 0;i < TX_PAR_THREAD;i++){
            for (auto &task : transactions[i].task_set_)
            {
                switch (task.ope_)
                {
                case Ope::READ:
                    transactions[i].read(task.key_);
                    // std::cout << "read" << std::endl;
                    break;
                case Ope::WRITE:
                    transactions[i].write(task.key_);
                    // std::cout << "write" << std::endl;
                    break;
                case Ope::SLEEP:
                    sleep_flg = 1;
                    break;
                default:
                    std::cout << "fail" << std::endl;
                    break;
                }
            }

            //実行しているTxのtid,Txが実行されているbatch_id、Txを実行しているthread_idを引数にとる

            transactions[i].ReserveWrite(transactions[i].tid_, batch_id, transactions[i].tx_id_);
        }
        //同期ポイント① batch内の全てTxのWriteReservationが終了するのを待つ
        barrier.wait(thread_id);
        if(thread_id == THREAD_NUM - 1){
            tx_counter += new_tx_num;
            abort_counter = 0;
        }
        //実行しているTxのtid,Txが実行されているbatch_idを引数にとる
        for(int i = 0;i < TX_PAR_THREAD;i++){
            if(transactions[i].WAW(transactions[i].tid_, batch_id))
            {   
                //wawがあった場合はabort
                transactions[i].status_ = Status::ABORTED;
                //wawによりabortした場合は、対応するabort_listの要素を1にして、他のthreadに自身がabortしたことを伝える
                aborted_list[transactions[i].tx_id_] = 1;
            }else{
                //abortしなかった場合はReadReservationを実行する
                transactions[i].ReserveRead(transactions[i].tid_, batch_id);
            }
        }
        //同期ポイント② batch内の全てのTxの、wawによるabort、もしくはReadReservationが終わるのを待つ
        barrier.wait(thread_id);
        //Reorderingを行う
        for(int i = 0;i < TX_PAR_THREAD;i++){
            if(transactions[i].status_ != Status::ABORTED)
            {
                //最初にRAWを持つか確認
                if(transactions[i].RAW(transactions[i].tid_, batch_id, aborted_list))
                {
                    //RAWを持つ場合、WARを確認
                    if(transactions[i].WAR(transactions[i].tid_, batch_id, aborted_list))
                    {
                        //両方持っていた場合、abort、片方しか持たない場合は、reorderingによりcommit
                        transactions[i].status_ = Status::ABORTED;
                    }
                }
            }

            if(transactions[i].status_ == Status::ABORTED)
            {
                abort_index = abort_counter.fetch_add(1);
                abort_tid_list[abort_index] = transactions[i].tid_;
                
                transactions[i].abort();
            }
            else
            {
                //if(transactions[i].tid_ < 39){
                   //cout << transactions[i].tid_ << std::endl;
                //}
                transactions[i].update();
                transactions[i].commit();
            }
        }
        //同期ポイント③ commitしたtransactionによるupdateを待つ
        barrier.wait(thread_id);
        
        for(int i = 0;i < TX_PAR_THREAD;i++){
            if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && transactions[i].status_ != Status::ABORTED)
            {
                myres.commit_cnt_++;
            }
        }
        
    }
    //quit == trueがmain関数内でなされた時に、全てのthreadがworker関数を適切に修了するためのもの。他のthreadが、同期ポイントで永遠に待つことがないようにする。
    barrier.arrive_and_drop(thread_id);
    batch = batch_id;
}

int main(int argc, char *argv[]) 
{

    // gflags::ParseCommandLineFlags(&argc, &argv, true);
    // std::cout << "#FLAGS_tuple_num" << FLAGS_tuple_num << "\n";

    // initilize rnd and zipf
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
       if(tx_make_count < 0){
            if(tx_make_count == 0){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1090);
                pre.first.task_set_.emplace_back(Ope::WRITE, 100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 101);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 10000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1010);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102023);
                pre.first.task_set_.emplace_back(Ope::WRITE, 0);
                pre.first.task_set_.emplace_back(Ope::READ, 0);
                pre.first.task_set_.emplace_back(Ope::READ, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 2);
                pre.first.task_set_.emplace_back(Ope::READ, 3);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
            }
            if(tx_make_count == 1){
                pre.first.task_set_.emplace_back(Ope::WRITE, 100000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 10100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 100100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1020);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 7);
                pre.first.task_set_.emplace_back(Ope::READ, 8);
                pre.first.task_set_.emplace_back(Ope::READ, 9);
            }
            if(tx_make_count == 2){
                pre.first.task_set_.emplace_back(Ope::WRITE, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
            }
            if(tx_make_count == 3){
                pre.first.task_set_.emplace_back(Ope::WRITE, 5);
                pre.first.task_set_.emplace_back(Ope::WRITE, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 4){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::WRITE, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 5){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 20);
                pre.first.task_set_.emplace_back(Ope::READ, 21);
                pre.first.task_set_.emplace_back(Ope::READ, 22);
                pre.first.task_set_.emplace_back(Ope::READ, 23);
                pre.first.task_set_.emplace_back(Ope::READ, 24);
                pre.first.task_set_.emplace_back(Ope::READ, 25);
                pre.first.task_set_.emplace_back(Ope::READ, 26);
                pre.first.task_set_.emplace_back(Ope::READ, 27);
                
            }
            if(tx_make_count == 6){
                pre.first.task_set_.emplace_back(Ope::WRITE, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 7){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::WRITE, 198);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1988);
                pre.first.task_set_.emplace_back(Ope::WRITE, 156);
                pre.first.task_set_.emplace_back(Ope::WRITE, 172);
                pre.first.task_set_.emplace_back(Ope::WRITE, 164);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 8){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 9){
                pre.first.task_set_.emplace_back(Ope::WRITE, 98);
                pre.first.task_set_.emplace_back(Ope::READ, 134);
                pre.first.task_set_.emplace_back(Ope::READ, 1877);
            }
            
            if(tx_make_count == 10){
                pre.first.task_set_.emplace_back(Ope::WRITE, 100000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 10100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 100100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1020);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 7);
                pre.first.task_set_.emplace_back(Ope::READ, 8);
                pre.first.task_set_.emplace_back(Ope::READ, 9);
            }
            if(tx_make_count == 11){
                pre.first.task_set_.emplace_back(Ope::WRITE, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
            }
            if(tx_make_count == 12){
                pre.first.task_set_.emplace_back(Ope::WRITE, 5);
                pre.first.task_set_.emplace_back(Ope::WRITE, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 13){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::WRITE, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 14){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 20);
                pre.first.task_set_.emplace_back(Ope::READ, 21);
                pre.first.task_set_.emplace_back(Ope::READ, 22);
                pre.first.task_set_.emplace_back(Ope::READ, 23);
                pre.first.task_set_.emplace_back(Ope::READ, 24);
                pre.first.task_set_.emplace_back(Ope::READ, 25);
                pre.first.task_set_.emplace_back(Ope::READ, 26);
                pre.first.task_set_.emplace_back(Ope::READ, 27);
                
            }
            if(tx_make_count == 15){
                pre.first.task_set_.emplace_back(Ope::WRITE, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count ==16){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::WRITE, 198);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1988);
                pre.first.task_set_.emplace_back(Ope::WRITE, 156);
                pre.first.task_set_.emplace_back(Ope::WRITE, 172);
                pre.first.task_set_.emplace_back(Ope::WRITE, 164);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 17){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 18){
                pre.first.task_set_.emplace_back(Ope::WRITE, 98);
                pre.first.task_set_.emplace_back(Ope::READ, 134);
                pre.first.task_set_.emplace_back(Ope::READ, 1877);
            }
            if(tx_make_count == 19){
                pre.first.task_set_.emplace_back(Ope::WRITE, 10);
                pre.first.task_set_.emplace_back(Ope::WRITE, 0);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1010);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102023);
                pre.first.task_set_.emplace_back(Ope::WRITE, 0);
                pre.first.task_set_.emplace_back(Ope::READ, 0);
                pre.first.task_set_.emplace_back(Ope::READ, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
            }
            if(tx_make_count == 20){
                pre.first.task_set_.emplace_back(Ope::WRITE, 5);
                pre.first.task_set_.emplace_back(Ope::WRITE, 4);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::WRITE, 2);
                pre.first.task_set_.emplace_back(Ope::WRITE, 12);
                pre.first.task_set_.emplace_back(Ope::WRITE, 9);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 7);
                pre.first.task_set_.emplace_back(Ope::READ, 8);
                pre.first.task_set_.emplace_back(Ope::READ, 9);
            }
            if(tx_make_count == 21){
                pre.first.task_set_.emplace_back(Ope::WRITE, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 10);
            }
            if(tx_make_count == 22){
                pre.first.task_set_.emplace_back(Ope::WRITE, 10);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
            }
            if(tx_make_count == 23){
                pre.first.task_set_.emplace_back(Ope::WRITE, 5);
                pre.first.task_set_.emplace_back(Ope::WRITE, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
                pre.first.task_set_.emplace_back(Ope::READ, 3);
            }
            if(tx_make_count == 24){
                pre.first.task_set_.emplace_back(Ope::WRITE, 6);
                pre.first.task_set_.emplace_back(Ope::WRITE, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 15);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 25){
                pre.first.task_set_.emplace_back(Ope::WRITE, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 0);
                pre.first.task_set_.emplace_back(Ope::READ, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 20);
                pre.first.task_set_.emplace_back(Ope::READ, 21);
                pre.first.task_set_.emplace_back(Ope::READ, 2);
                pre.first.task_set_.emplace_back(Ope::READ, 3);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::WRITE, 6);
                pre.first.task_set_.emplace_back(Ope::WRITE, 7);
                
            }
            if(tx_make_count == 26){
                pre.first.task_set_.emplace_back(Ope::WRITE, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 27){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::WRITE, 198);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1988);
                pre.first.task_set_.emplace_back(Ope::WRITE, 156);
                pre.first.task_set_.emplace_back(Ope::WRITE, 172);
                pre.first.task_set_.emplace_back(Ope::WRITE, 164);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 28){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 29){
                pre.first.task_set_.emplace_back(Ope::WRITE, 98);
                pre.first.task_set_.emplace_back(Ope::READ, 134);
                pre.first.task_set_.emplace_back(Ope::READ, 1877);
            }
            
            if(tx_make_count == 30){
                pre.first.task_set_.emplace_back(Ope::WRITE, 100000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 10100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 102000);
                pre.first.task_set_.emplace_back(Ope::WRITE, 100100);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1020);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 7);
                pre.first.task_set_.emplace_back(Ope::READ, 8);
                pre.first.task_set_.emplace_back(Ope::READ, 9);
            }
            if(tx_make_count == 31){
                pre.first.task_set_.emplace_back(Ope::WRITE, 6);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
            }
            if(tx_make_count == 32){
                pre.first.task_set_.emplace_back(Ope::WRITE, 5);
                pre.first.task_set_.emplace_back(Ope::WRITE, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1020);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count == 33){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::WRITE, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 5);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 34){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 18);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 20);
                pre.first.task_set_.emplace_back(Ope::READ, 21);
                pre.first.task_set_.emplace_back(Ope::READ, 22);
                pre.first.task_set_.emplace_back(Ope::READ, 23);
                pre.first.task_set_.emplace_back(Ope::READ, 24);
                pre.first.task_set_.emplace_back(Ope::READ, 25);
                pre.first.task_set_.emplace_back(Ope::READ, 26);
                pre.first.task_set_.emplace_back(Ope::READ, 27);
                
            }
            if(tx_make_count == 35){
                pre.first.task_set_.emplace_back(Ope::WRITE, 19);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
            }
            if(tx_make_count ==36){
                pre.first.task_set_.emplace_back(Ope::WRITE, 9);
                pre.first.task_set_.emplace_back(Ope::WRITE, 8);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::WRITE, 156);
                pre.first.task_set_.emplace_back(Ope::WRITE, 172);
                pre.first.task_set_.emplace_back(Ope::WRITE, 164);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::WRITE, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 17);
                pre.first.task_set_.emplace_back(Ope::READ, 1980);
                pre.first.task_set_.emplace_back(Ope::READ, 9);
                pre.first.task_set_.emplace_back(Ope::READ, 8);
                pre.first.task_set_.emplace_back(Ope::READ, 1);
                pre.first.task_set_.emplace_back(Ope::READ, 156);
                pre.first.task_set_.emplace_back(Ope::WRITE, 172);
                pre.first.task_set_.emplace_back(Ope::READ, 164);
                pre.first.task_set_.emplace_back(Ope::WRITE, 11);
                pre.first.task_set_.emplace_back(Ope::READ, 1);
            }
            if(tx_make_count == 37){
                pre.first.task_set_.emplace_back(Ope::WRITE, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 1342);
                pre.first.task_set_.emplace_back(Ope::READ, 19);
            }
            if(tx_make_count == 38){
                pre.first.task_set_.emplace_back(Ope::WRITE, 98);
                pre.first.task_set_.emplace_back(Ope::READ, 4);
                pre.first.task_set_.emplace_back(Ope::WRITE, 7);
            
            }
        }
        
        //if(tx_make_count > 39){
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
    std::cout << batch << " " << total_count / EX_TIME << std::endl;
    return 0;
}
