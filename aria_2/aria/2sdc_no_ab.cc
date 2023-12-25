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

#define PAGE_SIZE 4096
#define THREAD_NUM 12
#define TUPLE_NUM 1000000
#define MAX_OPE 100
#define RW_RATE 95
#define EX_TIME 3
#define PRE_NUM 1000000
#define SLEEP_TIME 0
#define SLEEP_TIME_INIT 2900 * 1000
//#define SKEW_PAR 0.80
#define BACKOFF_TIME 0
#define SLEEP_RATE 0

double SKEW_PAR = 0.9;
uint64_t tx_counter;
std::array<std::atomic<uint32_t>, THREAD_NUM> aborted_list = {};
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

class ReservationNode
{
public:
    std::atomic<uint32_t> r_tid_;
    uint32_t thread_id;
    ReservationNode* ptrLargeNum = nullptr;
    //ReservationNode* ptrSmallNum = nullptr;
    ReservationNode() : ptrLargeNum(nullptr) {}
};


class Tuple
{
public:
    std::atomic<uint64_t> w_lock_;
    std::atomic<uint64_t> r_lock_;
    uint64_t value_;
    //std::atomic<uint32_t> r_tid_;
    std::atomic<uint32_t> w_tid_;
    std::atomic<uint32_t> w_batch_id_;
    std::atomic<uint32_t> r_batch_id_;
    size_t thread_id_;
    std::array<ReservationNode, THREAD_NUM> ReadReservation_List = {};
    ReservationNode* ptrFirstReservation;
    Tuple() {
        for (int i = 0; i < THREAD_NUM; ++i) {
            ReadReservation_List[i].thread_id = (size_t)i;
        }
    }
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

    void ReserveWrite(uint32_t my_tid, uint32_t my_batch_id, size_t my_thread_id) 
    {
        uint64_t expected_lock = 0;
        for(auto &wset : write_set_){
            expected_lock = 0;
            while (!wset.tuple_->w_lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) //lock取得
            {
                expected_lock = 0;
            }
            // 条件チェック
             //すでに他のsmall tidを持つTxによってreservationされていて、reservationが失敗する場合に、自身をabort
            if (wset.tuple_->w_tid_ < my_tid && wset.tuple_->w_batch_id_ == my_batch_id)
            {
                if(aborted_list[my_thread_id] == 0){
                    aborted_list[my_thread_id] = 1;
                }
                wset.tuple_->w_lock_.store(0, std::memory_order_release); 
                continue;
            }
            if(wset.tuple_->w_tid_ == my_tid && wset.tuple_->w_batch_id_ == my_batch_id) //自身によってすでにreservationされているdata項目に繰り返しreservationを試みた場合
            {
                wset.tuple_->w_lock_.store(0, std::memory_order_release);
                continue;
            }
            if (wset.tuple_->w_tid_ > my_tid || wset.tuple_->w_batch_id_ < my_batch_id || wset.tuple_->w_tid_ == 0) //reservationが成功する場合
            {
                //変更対象のtupleをこのbatchでreseravtionしているTxがabortしておらず、かつ初期値でない場合
                if(aborted_list[wset.tuple_->thread_id_] == 0 && wset.tuple_->w_batch_id_ == my_batch_id && wset.tuple_->w_tid_ != 0){
                    aborted_list[wset.tuple_->thread_id_] = 1;
                }
                wset.tuple_->w_tid_ = my_tid; //w_tid_の更新
                wset.tuple_->thread_id_ = my_thread_id; //thread_id_の更新
                if(wset.tuple_->w_batch_id_ < my_batch_id) //そのbatch内において初めてw_tid_の更新が行われる場合にw_batch_id_も更新する
                {
                    wset.tuple_->w_batch_id_ = my_batch_id;
                }
                
            }
            wset.tuple_->w_lock_.store(0, std::memory_order_release); //unlock
        }
        return ;
    }

    void ReserveRead(uint32_t my_tid,  uint32_t my_batch_id, size_t my_thread_id) 
    {
        uint64_t expected_lock = 0;
        
        for(auto &rset : read_set_){
            expected_lock = 0;
            if(aborted_list[my_thread_id] == 1){
                return;                                                                                                                                              
            }
            while (!rset.tuple_->r_lock_.compare_exchange_strong(expected_lock, 1, std::memory_order_acquire)) //lock取得
            {
                expected_lock = 0; 
            }

            if (rset.tuple_->r_batch_id_ < my_batch_id) //batch内で最初にreservationが成功する場合
            {
                //Tuple直下にTxのreservationをつなぐ
                rset.tuple_->ptrFirstReservation = &(rset.tuple_->ReadReservation_List[my_thread_id]);
                rset.tuple_->ReadReservation_List[my_thread_id].r_tid_ = my_tid;
                rset.tuple_->ReadReservation_List[my_thread_id].ptrLargeNum = nullptr;
                rset.tuple_->r_batch_id_ = my_batch_id;
                rset.tuple_->r_lock_.store(0, std::memory_order_release); 
                continue;
            }else{
                ReservationNode* previous = nullptr;
                ReservationNode* current = rset.tuple_->ptrFirstReservation;
                while(current->r_tid_ < my_tid){
                    if(current->ptrLargeNum != nullptr){
                        previous = current;
                        current = current->ptrLargeNum;
                        continue;
                    }
                    break;
                }

                if(current->r_tid_ > my_tid){//２つのreservationの中間に挿入する場合
                    if(previous == nullptr){
                        rset.tuple_->ptrFirstReservation = &(rset.tuple_->ReadReservation_List[my_thread_id]);
                        rset.tuple_->ReadReservation_List[my_thread_id].ptrLargeNum = current;
                        rset.tuple_->ReadReservation_List[my_thread_id].r_tid_ = my_tid;
                        rset.tuple_->r_lock_.store(0, std::memory_order_release);
                        continue;
                    }
                    previous->ptrLargeNum = &(rset.tuple_->ReadReservation_List[my_thread_id]);
                    rset.tuple_->ReadReservation_List[my_thread_id].ptrLargeNum = current;
                    rset.tuple_->ReadReservation_List[my_thread_id].r_tid_ = my_tid;
                    rset.tuple_->r_lock_.store(0, std::memory_order_release);
                    continue;
                }
    
                if(current->r_tid_ < my_tid){
                    current->ptrLargeNum = &(rset.tuple_->ReadReservation_List[my_thread_id]);
                    rset.tuple_->ReadReservation_List[my_thread_id].ptrLargeNum = nullptr;
                    rset.tuple_->ReadReservation_List[my_thread_id].r_tid_ = my_tid;
                    rset.tuple_->r_lock_.store(0, std::memory_order_release);
                    continue;
                }

                if(current->r_tid_ == my_tid){//同じ項目に2回目のreservation
                    rset.tuple_->r_lock_.store(0, std::memory_order_release);
                    continue;
                }
            }
        }
        return ;
    }

    bool RAW(uint32_t my_tid, uint32_t my_batch_id)
    {
        for(auto &rset : read_set_){
            if(my_tid > rset.tuple_->w_tid_ && rset.tuple_->w_batch_id_ == my_batch_id && rset.tuple_->w_tid_ != 0) //rawが存在する場合
            {
                if(aborted_list[rset.tuple_->thread_id_] == 0) //対象のdata項目をreservationしていたTxがabortしていた場合、rawにはならない
                {
                    return true;
                }
            }
        }
        return false;
    }

    bool WAR(uint32_t my_tid, uint32_t my_batch_id)
    {
        for(auto &wset : write_set_){
            if(wset.tuple_->r_batch_id_ < my_batch_id){
                continue;
            }
            ReservationNode* current = wset.tuple_->ptrFirstReservation;
            //nodeがnullではない時。nullの時は、自身より小さいreservationはない
            while(current != nullptr){
                if(current->r_tid_ < my_tid){//自身より小さいreservationに遭遇した場合、そのthreadがabortしていれば次に行ける。そうでなければWAR==trueで返す。
                    if(aborted_list[current->thread_id] == 1){
                        current = current->ptrLargeNum;
                        continue;
                    }else{
                        return true;
                    }
                }else if(current->r_tid_ > my_tid || current->r_tid_ == my_tid){//自身より大きいreservationもしくは自身のreservationまでたどり着いたらOk
                    break;
                }
                
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
        Table[i].r_lock_ = 0;
        Table[i].w_lock_ = 0;
        Table[i].value_ = 0;
        Table[i].w_tid_ = 0;
        Table[i].w_batch_id_ = 0;
        Table[i].r_batch_id_ = 0;
        Table[i].thread_id_ = 0;
        Table[i].ptrFirstReservation = nullptr;
        //Table[i].NodeTuple.ptrSmallNum = nullptr;
        //Table[i].NodeTuple.r_tid_ = 0;
        //Table[i].NodeTuple.thread_id = 0;
        size_t k = 0; 
        for(auto &list : Table[i].ReadReservation_List){
            list.ptrLargeNum = nullptr;
            //list.ptrSmallNum = nullptr;
            list.r_tid_ = 0;
            list.thread_id = k;
            k++;
        }
    }
}

uint32_t batch;
std::atomic<uint64_t> tx_lock = 0;
void worker(int thread_id, int &ready, const bool &start, const bool &quit, std::barrier<> &sync_point)
{
    Result &myres = std::ref(AllResult[thread_id]);
    Transaction trans;
    uint64_t tx_pos;
    uint32_t batch_id = 0;
    //uint64_t sleep_flg = 0;
    uint64_t expected_lock = 0;
    uint32_t tid = 0;
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))
    {

        //前のbatchにおいてabortしていた場合は、同一のTxを実行
        if(trans.status_ != Status::ABORTED){
            // aquire giant lock
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

        // pre_tx_setからコピー
        trans.task_set_ = Pre_tx_set[tx_pos].first.task_set_;
        // uint64_t operation_count = 0;
        tid = Pre_tx_set[tx_pos].second;
        batch_id++;
        aborted_list[thread_id] = 0;
        //sleep_flg = 0;
        trans.begin();
        //if(thread_id == 1 && batch_id < 20){
            //cout << "batch" << batch_id << std::endl;
        //}


    //execution phase
        //make R&W-set
        for (auto &task : trans.task_set_)
        {
            switch (task.ope_)
            {
            case Ope::READ:
                trans.read(task.key_);
                // std::cout << "read" << std::endl;
                break;
            case Ope::WRITE:
                trans.write(task.key_);
                // std::cout << "write" << std::endl;
                break;
            case Ope::SLEEP:
                //sleep_flg = 1;
                break;
            default:
                std::cout << "fail" << std::endl;
                break;
            }
        }

        //実行しているTxのtid,Txが実行されているbatch_id、Txを実行しているthread_idを引数にとる
        trans.ReserveWrite(tid, batch_id, thread_id);
        trans.ReserveRead(tid, batch_id,thread_id);
        //同期ポイント② batch内の全てのTxの、wawによるabort、もしくはReadReservationが終わるのを待つ
        sync_point.arrive_and_wait();

        //Reorderingを行う
        if(aborted_list[thread_id] == 0)
        {
            //最初にRAWを持つか確認
            if(trans.RAW(tid, batch_id))
            {
                //RAWを持つ場合、WARを確認
                if(trans.WAR(tid, batch_id))
                {
                    //両方持っていた場合、abort、片方しか持たない場合は、reorderingによりcommit
                    trans.status_ = Status::ABORTED;
                }
            }
        }
        //1.3
        if(aborted_list[thread_id] == 1 || trans.status_ == Status::ABORTED)
        {
            trans.status_ = Status::ABORTED;
            trans.abort();
        }else{
            //if(tid < 39){
                //cout << tid << std::endl;
            //}
            trans.update();
            trans.commit();
        }
        //同期ポイント③ commitしたtransactionによるupdateを待つ
        sync_point.arrive_and_wait();
        
        if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && trans.status_ != Status::ABORTED)
        {
            myres.commit_cnt_++;
        }
        
    }
    //quit == trueがmain関数内でなされた時に、全てのthreadがworker関数を適切に修了するためのもの。他のthreadが、同期ポイントで永遠に待つことがないようにする。
    sync_point.arrive_and_drop();
    batch = batch_id;
    
}

int main(int argc, char *argv[]) 
{

    // gflags::ParseCommandLineFlags(&argc, &argv, true);
    // std::cout << "#FLAGS_tuple_num" << FLAGS_tuple_num << "\n";

    // initilize rnd and zipf
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
        //if(tx_make_count < 0){
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
            
            //}
            }
        //if(tx_make_count > 38){
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