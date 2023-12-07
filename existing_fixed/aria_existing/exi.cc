// #include "gflags/gflags.h"
#include "../include/zipf.hh"
#include "../include/random.hh"
#include "../include/util.hh"
#include "../include/rwlock.hh"
#include "../include/task.hh"
#include "../include/tuple.hh"
#include "../include/operation.hh"
#include "../include/status.hh"
#include "../include/transaction.hh"
#include "../include/make_db_task.hh"
#include "../include/result.hh"

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

extern Tuple *Table;
uint64_t tx_counter = 0;
std::vector<std::pair<Pre, uint32_t>> Pre_tx_set(PRE_NUM);
std::vector<Result> AllResult(THREAD_NUM);
RWLock lock_for_assign_tx;

void worker(int thread_id, int &ready, const bool &start, const bool &quit, std::barrier<> &sync_point)
{
    Result &myres = std::ref(AllResult[thread_id]);
    Transaction trans;
    uint32_t tid;
    uint32_t batch_id = 0;
    uint64_t tx_pos;
    uint64_t sleep_flg = 0;
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);

    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }
    // Thread starts
POINT:
    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))
    {
        // sequencing layer starts
        // １つ前のepochにおいて、abortしていなかった場合、新たなTxを取得、実行する
        if (trans.status_ != Status::ABORTED)
        {
            // aquire giant lock
            if (!lock_for_assign_tx.w_try_lock())
            {
                std::this_thread::sleep_for(std::chrono::microseconds(BACKOFF_TIME));
                goto POINT;
            }
            tx_pos = __atomic_load_n(&tx_counter, __ATOMIC_SEQ_CST);
            if (tx_pos >= PRE_NUM)
            {
                return;
            }
            __atomic_store_n(&tx_counter, tx_pos + 1, __ATOMIC_SEQ_CST);
            lock_for_assign_tx.w_unlock();
        }
        // Txの実行内容(task_set)の取得、tidの取得、batch_idの更新
        Pre &work_tx = Pre_tx_set[tx_pos].first;
        trans.task_set_ = work_tx.task_set_;
        tid = Pre_tx_set[tx_pos].second;
        batch_id++;
        sleep_flg = 0;
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
        if (sleep_flg == 1)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_TIME));
        }
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
            trans.update();
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
    std::barrier sync_point(THREAD_NUM);
    bool start = false;
    bool quit = false;
    makeDB();
    int tx_make_count = 0;
    uint32_t tid = 1;
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
        th.join();
    uint64_t total_count = 0;
    for (auto &re : AllResult)
        total_count += re.commit_cnt_;

    std::cout << "throughput exi:" << SLEEP_TIME << " " << total_count / EX_TIME << std::endl;

    return 0;
}
