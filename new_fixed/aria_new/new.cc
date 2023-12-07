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

uint64_t tx_counter = 0;
std::array<uint32_t, THREAD_NUM> aborted_list = {};
std::vector<std::pair<Pre, uint32_t>> Pre_tx_set(PRE_NUM);
std::vector<Result> AllResult(THREAD_NUM);
RWLock lock_for_assign_tx;

void worker(int thread_id, int &ready, const bool &start, const bool &quit, std::barrier<> &sync_point)
{
    Result &myresult = std::ref(AllResult[thread_id]);
    Transaction trans;
    uint64_t tx_pos;
    uint32_t tid;
    uint32_t batch_id = 0;
    uint64_t sleep_flg = 0;
    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }
POINT:

    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))
    {
        // 前のbatchにおいてabortしていた場合は、同一のTxを実行
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
        // pre_tx_setからコピー
        Pre &work_tx = Pre_tx_set[tx_pos].first;
        trans.task_set_ = work_tx.task_set_;
        tid = Pre_tx_set[tx_pos].second;
        batch_id++;
        aborted_list[thread_id] = 0;
        sleep_flg = 0;
        trans.begin();

        // execution phase
        // make R&W-set
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
                sleep_flg = 1;
                break;
            default:
                std::cout << "fail" << std::endl;
                break;
            }
        }

        // 実行しているTxのtid,Txが実行されているbatch_id、Txを実行しているthread_idを引数にとる
        trans.ReserveWrite(tid, batch_id, thread_id);

        // 同期ポイント① batch内の全てTxのWriteReservationが終了するのを待つ
        sync_point.arrive_and_wait();

        // 実行しているTxのtid,Txが実行されているbatch_idを引数にとる
        if (trans.WAW(tid, batch_id))
        {
            // wawがあった場合はabort
            trans.status_ = Status::ABORTED;
            // wawによりabortした場合は、対応するabort_listの要素を1にして、他のthreadに自身がabortしたことを伝える
            aborted_list[thread_id] = 1;
        }
        else
        {
            // abortしなかった場合はReadReservationを実行する
            trans.ReserveRead(tid, batch_id);
        }

        // 同期ポイント② batch内の全てのTxの、wawによるabort、もしくはReadReservationが終わるのを待つ
        sync_point.arrive_and_wait();

        // Reorderingを行う
        if (trans.status_ != Status::ABORTED)
        {
            // 最初にRAWを持つか確認
            if (trans.RAW(tid, batch_id, aborted_list))
            {
                // RAWを持つ場合、WARを確認
                if (trans.WAR(tid, batch_id))
                {
                    // 両方持っていた場合、abort、片方しか持たない場合は、reorderingによりcommit
                    trans.status_ = Status::ABORTED;
                }
            }
        }

        if (trans.status_ == Status::ABORTED)
        {
            trans.abort();
        }
        else
        {
            trans.update();
            trans.commit();
        }
        // 同期ポイント③ commitしたtransactionによるupdateを待つ
        sync_point.arrive_and_wait();

        if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && trans.status_ != Status::ABORTED)
        {
            myresult.commit_cnt_++;
        }
    }
    // quit == trueがmain関数内でなされた時に、全てのthreadがworker関数を適切に修了するためのもの。他のthreadが、同期ポイントで永遠に待つことがないようにする。
    sync_point.arrive_and_drop();
}

int main(int argc, char *argv[])
{
    Xoroshiro128Plus rnd;
    FastZipf zipf(&rnd, SKEW_PAR, TUPLE_NUM);
    makeDB();
    bool start = false;
    bool quit = false;
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
    std::barrier sync_point(THREAD_NUM);
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
    std::cout << "throughput new:" << SLEEP_TIME << " " << total_count / EX_TIME << std::endl;

    return 0;
}
