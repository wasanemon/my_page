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

uint64_t tx_counter = 0;                                   // txが何個実行されたか
std::vector<std::pair<Pre, uint32_t>> Pre_tx_set(PRE_NUM); // txのtask、tid格納用
std::vector<Result> AllResult(THREAD_NUM);                 // 結果格納用
RWLock lock_for_assign_tx;                                 // 安全にtxをthreadに割り当てるためのlock

// 各threadが実行する関数
void worker(int thread_id, int &ready, const bool &start, const bool &quit, std::barrier<> &sync_point)
{
    Result &myresult = std::ref(AllResult[thread_id]); // commit数をカウント
    Transaction trans;
    uint32_t tid;           // 自身のtid
    uint32_t batch_id = 0;  // Txが実行されているbatch
    uint64_t tx_pos;        // 準備セットのどこまでtxの取得が進んでいるか
    uint64_t sleep_flg = 0; // 性能計測用

    __atomic_store_n(&ready, 1, __ATOMIC_SEQ_CST);
    while (!__atomic_load_n(&start, __ATOMIC_SEQ_CST))
    {
    }
    // Thread starts
POINT:
    while (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST))
    {
        // sequencing layer starts
        // １つ前のbatchにおいてabortしていなかった場合、新たなTxを取得し、実行する。abortしていた場合はif文内を実行せずに同じTxを実行。
        if (trans.status_ != Status::ABORTED)
        {
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

        Pre &work_tx = Pre_tx_set[tx_pos].first; // Txの実行内容の取得
        trans.task_set_ = work_tx.task_set_;     // Txの実行内容の取得
        tid = Pre_tx_set[tx_pos].second;         // tidの取得
        batch_id++;                              // batchを進める
        sleep_flg = 0;                           // 性能計測用
        trans.begin();
        // sequencing layer ends (同期は必要ない)
        //----------------------------------------------------------------------------------------------------------------------------------------------
        // execution phase starts
        // task_setの内容に基づいて、read set、write setを作成 (thread local)
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
        sync_point.arrive_and_wait();
        // execution phase ends
        //--------------------------------------------------------------------------------------------------------------------------------------------------

        // commit phase starts
        // wawを持つか確認
        if (trans.WAW(tid, batch_id))
        {
            trans.status_ = Status::ABORTED;
        }
        // WAW関数においてabortしなかったTxのみ、WAR & RAWを実行する。(Reordering)
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
        sync_point.arrive_and_wait();
        // commit phase ends
        //----------------------------------------------------------------------------------------------------------------------------------------------

        if (!__atomic_load_n(&quit, __ATOMIC_SEQ_CST) && trans.status_ != Status::ABORTED)
        {
            myresult.commit_cnt_++;
        }
    }
    // 各threadがworker関数を抜ける際に、他の同期ポイントで永遠に待つことがないようにするためのもの
    sync_point.arrive_and_drop();
}

int main(int argc, char *argv[])
{
    Xoroshiro128Plus rnd;                     // 乱数生成用の変数
    FastZipf zipf(&rnd, SKEW_PAR, TUPLE_NUM); // 用いる分布
    std::barrier sync_point(THREAD_NUM);      // thread同期用の変数 (barrier)
    bool start = false;                       // worker thread開始用の変数
    bool quit = false;                        // worker thread終了用の変数
    makeDB();                                 // database作成
    uint32_t tid = 1;                         // txに付与するtidの初期化

    // txが実行するtaskの作成
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
        tid++;
    }

    // worker threadの準備状態を管理するためのvector
    std::vector<int> readys;
    for (size_t i = 0; i < THREAD_NUM; ++i)
    {
        readys.emplace_back(0);
    }

    // worker threadを生成、worker関数を実行させる
    std::vector<std::thread> thv;
    for (size_t i = 0; i < THREAD_NUM; ++i)
    {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(sync_point));
    }

    // 全てのworker threadの準備が終わるまで待つ
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

    __atomic_store_n(&start, true, __ATOMIC_SEQ_CST);                       // tx 開始の合図
    std::this_thread::sleep_for(std::chrono::milliseconds(1000 * EX_TIME)); // 実行時間の調整
    __atomic_store_n(&quit, true, __ATOMIC_SEQ_CST);                        // worker thread終了の合図

    // 全てのthread の終了を待つ
    for (auto &th : thv)
        th.join();
    uint64_t total_count = 0; // commit数の集計用
    // commit数の集計
    for (auto &re : AllResult)
        total_count += re.commit_cnt_;

    std::cout << "throughput exi:" << SLEEP_TIME << " " << total_count / EX_TIME << std::endl;

    return 0;
}
