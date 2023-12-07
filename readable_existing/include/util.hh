#pragma once

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
#define PRE_NUM 3000000
#define SLEEP_TIME 10
#define SLEEP_TIME_INIT 2900 * 1000
#define SKEW_PAR 0.0
#define BACKOFF_TIME 0
#define SLEEP_RATE 1