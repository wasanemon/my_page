#pragma once

#include <atomic>

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