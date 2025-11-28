#pragma once
#include "concurrent_queue.h"
#include "log_entry.h"
#include "rsm.h"
#include <atomic>
namespace kv {
class Channel : public raft::Rsm {
 public:
  static Channel *NewChannel(size_t capacity) { return new Channel(capacity); }

  Channel(size_t capacity) : queue_(capacity) {
      // Initialize atomics
      threshold1_ = 0;
      threshold2_ = 0;
      flag_ = false;
  };
  Channel() = default;
  ~Channel() = default;

  void ApplyLogEntry(raft::LogEntry entry) override { queue_.Push(entry); }
  raft::LogEntry Pop() { return queue_.Pop(); }
  bool TryPop(raft::LogEntry &ent) { return queue_.TryPop(ent); }

  int GetThreshold1() const {
      return threshold1_;
  }

  int GetThreshold2() const {
      return threshold2_;
  }

  bool GetFlag() const {
      return flag_;
  }

  void SetThreshold1(int threshold1) {
      threshold1_ = threshold1;
  }

  void SetThreshold2(int threshold2) {
      threshold2_ = threshold2;
  }

  void SetFlag(bool flag) {
      flag_ = flag;
  }

 private:
  ConcurrentQueue<raft::LogEntry> queue_;
  std::atomic<int> threshold1_{0};
  std::atomic<int> threshold2_{0};
  std::atomic<bool> flag_{false};
};
}  // namespace kv
